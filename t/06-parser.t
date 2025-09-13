#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Socket::Stream;
use Socket::Stream::RESP2Parser;

my ($left, $right) = eval { Socket::Stream::pair() };
plan skip_all => "Test requires working socketpair()"
    unless $left && $right;
my $client = Socket::Stream->new($left);

# Test non-I/O methods

# Test basic construction
my $parser = Socket::Stream::RESP2Parser->new($client);
isa_ok($parser, 'Socket::Stream::RESP2Parser', 'new() returns correct class');
is($client->delimiter, "\x0D\x0A", 'Stream set to CRLF delimiter');

# Check initial state
is($parser->count, 0, 'No data initially');
is($parser->error, '', 'No error initially');
is_deeply([$parser->take], [], 'take() returns empty list initially');
is_deeply([$parser->take(5)], [], 'take(n) returns empty list initially');

# $MaxData
ok(eval { $Socket::Stream::RESP2Parser::MaxData > 0 }, 'Default $MaxData > 0');
is(eval { $Socket::Stream::RESP2Parser::MaxData = 1000 }, 1000, '$MaxData is settable');

# Error behaviour
is($parser->error('test'), $parser, 'Set error returns self');
is($parser->{stream}, undef, 'Setting error undefs stream');
is($parser->error, 'test', 'Error text set correctly');
is($parser->receive, -1, 'receive() returns -1 if error set');

# Test I/O methods

my $server = Socket::Stream->new($right)->use_CRLF;
$parser = Socket::Stream::RESP2Parser->new($client); # reset
is($parser->receive, 0, 'receive() returns 0 when no data pending');

# Simple string
$server->send_msg('+OK');
is($parser->receive, 0, 'receive() returns 0 when count unspecified');
is($parser->count, 1, 'One item available');
is($parser->receive(1), 1, 'receive() returns 1 when count satisfied');
is($parser->count, 1, 'One item still available');
is($parser->take, 'OK', 'Simple string parsed correctly');
is($parser->count, 0, 'Count decremented after take');

# Error string
$server->send_msg('-ERR bad command');
is($parser->receive(1), 1, 'Error response received');
my $err = $parser->take;
is(ref($err), 'SCALAR', 'Error is a scalar reference');
is($$err, 'ERR bad command', 'Error content correct');
is($parser->count, 0, 'Zero items remain');

# Integer
$server->send_msg(':42', ':-100', ':0');
is($parser->receive(3), 1, 'Three integers received');
is($parser->count, 3, 'Three items available');
my @num = $parser->take(3);
is($num[0], '42', 'Positive integer parsed');
is($num[1], '-100', 'Negative integer parsed');
is($num[2], '0', 'Zero parsed');
is($parser->count, 0, 'Zero items remain');

# Bulk strings
$server->send_msg('$5', 'hello', '$0', '', '$-1');
is($parser->receive(3), 1, 'Bulk strings received');
my @bulk = $parser->take(3);
is($bulk[0], 'hello', 'Normal bulk string parsed');
is($bulk[1], '', 'Empty bulk string parsed');
is($bulk[2], undef, 'Null bulk string parsed');
is($parser->count, 0, 'Zero items remain');

# Arrays
$server->send_msg('*0', '*2', '+first', '+second', '*-1');
is($parser->receive(3), 1, 'Arrays received');
my @arrays = $parser->take(3);
is_deeply($arrays[0], [], 'Empty array parsed');
is_deeply($arrays[1], ['first', 'second'], 'Two-element array parsed');
is($arrays[2], undef, 'Null array parsed');
is($parser->count, 0, 'Zero items remain');

# Nested arrays
ok($server->send_msg('*2', '*2', ':1', ':2', '*2', ':3', ':4'), 'Sent nested array');
is($parser->receive(1), 1, 'Nested array received');
is_deeply($parser->take, [[1, 2], [3, 4]], 'Nested array structure correct');
is($parser->count, 0, 'Zero items remain');

# Inline commands (from client; ignore blank lines)
ok($server->send_msg('GET key', '', 'SET key value'), 'Sent three messages');
is($parser->receive, 0, 'receive() OK');
is($parser->count, 2, 'Two inline commands received');
my @cmds = $parser->take(2);
is_deeply($cmds[0], ['GET', 'key'], 'GET command parsed');
is_deeply($cmds[1], ['SET', 'key', 'value'], 'SET command parsed');
is($parser->count, 0, 'Zero items remain');

# Partial data
ok($server->send_data("*2\x0D\x0A+fir"), 'Sent partial data');
is($parser->receive(1), 0, 'Partial data returns 0');
is($parser->count, 0, 'No complete items yet');
ok($server->send_data("st\x0D\x0A+second\x0D\x0A"), 'Sent remaining data');
is($parser->receive(1), 1, 'Complete after more data');
is_deeply($parser->take, ['first', 'second'], 'Partial array completed');
is($parser->count, 0, 'Zero items remain');

# Receive with limit
ok($server->send_msg(map { ":$_" } 1..10), 'Sent ten messages');
is($parser->receive(5), 1, 'receive(n) stops at n items');
is($parser->count, 5, 'Exactly n items available');
is($parser->receive(10), 1, 'Can receive more');
is($parser->count, 10, 'All items now available');
is($parser->receive(11), 0, 'No more available');
my @all = $parser->take(20);  # More than available
is(0 + @all, 10, 'take(n) returns only what is available');
is($parser->count, 0, 'Zero items remain');

# Bad protocol handling
my $parser2 = Socket::Stream::RESP2Parser->new($client);
$server->send_msg('$bad');
is($parser2->receive(1), -1, 'Bad bulk count causes error');
like($parser2->error, qr/invalid bulk count/, 'Appropriate error message');

# EOF handling
$server->send_msg('+first', '*2', '+part');
close($right);  # Close server side
is($parser->receive(1), 1, 'receive(1) returns 1 despite EOF');
is($parser->receive(2), -1, 'receive(2) returns -1 because of EOF');
like($parser->error, qr/connection closed/, 'EOF error message');
is($parser->count, 1, 'One item remains');
is($parser->take, 'first', 'Completed item correctly queued');
is($parser->count, 0, 'Zero items remain');

done_testing(72);
