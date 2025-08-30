#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Socket::Stream;

my ($left, $right) = eval { Socket::Stream::pair() };
plan skip_all => "Test requires working socketpair()"
    unless $left && $right;

# Test all non-IO methods

my $stream = Socket::Stream->new($left);
isa_ok($stream, 'Socket::Stream', 'new() returns correct class');
ok(!$left->blocking, 'Socket set to non-blocking mode');

# Check initial status
ok(!$stream->recv_eof, 'Not at EOF initially');
ok(!$stream->recv_err, 'No recv error initially');
ok(!$stream->recv_end, 'Recv not ended initially');
ok(!$stream->send_err, 'No send error initially');
ok(!$stream->has_timeout, 'No timeout initially');

# Check buffer state
is($stream->buffer_used, 0, 'Buffer empty initially');
ok(!$stream->buffer_full, 'Buffer not full initially');
is($stream->buffer_free, Socket::Stream::DEFAULT_MAX_READ,
   'Full buffer space available initially');

# Check initial attributes
is($stream->delimiter, "\n", 'Default delimiter is newline');
is($stream->max_read, Socket::Stream::DEFAULT_MAX_READ,
   'Default max_read is set');
is($stream->timeout, undef, 'Default timeout is undef');

# Test setting attributes
is($stream->delimiter("\0"), $stream, 'Set delimiter returns self');
is($stream->delimiter, "\0", 'Delimiter was set');
is($stream->use_CRLF, $stream, 'use_CRLF returns self');
is($stream->delimiter, "\x0D\x0A", 'CRLF delimiter set correctly');

is($stream->max_read(1000), $stream, 'Set max_read returns self');
is($stream->max_read, 1000, 'max_read was set');
is($stream->buffer_free, 1000, 'max_read altered buffer_free');

is($stream->timeout(5), $stream, 'Set timeout returns self');
is($stream->timeout, 5, 'timeout was set');
ok($stream->has_timeout, 'has_timeout is true');
$stream->timeout('30s');
is($stream->timeout, 30, 'Timeout accepts seconds string');
$stream->timeout('2m');
is($stream->timeout, 120, 'Timeout accepts minutes string');
$stream->timeout('1h');
is($stream->timeout, 3600, 'Timeout accepts hours string');
$stream->timeout('0.5d');
is($stream->timeout, 43200, 'Timeout accepts days string');
$stream->timeout(undef);
is($stream->timeout, undef, 'Timeout accepts undef');
ok(!$stream->has_timeout, 'has_timeout is false');
eval { $stream->timeout('invalid') };
ok($@, 'Invalid timeout string causes exception');

# Basic timer behaviour
$stream->timeout(1)->start_timer;
ok($stream->time_left <= 1, 'Timer initialized correctly');
ok(!$stream->timer_expired, 'Timer not expired initially');
$stream->timeout(0)->start_timer;
ok($stream->timer_expired, 'Zero timer is immediately expired');
$stream->timeout(undef)->start_timer;
is($stream->time_left, undef, 'Indefinite timer returns undef time_left');
ok(!$stream->timer_expired, 'Indefinite timer not expired');

# Test callback setters (just registration, not callback activity)
my $called = 0;
is($stream->on_recv_eof(sub { $called++ }), $stream, 'on_recv_eof returns self');
is($stream->on_recv_err(sub { $called++ }), $stream, 'on_recv_err returns self');
is($stream->on_send_err(sub { $called++ }), $stream, 'on_send_err returns self');
is($called, 0, 'Callbacks not called during registration');

# Inheritance helper
ok(eval { Socket::Stream::_EXTEND() > 0 }, '_EXTEND constant defined');

done_testing(40);
