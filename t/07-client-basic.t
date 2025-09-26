#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Socket::Stream;
use Socket::Stream::RESP2Client;
use Socket::Stream::RESP2Parser;

my ($left, $right) = eval { Socket::Stream::pair() };
plan skip_all => "Test requires working socketpair()"
    unless $left && $right;

subtest "constructor" => sub {
    # Test with socket object
    my ($s1, $s2) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($s1);
    isa_ok($client, 'Socket::Stream::RESP2Client');
    is($client->socket, $s1, 'socket() returns original socket');
    
    # Test string parsing via local override
    my @inet_args;
    local *Socket::Stream::INET = sub { @inet_args = @_; return $s2 };
    
    $client = Socket::Stream::RESP2Client->new('example.com:1234');
    is_deeply(\@inet_args, ['example.com:1234'], 'Host:port passed through');
    
    $client = Socket::Stream::RESP2Client->new('example.com');
    is_deeply(\@inet_args, ['example.com:6379'], 'Missing port uses 6379');
    
    $client = Socket::Stream::RESP2Client->new(':1234');
    is_deeply(\@inet_args, ['127.0.0.1:1234'], 'Missing host uses 127.0.0.1');
    
    $client = Socket::Stream::RESP2Client->new('');
    is_deeply(\@inet_args, ['127.0.0.1:6379'], 'Empty string uses defaults');
    
    $client = Socket::Stream::RESP2Client->new(undef);
    is_deeply(\@inet_args, ['127.0.0.1:6379'], 'Undef uses defaults');
};

subtest "timeout attribute" => sub {
    my ($s1, $s2) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($s1);
    
    is($client->timeout, undef, 'Default timeout is undef');
    is($client->timeout(5), $client, 'timeout() returns self');
    is($client->timeout, 5, 'Timeout was set');
    $client->timeout('10s');
    is($client->timeout, 10, 'String timeout accepted');
    $client->timeout(undef);
    is($client->timeout, undef, 'Can set back to undef');
    
    eval { $client->timeout('bad') };
    like($@, qr/Invalid timeout/, 'Invalid timeout rejected');
};

subtest "request formatting" => sub {
    my ($s1, $s2) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($s1);
    my $parser = Socket::Stream::RESP2Parser->new(
        Socket::Stream->new($s2)->use_CRLF
    );
    
    # Single request
    $client->request('GET', 'key');
    is($parser->receive(1), 1, 'Request received');
    is_deeply($parser->take, ['GET', 'key'], 'Simple request parsed');
    
    # Request with null
    $client->request('SET', 'k', undef);
    $parser->receive(1);
    is_deeply($parser->take, ['SET', 'k', undef], 'Request with null');
    
    # Multiple requests via arrays
    $client->request(['PING'], ['GET', 'x']);
    $parser->receive(2);
    is_deeply([$parser->take(2)], [['PING'], ['GET', 'x']], 
              'Multiple requests sent');
    
    # Empty array edge case
    $client->request();
    $parser->receive(1);
    is_deeply($parser->take, [], 'Empty request creates empty array');
};

subtest "request_raw" => sub {
    my ($s1, $s2) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($s1);
    my $stream = Socket::Stream->new($s2)->use_CRLF;
    
    $client->request_raw('PING', 'INFO');
    is($stream->recv_msg, 'PING', 'First raw message');
    is($stream->recv_msg, 'INFO', 'Second raw message');
    
    # Inline command format
    $client->request_raw('GET key');
    is($stream->recv_msg, 'GET key', 'Inline command sent raw');
};

subtest "response parsing" => sub {
    my ($s1, $s2) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($s1);
    my $server = Socket::Stream->new($s2)->use_CRLF;
    
    # Simple string
    $server->send_msg('+OK');
    is($client->response, 'OK', 'Simple string response');
    
    # Error response
    $server->send_msg('-ERR bad');
    my $err = $client->response;
    isa_ok($err, 'SCALAR', 'Error is scalar ref');
    is($$err, 'ERR bad', 'Error content');
    
    # Multiple responses
    $server->send_msg(':42', '+YES', '$5', 'hello');
    my @resp = $client->response(3);
    is($resp[0], 42, 'Integer response');
    is($resp[1], 'YES', 'Simple string');
    is($resp[2], 'hello', 'Bulk string');
    
    # Array response
    $server->send_msg('*2', '+first', '+second');
    my $arr = $client->response;
    is_deeply($arr, ['first', 'second'], 'Array response');
    
    # Null responses
    $server->send_msg('$-1', '*-1');
    @resp = $client->response(2);
    is($resp[0], undef, 'Null bulk string');
    is($resp[1], undef, 'Null array');
};

subtest "response count zero" => sub {
    my ($s1, $s2) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($s1);
    
    my @empty = $client->response(0);
    is_deeply(\@empty, [], 'response(0) returns empty list');
};

subtest "available_responses" => sub {
    my ($s1, $s2) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($s1);
    my $server = Socket::Stream->new($s2)->use_CRLF;
    
    # Scalar context
    is($client->available_responses, 0, 'None available initially');
    
    $server->send_msg('+ONE', '+TWO', '+THREE');
    is($client->available_responses(2), 2, 'Two available (limited)');
    is($client->available_responses, 3, 'Three available (unlimited)');
    
    # List context with limit
    my @resp = $client->available_responses(2);
    is_deeply(\@resp, ['ONE', 'TWO'], 'Got two responses');
    
    # List context without limit
    @resp = $client->available_responses;
    is_deeply(\@resp, ['THREE'], 'Got remaining response');
    
    # Edge case: available_responses(0)
    $server->send_msg('+X', '+Y');
    @resp = $client->available_responses(0);
    is_deeply(\@resp, ['X', 'Y'], 'Limit 0 returns all available');
    is($client->available_responses, 0, 'All consumed');
};

subtest "await_response" => sub {
    my ($s1, $s2) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($s1);
    my $server = Socket::Stream->new($s2)->use_CRLF;
    
    # Pre-send responses
    $server->send_msg('+A', '+B');
    
    is($client->await_response(2), $client, 'Returns self');
    is($client->available_responses, 2, 'Two responses now available');
    
    # await_response with default count
    $server->send_msg('+C');
    $client->await_response;
    is($client->available_responses, 3, 'Default awaits at least 1');
};

subtest "stream error propagation" => sub {
    my ($s1, $s2) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($s1);
    
    close $s2;  # Close server side
    
    eval { $client->response };
    like($@, qr/Stream ended/, 'EOF causes exception');
    
    # Further operations should also fail
    eval { $client->request('PING') };
    like($@, qr/Can't send/, 'Send after error fails');
};

done_testing();
