#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Socket::Stream;
use Socket::Stream::RESP2Client;
use Socket::Stream::RESP2Parser;

# Check fork availability early
my $can_fork = eval {
    my $pid = fork();
    if (defined $pid) {
        if ($pid == 0) { exit 0 }
        else { waitpid($pid, 0) }
        1;
    }
};
plan skip_all => "Fork not available: $@" unless $can_fork;

# Mock server that processes commands via RESP2Parser
my %RESP = (
    EMPTY     => '*0',
    ERROR     => '-ERR requested error',
    INCR      => ':42',
    NULL      => '$-1',
    NULLARRAY => '*-1',
    PING      => '+PONG',
    SET       => '+OK',
    );
sub mock_server {
    my ($stream) = @_;
    my $parser = Socket::Stream::RESP2Parser->new($stream);
    while (1) {
        $stream->await_data until $parser->receive(1);
        my $cmd = $parser->take
            // return;
        my @response;
        # Simple command dispatch
        my $op = uc($cmd->[0] // '');
        note("Server received '$op' command");
        if ($RESP{$op}) {
            @response = $RESP{$op};
        }
        elsif ($op eq 'GET') {
            # Return the key as value for predictability
            my $key = $cmd->[1] // '';
            @response = ('$' . length($key), $key);
        }
        elsif ($op eq 'LRANGE') {
            @response = ('*2', '$3', 'foo', '$3', 'bar');
        }
        elsif ($op eq 'CLOSE') {
            return;
        }
        elsif ($op eq 'BLOCK') {
            sleep 10;  # For timeout testing
            @response = '+NEVER';
        }
        else {
            @response = "-ERR unknown command '$op'";
        }
        # Send response
        $stream->send_msg(@response);
    }
}

# Test wrapper with fork
sub with_server (&) {
    my ($test) = @_;
    my ($c_sock, $s_sock) = Socket::Stream::pair()
        or die "socketpair: $!";
    my $pid = fork();
    die "fork: $!" unless defined $pid;
    if ($pid == 0) {
        # Child - server
        close $c_sock;
        close STDOUT;
        close STDERR;
        $SIG{TERM} = sub { exit 0 };
        eval {
            my $stream = Socket::Stream->new($s_sock)->use_CRLF;
            mock_server($stream);
            note("Shutting server down");
            shutdown($s_sock, 2);
        };
        exit 0;
    }
    # Parent - client test
    close $s_sock;
    my $client = Socket::Stream::RESP2Client->new($c_sock);
    my $result;
    eval {
        local $SIG{ALRM} = sub { die "Test took too long (ALARM)\n" };
        alarm(5);
        $result = $test->($client);
        alarm(0);
    };
    my $err = $@;
    kill 'TERM', $pid if $pid;
    waitpid($pid, 0);
    die $err if $err;
    return $result;
}

subtest "call method" => sub {
    with_server {
        my $c = shift;
        is($c->call('PING'), 'PONG', 'Simple call');
        is($c->call('GET', 'mykey'), 'mykey', 'GET returns key as value');
        is($c->call('SET', 'k', 'v'), 'OK', 'SET returns OK');
        is($c->call('INCR', 'counter'), 42, 'Integer response');
    };
};

subtest "call with multiple requests" => sub {
    with_server {
        my $c = shift;
        my @results = $c->call(['PING'], ['GET', 'x']);
        is($results[0], 'PONG', 'First response');
        is($results[1], 'x', 'Second response');
    };
};

subtest "error responses" => sub {
    with_server {
        my $c = shift;
        my $err = $c->call('ERROR');
        isa_ok($err, 'SCALAR', 'Error is scalar ref');
        is($$err, 'ERR requested error', 'Error message');
        
        # Unknown command
        $err = $c->call('NOSUCHCMD');
        isa_ok($err, 'SCALAR', 'Unknown command error');
        like($$err, qr/unknown command/, 'Unknown command message');
    };
};

subtest "pipeline depth" => sub {
    with_server {
        my $c = shift;
        # Send multiple requests
        $c->request_raw(('PING') x 5);
        
        # Read responses one by one
        for (1..5) {
            is($c->response, 'PONG', "Pipelined response $_");
        }
    };
    
    with_server {
        my $c = shift;
        # Send batch, read batch
        $c->request_raw(map("GET $_", qw(a b c)));
        my @vals = $c->response(3);
        is_deeply(\@vals, ['a', 'b', 'c'], 'Batch responses');
    };
};

subtest "request_raw inline commands" => sub {
    with_server {
        my $c = shift;
        $c->request_raw('PING');
        is($c->response, 'PONG', 'Inline command gets OK');
        
        # Multiple inline
        $c->request_raw('SET foo bar', 'SET bar foo');
        my @resp = $c->response(2);
        is_deeply(\@resp, ['OK', 'OK'], 'Multiple inline commands');
    };
};

subtest "null and empty responses" => sub {
    with_server {
        my $c = shift;
        # Null bulk string
        my $val = $c->call('NULL');
        is($val, undef, 'Null bulk string');
        
        # Empty array
        my $arr = $c->call('EMPTY');
        is_deeply($arr, [], 'Empty array');
        
        # Null array
        $arr = $c->call('NULLARRAY');
        is($arr, undef, 'Null array');
    };
};

subtest "array responses" => sub {
    with_server {
        my $c = shift;
        my $result = $c->call('LRANGE', 'list', '0', '-1');
        is_deeply($result, ['foo', 'bar'], 'Array response parsed');
    };
};

subtest "timeout handling" => sub {
    with_server {
        my $c = shift;
        $c->timeout(0.2);
        eval { $c->call('BLOCK') };
        note("Exception: $@") if $@;
        unlike($@, qr/ALARM/, 'Timeout detected');
    };
};

subtest "connection loss mid-pipeline" => sub {
    with_server {
        my $c = shift;
        $c->request(['PING'], ['CLOSE']);
        is($c->response, 'PONG', 'First response OK');
        # Second fails due to connection loss
        eval { $c->response };
        note("Exception: $@") if $@;
        unlike($@, qr/ALARM/, 'Connection loss detected');
    };
};

subtest "await_response blocking" => sub {
    with_server {
        my $c = shift;
        # Nothing available yet
        is($c->available_responses, 0, 'Nothing available');
        
        # Send request
        $c->request('PING');
        
        # This will block until response arrives
        $c->await_response;
        
        is($c->available_responses, 1, 'Response now available');
        is($c->response, 'PONG', 'Got response');
    };
};

subtest "mixed operations" => sub {
    with_server {
        my $c = shift;
        
        # Mix different operation styles
        $c->request('PING');
        is($c->response, 'PONG', 'First');
        
        is($c->call('GET', 'k'), 'k', 'Call in middle');
        
        $c->request(['PING'], ['GET', 'y']);
        my @r = $c->response(2);
        is_deeply(\@r, ['PONG', 'y'], 'Multiple at end');
    };
};

subtest "response synchronization" => sub {
    with_server {
        my $c = shift;
        
        # Send several requests
        $c->request((['PING']) x 3);
        ok($c->await_response(3, 0.2), 'Three responses waiting');
        
        # response(0) should be a no-op but not fail
        my @empty = $c->response(0);
        is_deeply(\@empty, [], 'response(0) returns empty');
        
        # But the responses are still there
        is(0 + $c->available_responses, 3, 'Three responses still waiting');
    };
};

done_testing();
