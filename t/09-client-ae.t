#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Socket::Stream;
use Socket::Stream::RESP2Client;
use Socket::Stream::RESP2Parser;

# Check AnyEvent availability
BEGIN {
    eval { require AnyEvent; 1 }
        or plan skip_all => "AnyEvent not available";
}

# sleep, but allow event loop
sub ae_sleep {
    my ($t) = @_;
    my $cv = AE::cv();
    my $timer = AE::timer($t, 0, sub { $cv->send });
    $cv->recv;
    return;
}

# Mock server responses
my %RESP = (
    EMPTY     => '*0',
    ERROR     => '-ERR requested error',
    INCR      => ':42',
    NULL      => '$-1',
    PING      => '+PONG',
    SET       => '+OK',
    );
# AnyEvent mock server
sub mock_server_ae {
    my ($sock) = @_;
    my $stream = Socket::Stream->new($sock);
    my $parser = Socket::Stream::RESP2Parser->new($stream);
    my $buffer = '';
    my ($w, $r);
    my $send = sub {
        $buffer .= join('', map { "$_\x0D\x0A" } @_);
        $w //= AE::io($sock, 1, sub {
            my $n = syswrite($sock, $buffer);
            if (!defined $n) {
                return if $!{EAGAIN} || $!{EWOULDBLOCK};
            }
            elsif ($n < length($buffer)) {
                # Partial send
                substr($buffer, 0, $n, '');
                return;
            }
            # All sent or fatal error
            undef $w;
        });
    };
    $r = AE::io($sock, 0, sub {
        while ($parser->receive(1) == 1) {
            my $cmd = $parser->take;
            my $op = uc($cmd->[0] // '');
            note("Server received '$op' command");
            if ($RESP{$op}) {
                $send->($RESP{$op});
            }
            elsif ($op eq 'GET') {
                my $key = $cmd->[1] // '';
                $send->('$' . length($key), $key);
            }
            else {
                $send->("-ERR unknown command '$op'");
            }
        }
        if ($stream->recv_end) { undef $r; undef $w }
    });
}

# Test harness
sub with_ae_server (&) {
    my ($test) = @_;
    my ($c_sock, $s_sock) = Socket::Stream::pair()
        or die "socketpair: $!";
    mock_server_ae($s_sock);
    my $client = Socket::Stream::RESP2Client->new($c_sock);
    local $SIG{ALRM} = sub { die "Test took too long (ALARM)\n" };
    alarm(5);
    $test->($client);
    alarm(0);
    return;
}

with_ae_server {
    my ($c) = @_;
    $c->request('PING'); # request first; most tests receive first
    is($c->response_cv->recv, 'PONG', 'Basic response_cv');
};

with_ae_server {
    my ($c) = @_;
    is($c->call_cv(qw(GET mykey))->recv, 'mykey', 'Basic call_cv');
};

with_ae_server {
    my ($c) = @_;
    my @results = $c->call_cv(['PING'], ['GET', 'x'])->recv;
    is_deeply(\@results, ['PONG', 'x'], 'Batched call_cv');
};

subtest "queued response_cv" => sub {
    with_ae_server {
        my ($c) = @_;
        # Queue multiple async responses, then send requests
        my ($cv1, $cv2, $cv3) = map { $c->response_cv } 1..3;
        $c->request_raw('PING', 'SET x x', 'GET x');
        is($cv1->recv, 'PONG', 'First queued response');
        is($cv2->recv, 'OK', 'Second queued response');
        is($cv3->recv, 'x', 'Third queued response');
    };
};

subtest "mixed sync after async" => sub {
    with_ae_server {
        my ($c) = @_;
        my $cv = $c->response_cv;                   # Queue async response
        $c->request_raw('PING', 'SET k v');         # Send two commands
        is($c->response, 'OK', 'Sync after async'); # Get sync response
        is($cv->recv, 'PONG', 'Async completed');   # Verify async resposne
    };
};

subtest "error propagation in queue" => sub {
    my ($c_sock, $s_sock) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($c_sock);
    my $server = Socket::Stream->new($s_sock)->use_CRLF;
    # Queue multiple receives on client
    my @cv = map { $client->response_cv } 1..3;
    # Send one OK response from server
    $server->send_msg('+OK');
    # Now close the server socket
    close $s_sock;
    my @result = map { eval { $_->recv eq 'OK' ? 1 : -1 } // 0 } @cv;
    note("$@") if $@;
    ok($@, 'CV croaks');
    is_deeply(\@result, [1, 0, 0], 'First succeeds, others fail');
};

# Prove that slow responses don't trigger timeouts for queued items
subtest "timeout in queue" => sub {
    my ($c_sock, $s_sock) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($c_sock);
    my $server = Socket::Stream->new($s_sock)->use_CRLF;
    # Queue three responses: timeouts long, short, short
    my ($cv1, $cv2, $cv3) = map {
        $client->response_cv(1, $_)
    } (1.00, 0.05, 0.05);
    # Send three responses: speeds slow, fast, slow
    my $n = 0;
    for (0.1, 0.01, 0.1) {
        ae_sleep($_);
        $n++;
        $server->send_msg("+OK$n");
    }
    is($cv1->recv, 'OK1', 'First completed');
    is($cv2->recv, 'OK2', 'Second completed');
    is(eval { $cv3->recv; 1 }, undef, 'Third failed');
    note("$@") if $@;
};

subtest "synchronization" => sub {
    with_ae_server {
        my ($c) = @_;
        my $order = '';
        # First real response
        $c->response_cv(1)->cb(sub {
            $order .= 'A';
            is($order, 'A', 'First response');
        });
        # Sync point
        $c->response_cv(0)->cb(sub {
            my @empty = $_[0]->recv;
            is_deeply(\@empty, [], 'Zero count returns empty');
            $order .= 'B';
            is($order, 'AB', 'Sync point');
        });
        # Second real response
        $c->response_cv(1)->cb(sub {
            $order .= 'C';
            is($order, 'ABC', 'Second response');
        });
        is($order, '', 'Before send');
        # Send two requests
        $c->request_raw('PING', 'PING');
        # Sync all responses
        $c->response(0);
        $order .= 'D';
        is($order, 'ABCD', 'Final sync');
    };
};

subtest "available_responses with async pending" => sub {
    my ($c_sock, $s_sock) = Socket::Stream::pair();
    my $client = Socket::Stream::RESP2Client->new($c_sock);
    my $server = Socket::Stream->new($s_sock)->use_CRLF;
    # Expect five responses
    my $cv = $client->response_cv(5);
    # Send four
    $server->send_msg(('+OK') x 4);
    # None should be visible as available
    is($client->available_responses, 0, 'Empty while async pending');
    # Send two more
    $server->send_msg('+OK', '+LAST');
    # STILL not available unless that send blocked, so wait
    $client->await_response(1, 0.1);
    is($client->available_responses, 1, 'Available after async done');
    is_deeply([$cv->recv], [('OK') x 5], 'Async receive correct');
    is($client->response, 'LAST', 'Sync receive correct');
    is($client->available_responses, 0, 'All clear');
};

done_testing();
