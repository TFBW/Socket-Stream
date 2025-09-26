#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Errno ();
use Socket::Stream;

# Micro-class for tracking closure lifetime
my $xsubs = 0;
sub xsub (&) { my $code = shift; ++$xsubs; return bless($code, '_xsub') }
sub _xsub::DESTROY { --$xsubs }

my ($left, $right, $s1, $s2);
sub reset_sockets {
    undef $_ for $s1, $s2, $left, $right;
    ($left, $right) = eval { Socket::Stream::pair() };
    $s1 = Socket::Stream->new($left)->timeout(0.2)
        if $left;
    $s2 = Socket::Stream->new($right)->timeout(0.2)
        if $right;
}

reset_sockets();
plan skip_all => "Test requires working socketpair()"
    unless $left && $right;

subtest "Fill send buffer and time out" => sub {
    my $err = 0;
    my $count = 0;
    $s1->on_send_err(xsub { $err = $! });
    is($xsubs, 1, "Closure exists");
    while ($s1->send_data('1234567890')) {
        $count += 10;
        if ($err) {
            ok(0, "send_data returned success but err=$err");
            return;
        }
    }
    is($xsubs, 0, "Closure destroyed");
    note("Sent $count bytes before failing");
    is($err, Socket::Stream::ETIMEDOUT, "send_data reached limit and timed out");
    is($err, $s1->send_err, "send_err matches");
};

reset_sockets();
subtest "Time out or not on await_io" => sub {
    $s2->start_timer;
    cmp_ok($s2->time_left, '>', 0, "timer has not expired");
    $s2->await_io(0);
    cmp_ok($s2->time_left, '<=', 0, "timer has expired");
    ok(!$s2->recv_err, "no recv_err");
    ok($s1->send_data('X'), "send_data ");
    $s2->start_timer;
    cmp_ok($s2->time_left, '>', 0, "timer has not expired");
    $s2->await_io(0);
    cmp_ok($s2->time_left, '>', 0, "timer still has not expired");
};

reset_sockets();
subtest "Time out on recv_data" => sub {
    my $err = 0;
    unless ($s1->send_data('1234567890')) {
        ok(0, "send_data failed");
        return;
    }
    ok(1, "Sent data");
    $s2->on_recv_err(xsub { $err = $! });
    is($xsubs, 1, "Closure exists");
    my $n = 0;
    $n++ while defined($s2->recv_data(1)) and !$err;
    is($n, 10, "Read all data");
    is($xsubs, 0, "Closure destroyed");
    is($err, Socket::Stream::ETIMEDOUT, "recv_data timed out");
    is($err, $s2->recv_err, "recv_err matches");
    ok($s2->recv_end, "Is a recv_end condition");
    ok(!$s2->recv_eof, "Not an EOF condition");
    ok(!$s2->data_end, "Not a data_end condition");
};

reset_sockets();
subtest "Reach buffer full condition on read" => sub {
    my $err = 0;
    $s2->max_read(10)->on_recv_err(xsub { $err = $! });
    is($xsubs, 1, "Closure exists");
    my $n = 0;
    while (!$err and $n++ < 20) {
        unless ($s1->send_data('X')) {
            ok(0, "send_data failed unexpectedly ($!)");
            return;
        }
        if (defined $s2->recv_msg_nb) {
            ok(0, "recv_msg_nb returned data unexpectedly");
            return
        }
    }
    is($n, 10, "executed expected number of loops");
    is($xsubs, 0, "Closure destroyed");
    is($err, Socket::Stream::EMSGSIZE, "recv_data_nb hit buffer limit");
    is($err, $s2->recv_err, "recv_err matches");
    ok($s2->recv_end, "Is a recv_end condition");
    ok(!$s2->recv_eof, "Not an EOF condition");
    ok(!$s2->data_end, "Not a data_end condition");
};

reset_sockets();
subtest "Reach EOF condition on read" => sub {
    my $eof = 0;
    $s2->on_recv_eof(xsub { $eof = 1 });
    is($xsubs, 1, "Closure exists");
    unless ($s1->send_data("Hello")) {
        ok(0, "send_msg failed unexpectedly: $!");
        return;
    }
    close $left;
    ok($s2->await_data, "Data arrived");
    ok(!$eof, "EOF not detected yet");
    ok(!$s2->await_data, "No more data arrived");
    ok($eof, "EOF hook was called");
    is($xsubs, 0, "Closure destroyed");
    ok($s2->recv_eof, "EOF status is set");
    ok($s2->recv_end, "Is a recv_end condition");
    ok(!$s2->recv_err, "Not flagged as an error");
    ok(!$s2->data_end, "Not a data_end condition (yet)");
    is($s2->recv_data(5), "Hello", "Expected data received");
    ok($s2->data_end, "data_end now true");
};

reset_sockets();
subtest "Close own socket" => sub {
    my ($rerr, $serr);
    $s2->on_recv_err(xsub { $rerr = $! });
    $s2->on_send_err(xsub { $serr = $! });
    is($xsubs, 2, "Closures exist");
    close $right;
    ok(!($rerr||$serr), "No errors yet");
    ok(!$s2->await_data, "await_data returns false");
    is($xsubs, 1, "First closure destroyed");
    is($rerr, Socket::Stream::EBADF, "Got bad file descriptor error");
    is($rerr, $s2->recv_err, "recv_err matches");
    ok($s2->recv_end, "Is a recv_end condition");
    ok(!$s2->recv_eof, "Not an EOF condition");
    ok(!$s2->data_end, "Not a data_end condition");
    ok(!$serr, "Send error still clear");
    ok(!$s2->send_msg(''), "send_msg fails");
    is($xsubs, 0, "Second closure destroyed");
    is($serr, Socket::Stream::EBADF, "Got bad file descriptor error");
    is($serr, $s2->recv_err, "recv_err matches");
};

reset_sockets();
subtest "Write to closed socket" => sub {
    my $err = 0;
    my $sig = 'no';
    local $SIG{PIPE} = sub { $sig = 'yes' };
    $s1->on_send_err(xsub { $err = $! });
    is($xsubs, 1, "Closure exists");
    unless ($s1->send_data("Hello")) {
        ok(0, "send_msg failed unexpectedly: $!");
        return;
    }
    ok(!$s1->send_err, "no send_err yet");
    close $right;
    ok(!$s1->send_data("there"), "send_data to closed socket failed");
    ok($err, "send_err is true ($err)");
    is($xsubs, 0, "Closure destroyed");
    is($err, $s1->send_err, "send_err matches");
    note("SIGPIPE received? $sig");
};

done_testing(7);
