#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Socket::Stream;

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
plan skip_all => "Test requires AnyEvent"
    unless eval { require AnyEvent; 1 };

ok(Socket::Stream::USING_AE, "USING_AE test works");

my $LARGE = 'blah' x int(0.9 * $s1->max_read / 4);
my @MULTI = ($LARGE, $LARGE, $LARGE, $LARGE, $LARGE);
my $SIZE = length($LARGE);

subtest "Transfer large delimited messages" => sub {
    my $ok = 0;
    my $bad = 0;
    my $cv = AnyEvent->condvar;
    my $w; $w = AnyEvent->io(
        fh => $right,
        poll => 'r',
        cb => sub {
            my $x;
            while (defined($x = $s2->recv_msg_nb)) {
                if ($x eq 'stop') { $cv->send; undef $w }
                elsif ($x eq $LARGE) { $ok++ }
                else { $bad++ }
            }
        }
        );
    ok($s1->send_msg(@MULTI, 'stop'), "send_msg succeeded");
    $cv->recv;
    is($bad, 0, "No bad messages");
    is($ok, scalar(@MULTI), "Correct message count");
};

subtest "Transfer large regex-delimited messages" => sub {
    my $ok = 0;
    my $bad = 0;
    my $cv = AnyEvent->condvar;
    my $w; $w = AnyEvent->io(
        fh => $right,
        poll => 'r',
        cb => sub {
            my $x;
            while (defined($x = $s2->recv_re_nb(qr/\s/))) {
                $x =~ s/\s//g;
                if ($x eq 'stop') { $cv->send; undef $w }
                elsif ($x eq $LARGE) { $ok++ }
                else { $bad++ }
            }
        }
        );
    ok($s1->send_data(join("\t", @MULTI, 'stop', '')), "send_msg succeeded");
    $cv->recv;
    is($bad, 0, "No bad messages");
    is($ok, scalar(@MULTI), "Correct message count");
};

subtest "Transfer large fixed messages ($SIZE bytes)" => sub {
    my $ok = 0;
    my $bad = 0;
    my $cv = AnyEvent->condvar;
    my $w; $w = AnyEvent->io(
        fh => $right,
        poll => 'r',
        cb => sub {
            my $x;
            while (defined($x = $s2->recv_data_nb($SIZE))) {
                if ($x eq $LARGE) { $ok++ }
                else { $bad++ }
                if ($ok + $bad == @MULTI) { $cv->send; undef $w }
            }
        }
        );
    ok($s1->send_data(@MULTI), "send_msg succeeded");
    $cv->recv;
    is($bad, 0, "No bad messages");
    is($ok, scalar(@MULTI), "Correct message count");
};

done_testing();
