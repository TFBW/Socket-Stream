#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use IO::Socket ();
use Socket ();
use Socket::Stream;

 SKIP:
{
    skip "Platform does not support AF_INET", 1
        unless eval { IO::Socket->new(Domain => Socket::AF_INET) };
    my $sock = Socket::Stream::INET();
    isa_ok($sock, 'IO::Socket::INET', 'Socket::Stream::INET()');
}

 SKIP:
{
    skip "Platform does not support AF_UNIX", 1
        unless eval { IO::Socket->new(Domain => Socket::AF_UNIX) };
    my $sock = Socket::Stream::UNIX();
    isa_ok($sock, 'IO::Socket::UNIX', 'Socket::Stream::UNIX()');
}

 SKIP:
{
    skip "Platform does not suport socketpair", 2
        unless eval { socketpair(my $s1, my $s2,
                                 Socket::AF_UNIX(),
                                 Socket::SOCK_STREAM(),
                                 Socket::PF_UNSPEC()) };
    my ($s1, $s2) = Socket::Stream::pair();
    ok(-S $s1, 's1 is a socket');
    ok(-S $s2, 's2 is a socket');
};

done_testing(4);
