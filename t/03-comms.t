#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Socket::Stream;

my $BLAH = join(' ', map(chr($_), 33..126));
my $N = length($BLAH);

my ($left, $right) = eval { Socket::Stream::pair() };
plan skip_all => "Test requires working socketpair()"
    unless $left && $right;
my $s1 = Socket::Stream->new($left)->timeout(0.2);
my $s2 = Socket::Stream->new($right)->timeout(0.2);

plan skip_all => "Test requires socket write buffer at least $N bytes"
    unless (send($left, $BLAH, 0)//0) == $N;
my $data;
recv($right, $data, $N, 0);
plan skip_all => "Test requires working socket send/recv"
    unless $data && $data eq $BLAH;

sub assert_buffer_empty { is($s2->buffer_used, 0, "Buffer is empty") }
assert_buffer_empty();

$s1->send_msg('test');
is($s2->recv_msg, 'test', "Single send_msg+recv_msg");
assert_buffer_empty();

$s1->send_msg(qw(foo bar baz));
is($s2->recv_msg_nb, $_, "Multi send_msg+recv_msg_nb ($_)")
    for qw(foo bar baz);
assert_buffer_empty();

$s1->send_data($BLAH);
is($s2->recv_data($N), $BLAH, "Single send_data+recv_data");
assert_buffer_empty();

$s1->send_data('A'..'Z');
is($s2->recv_data_nb(26), join('', 'A'..'Z'), "Multi send_data+recv_data_nb");
assert_buffer_empty();

$s1->send_data('123X456Y');
is($s2->recv_re_nb(qr/A/), undef, 'Mismatch recv_re_nb');
is($s2->recv_re(qr/X/), '123X', 'Match recv_re');
is($s2->recv_re_nb(qr/Y/), '456Y', 'Match recv_re_nb');
assert_buffer_empty();

$s1->send_data('123456');
is($s2->recv_status, 6, "Positive recv_status");
is($s2->recv_status, -1, "Blocking recv_status");
$s2->max_read(6);
is($s2->recv_status, -2, "Full recv_status");
$s2->max_read(10);
close $left;
is($s2->recv_status, 0, "EOF recv_status");
ok(!$s2->await_data, "await_data false at EOF");

done_testing();
