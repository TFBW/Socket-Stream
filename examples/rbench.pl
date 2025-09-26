#!/usr/bin/env perl
use warnings;
use strict;

use Socket::Stream::RESP2Client;
use Time::HiRes qw(time);

our %DEP;
BEGIN { $DEP{$_} = 1 for grep { /^\w+$/ } @ARGV }
use if $DEP{event}, 'AnyEvent';
use if $DEP{redis}, 'Redis';
use if $DEP{fred},  'Redis::Fast';

my $RED = $DEP{redis} && Redis->new;
my $FRED = $DEP{fred} && Redis::Fast->new;
my $RESP = Socket::Stream::RESP2Client->new;
my $PARSER = $RESP->[Socket::Stream::RESP2Client::PARSER];
my $STREAM = $RESP->[Socket::Stream::RESP2Client::STREAM];
my $CMD = "set hoge fuga";

sub redis {
    my ($count, $batch) = @_;
    my $err = 0;
    while ($count) {
        $batch = $count if $batch > $count;
        $count -= $batch;
        $RED->set(hoge => 'fuga', sub { $err++ if defined $_[1] })
            for 1..$batch;
        $RED->wait_all_responses;
    }
    return $err;
}

sub fred {
    my ($count, $batch) = @_;
    my $err = 0;
    while ($count) {
        $batch = $count if $batch > $count;
        $count -= $batch;
        $FRED->set(hoge => 'fuga', sub { $err++ if defined $_[1] })
            for 1..$batch;
        $FRED->wait_all_responses;
    }
    return $err;
}

sub stream {
    my ($count, $batch) = @_;
    my $err = 0;
    while ($count) {
        $batch = $count if $batch > $count;
        $count -= $batch;
        $STREAM->send_msg(($CMD) x $batch);
        for (1..$batch) { $err++ if $STREAM->recv_msg ne '+OK' }
    }
    return $err;
}

sub parse {
    my ($count, $batch) = @_;
    my $err = 0;
    while ($count) {
        $batch = $count if $batch > $count;
        $count -= $batch;
        $STREAM->send_msg(($CMD) x $batch);
        $STREAM->await_data until $PARSER->receive($batch);
        for ($PARSER->take($batch)) { $err++ if ref eq 'SCALAR' }
    }
    return $err;
}

sub basic {
    my ($count, $batch) = @_;
    my $err = 0;
    while ($count) {
        $batch = $count if $batch > $count;
        $count -= $batch;
        $RESP->request_raw(($CMD) x $batch);
        for ($RESP->response($batch)) { $err++ if ref eq 'SCALAR' }
    }
    return $err;
}

sub staged {
    my ($count, $batch) = @_;
    my $stot = my $rtot = $count;
    my $stage = 2;
    my $n;
    my $err = 0;
    while ($rtot) {
        if ($stot) {
            $n = $batch > $stot ? $stot : $batch;
            $stot -= $n;
            $RESP->request_raw(($CMD) x $n);
        }
        next if --$stage > 0;
        $n = $batch > $rtot ? $rtot : $batch;
        $rtot -= $n;
        for ($RESP->response($n)) { $err++ if ref eq 'SCALAR' }
    }
    return $err;
}

sub avail {
    my ($count, $batch) = @_;
    my $stot = my $rtot = $count;
    my $n;
    my $err = 0;
    while ($rtot) {
        if ($stot and $rtot - $stot <= $batch) {
            $n = $batch > $stot ? $stot : $batch;
            $stot -= $n;
            $RESP->request_raw(($CMD) x $n);
        }
        else { $RESP->await_response }
        for ($RESP->available_responses($batch)) { $rtot--; $err++ if ref eq 'SCALAR' }
    }
    return $err;
}

sub event {
    my ($count, $batch) = @_;
    my @block;
    my $err = 0;
    while ($count) {
        $batch = $count if $batch > $count;
        $count -= $batch;
        my $done = AE::cv();
        push @block, $done;
        $RESP->response_cv($batch)->cb(
            sub {
                for ($_[0]->recv) { $err++ if ref eq 'SCALAR' }
                $done->send;
            });
        $RESP->request_raw(($CMD) x $batch);
        shift(@block)->recv if @block > 1;
    }
    $_->recv for @block; # wait for completion
    return $err;
}

my %mode = (
    avail  => \&avail,
    basic  => \&basic,
    event  => \&event,
    fred   => \&fred,
    parse  => \&parse,
    redis  => \&redis,
    staged => \&staged,
    stream => \&stream,
    );

my $count = 1_000_000;
my $batch = 1_000; # items per batch
for (@ARGV) {
    if (m|^(\d*)/?(\d*)$|) {
        $count = $1 if $1;
        $batch = $2 if $2;
    }
    elsif (exists $mode{$_}) {
        print "$_: $count/$batch\n";
        my $t = time;
        my $e = $mode{$_}($count, $batch);
        my $rate = int($count / (time - $t));
        print "$rate/s #ERR=$e\n";
    }
    else { print "$_: no such mode\n" }
}
print "Done\n";
exit;
