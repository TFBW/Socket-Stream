use 5.010;
use strict;
use warnings;

package Socket::Stream::RESP2Client;
our $VERSION = '0.990';

use Socket::Stream;
use Socket::Stream::RESP2Parser;
use Time::Left qw(to_seconds);

# Enumerated fields for array-based object
use constant do {
    my $i = 0;
    my %enum = map { ($_ => $i++) } qw(
        SOCKET
        STREAM
        PARSER
        TIMEOUT
        PENDING
        _EXTEND
        );
    \%enum
};

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }
sub _timeout { defined($_[0]) ? to_seconds($_[0]) // _die("Invalid timeout '$_[0]'") : undef }
sub _timer { Time::Left->new(_timeout($_[0])) }

# Arg is socket or host:port; default local Redis
sub new {
    my ($class, $socket) = @_;
    unless (ref $socket) {
        $socket //= '';
        my ($host, $port) = split(/:/, $socket, 2);
        $host ||= '127.0.0.1';
        $port ||= 6379; # use default Redis port if empty or zero
        $socket = Socket::Stream::INET("$host:$port");
    }
    my $stream = Socket::Stream->new($socket);
    my $parser = Socket::Stream::RESP2Parser->new($stream);
    return bless([$socket, $stream, $parser], ref($class)||$class);
}

sub socket { $_[0][SOCKET] }
sub timeout { @_ == 1 ? $_[0][TIMEOUT] : do { $_[0][TIMEOUT] = _timeout($_[1]); $_[0] } }

sub request_raw {
    my $self = shift;
    $self->[STREAM]->timeout($self->[TIMEOUT]);
    $self->[STREAM]->send_msg(@_)
        or _die("Can't send request: $!");
    return $self;
}

sub request {
    my $self = shift;
    my $n = @_;
    return $self->request_raw(
        "*$n", map { defined ? ('$'.length($_), $_) : '$-1' } @_
        );
}

sub response {
    my ($self, $count, $timeout) = @_;
    $count //= 1;
    $timeout = $self->[TIMEOUT] if @_ < 3;
    # Convert to an async request if any in progress
    return $self->response_cv($count, $timeout)->recv
        if defined $self->[PENDING];
    return () unless $count > 0;
    return $self->await_response($count, $timeout)->[PARSER]->take($count);
}

sub call {
    my $self = shift;
    my $timer = _timer($self->timeout);
    return $self->request(@_)->response(1, $timer->remaining);
}

sub available_responses {
    my ($self, $count) = @_;
    return wantarray ? () : 0
        if defined $self->[PENDING];
    my $status = $self->[PARSER]->receive($count);
    return $self->[PARSER]->take($count || $self->[PARSER]->count)
        if wantarray;
    return $count && $status == 1 ? $count : $self->[PARSER]->count;
}

sub await_response {
    my ($self, $count, $timeout) = @_;
    $count ||= 1;
    $timeout = $self->[TIMEOUT] if @_ < 3;
    $self->response_cv(0, $timeout)->recv
        if defined $self->[PENDING];
    my $timer = _timer($timeout);
    until ($self->[PARSER]->receive($count) == 1) {
        if (my $end = $self->[STREAM]->recv_end) { _die("Stream ended: $end") }
        $self->[STREAM]->timeout($timer->remaining)->start_timer;
        $self->[STREAM]->await_data;
    }
    return $self;
}

### Receive data the hard way: non-blocking.

sub _next_pending {
    my ($self) = @_;
    my ($Parser, $Pending) = @$self[PARSER, PENDING]; # object attributes
    while (@$Pending) {
        my ($cv, $count, $timeout) = @{shift @$Pending};
        my $timer = _timer($timeout);
        my (@result, $aet, $aeio, $done, $recurse, $status);
        my $finish = sub { undef $aet; undef $aeio; $done = 1 };
        my $receive = sub {
            do {
                $status = $Parser->receive;
                push @result, $Parser->take($count - @result);
            } while $status == 1 and $count > @result;
            if ($count == @result) {
                $cv->send(@result);
                &$finish;
                $self->_next_pending if $recurse;
            }
            elsif (my $err = $Parser->error) {
                $cv->croak($err);
                while (my $p = shift @$Pending) { $p->[0]->croak($err) }
                &$finish;
            }
            return;
        };
        $receive->();
        next if $done;
        # Still here? We need to wait for data using AnyEvent.
        $recurse = 1;
        $aeio = AE::io($self->[SOCKET], 0, $receive);
        $aet = AE::timer($timer->remaining, 0,
                         sub { $Parser->error('timeout'); $receive->() })
            if $timer->is_limited;
        return;
    }
    undef $self->[PENDING];
    return;
}

sub response_cv {
    my ($self, $count, $timeout) = @_;
    $count //= 1;
    $timeout = $self->[TIMEOUT] if @_ < 3;
    _die("Invalid count '$count'")
        if $count =~ /\D/;
    _die("response_cv() requires AnyEvent")
        unless exists &AE::cv;
    my $cv = AE::cv();
    if ($self->[PENDING]) {
        push @{$self->[PENDING]}, [$cv, $count, $timeout];
    }
    else {
        $self->[PENDING] = [[$cv, $count, $timeout]];
        $self->_next_pending;
    }
    return $cv;
}

sub call_cv {
    my $self = shift;
    my $cv = $self->response_cv(1);
    $self->request(@_);
    return $cv;
}

1;
__END__

=head1 NAME

Socket::Stream::RESP2Client - RESP2 (Redis) client using Socket::Stream

=head1 SYNOPSIS

    use Socket::Stream::RESP2Client;
    $client = Socket::Stream::RESP2Client->new($socket);
    $client = $client->request(@args);
    $client = $client->request_raw(@strings);
    @data = $client->response($count);
    $data = $client->response;
    $data = $client->call(@args);
    @data = $client->available_responses($count);
    $n = $client->available_responses($count);
    $client = $client->await_response($count);
    $cv = $client->response_cv($count);
    $cv = $client->call_cv(@args);
    $client = $client->timeout($duration);
    $socket = $client->socket;

=head1 DESCRIPTION

This is a simple low-level RESP2 (Redis) client interface based around
L<Socket::Stream> and L<Socket::Stream::RESP2Parser>.  It is low-level
in that is has no knowledge of any Redis commands, only the protocol
(RESP2) which conveys them.  This is a fairly thin convenience layer
over L<Socket::Stream> and L<Socket::Stream::RESP2Parser> except for
the management of asynchronous responses via L</"response_cv">, which
adds substantial logic.

Requests are always sent synchronously; responses may be received
synchronously or, with L<AnyEvent>, asynchronously.  One distinctive
feature of this module is the ability to pipeline commands to a much
greater extent than usual: you can even set up asynchronous response
handlers in advance, then send the associated requests.  This pattern
is highly efficient and can outperform L<Redis::Fast> in some cases.

=head1 METHODS

The module is object-oriented and has the following methods.  Note
that any exceptions or failures are generally unrecoverable: once you
encounter one, you should dispose of the object and its socket rather
than attempt further communication.  "Blocking" operations allow the
event loop to run if the L<AnyEvent> module is loaded.

=head2 new

    $client = Socket::Stream::RESP2Client->new($socket);

Creates a new $client object.  The $socket can be either an existing
connected socket, or an INET "host:port" spec, in which case the host
part defaults to 127.0.0.1 and the port defaults to 6379, the default
Redis port.  If $socket is undef or empty, both defaults are used.  An
exception is raised if socket connection fails.

=head2 request

    $client = $client->request(@args);

Sends a request to the server in the usual "array of bulk strings"
style or dies trying.  The contents of @args should be byte-strings:
any strings which may contain wide chars should be converted before
sending.  The operation will throw an exception if it fails, including
if it blocks for longer than the L</"timeout"> value.

=head2 request_raw

    $client = $client->request_raw(@strings);

Sends one or more @strings, each followed by CRLF, to the server, or
dies trying.  The request() method is written in terms of this, but
you can use it to send commands in the "inline" style if you want to.
Same timeout/exception semantics as request().

=head2 response

    @data = $client->response($count, $timeout);
    $data = $client->response;

Blocks and waits for $count responses (default 1), returning the data
as parsed by L<Socket::Stream::RESP2Parser>, or raising an exception
if that is not possible.  Refer to that class for details on the data
representation.  In scalar context, returns the B<last> item of @data.

A $count of zero always returns an empty list but has the useful side
effect of blocking until all L</"response_cv"> operations complete,
like wait_all_responses() in L<Redis>.  If a $timeout is not given,
the current L</"timeout"> is used.  The timeout does not start until
any queued response_cv() operations are complete.

=head2 call

    $data = $client->call(@args);

Sends @args via request(), waits for a single response, and returns
the response $data or raises an exception if an error prevents this or
if the total execution time reaches the L</"timeout"> value.

=head2 available_responses

    @data = $client->available_responses($count);
    $n = $client->available_responses($count);

This is a nonblocking operation which obtains responses only if they
are immediately available.  In a list context, it receives the actual
responses; in a scalar context it returns a count of responses which
could be requested without blocking.  If $count is a true value, it
sets an upper limit on the number of responses to receive or report;
if it's false, the upper limit is imposed by the parser.  The main
difference is that the open-ended version will parse as much data as
is available, whereas the $count-limited version will stop parsing and
return if the count is satisfied.

Note that this will immediately return zero/empty if called when any
response_cv() operations are in progress.

=head2 await_response

    $client = $client->await_response($count, $timeout);

This is a simple blocking operation which returns when at least $count
responses are available, defaulting to 1 if false.  If there are any
response_cv() operations in progress, this will wait for them to
complete before starting the timeout.  Uses L</"timeout"> if $timeout
is omitted.  Dies on timeout or other stream errors.

Use this in conjunction with available_responses() if no responses are
available and you have nothing better to do than wait for one.

=head2 response_cv

    $cv = $client->response_cv($count, $timeout);

Asynchronous response handling: only available if you have loaded the
L<AnyEvent> module.  Differs from response() in that it does not block
and delivers the data via $cv, an L<AnyEvent> condition variable.  If
the requested number of responses can't be obtained due to parser or
stream failure, the $cv will croak.

Calling response_cv() again before the previous one is complete is
permitted: the requests are queued in the natural FIFO order.  Using a
$count of zero is permitted and acts as a synchronisation point: no
data is returned, but the $cv sends when the operation reaches the
head of the queue.

The response() method can also be called when response_cv() operations
are in progress: it will wait until all queued operations complete,
then receive responses.  You may use C<< $client->response(0) >> as a
synchronisation point: it returns no data but blocks until all queued
operations complete.

=head2 call_cv

    $cv = $client->call_cv(@args);

Sends @args via request() and returns an L<AnyEvent> condition
variable to deliver the response data.  The response handler is set up
before the request is sent, which is best practice for avoiding I/O
deadlock.  The L</"timeout"> is applied independently to the request
and response parts due to their asynchronous operation.

=head2 timeout

    $client = $client->timeout($duration);
    $seconds = $client->timeout;

This is a get/set attribute for the time limit on blocking operations.
Where practical, operations also allow the timeout to be specified on
a case-by-case basis as a parameter, in which case this value is the
default.  The initial value is undef, meaning no limit.  The $duration
can be set to a number of seconds or a time-unit string as accepted by
to_seconds() in L<Time::Left>; invalid values result in an exception.

Operations which exceed the time limit are aborted with an error.
Bear in mind that a timeout is a fatal error on the underlying stream,
not a soft interrupt.  In the case of response_cv(), the timer starts
when it reaches the head of the queue.

If you want to impose a timeout on a group of response() operations,
dynamically adjust this timeout between calls using a L<Time::Left>
object or similar.  Asynchronous response_cv() calls are harder to
manage in this way because they are usually set up in advance.  In an
asynchronous context it may be simpler to impose the limit with a
separate L<AnyEvent> timer that shuts down the stream or similar.

=head2 socket

    $socket = $client->socket;

Provides access to the socket created at new().  If a connected socket
was provided at new(), that socket is returned.

=head1 ERRORS

If a request or response method fails for any reason, consider the
whole RESP2 session failed beyond recovery.  You will need to start
from scratch with a new client object and socket if you want to
perform further operations.  Long-running applications should always
remain aware that servers need to restart occasionally, so loss of
connectivity should be handled gracefully.

=head1 EXAMPLES

The following examples are variations on one in the L<Redis::Fast> POD
which demonstrates pipelined performance.  The workload consists of a
large number of small, fast operations.  These examples also serve as
a short tutorial on pipeline performance.

The examples will start with the most basic and build towards more
efficient and sophisticated solutions.  Each example is a function
which takes a total $count and $batch size to use when performing a
single operation, specifically "set hoge fuga", repeatedly.  The
function returns the number of errors encountered, which is a feature
not present in the original L<Redis::Fast> example.  The original
example did not have batching, either: all the work was processed in a
single huge batch.

The first example is a translation of the original L<Redis::Fast>
example code into this batched-subroutine format.

    use Redis::Fast;
    my $REDIS = Redis::Fast->new;
    sub redis {
        my ($count, $batch) = @_;
        my $err = 0;
        while ($count) {
            $batch = $count if $batch > $count;
            $count -= $batch;
            $REDIS->set(hoge => 'fuga', sub { $err++ if defined $_[1] })
                for 1..$batch;
            $REDIS->wait_all_responses;
        }
        return $err;
    }

This is fairly straightforward: the total load is broken up into
batches of the specified size, with the last batch taking the remains.
The batch of commands is sent, and we await the responses.  Each batch
is a simple pipeline where everything is added to the pipeline, then
everything is extracted from it.  Larger batches use more memory.

Here's the same concept in terms of this module.

    use Socket::Stream::RESP2Client;
    my $RESP = Socket::Stream::RESP2Client->new;
    sub basic {
        my ($count, $batch) = @_;
        my $err = 0;
        while ($count) {
            $batch = $count if $batch > $count;
            $count -= $batch;
            $RESP->request_raw(("set hoge fuga") x $batch);
            for ($RESP->response($batch)) { $err++ if ref eq 'SCALAR' }
        }
        return $err;
    }

The key difference is that we have separate methods for sending
requests and receiving responses, each called once par batch.  The
larger the batch size, the more memory used, but even small batch
sizes (e.g. 10) improve throughput significantly.  This module
performs comparably to L<Redis::Fast> in this context, getting better
with larger batch sizes.  The speed of L<Redis::Fast> is mostly a
factor of its parser, and there's not a lot to parse here.

Both these examples violate the first rule of pipelining, however:
"keep your pipeline as small as possible without letting it run dry."
Both examples run dry at the end of each batch.  This means there is
idle time at the server whille it waits for the next batch.  The next
example solves this by having a "staged" approach where the response
processing of the first batch is postponed until after the second
batch of requests has been sent.  Requests and responses are counted
separately to acommodate this.

    use Socket::Stream::RESP2Client;
    my $RESP = Socket::Stream::RESP2Client->new;
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
                $RESP->request_raw(("set hoge fuga") x $n);
            }
            next if --$stage > 0;
            $n = $batch > $rtot ? $rtot : $batch;
            $rtot -= $n;
            for ($RESP->response($n)) { $err++ if ref eq 'SCALAR' }
        }
        return $err;
    }

This is quite an efficient pattern, and my development environment was
able to drive Redis to near 100% CPU utilisation with a batch size as
small as 100, making Redis itself the bottleneck.  L<Redis::Fast>
can't reach that kind of throughput with this workload: its parser
performance doesn't help given the simple "+OK" responses expected.

A variation on this pattern is to read available responses, sending
another batch of requests when the total number in the pipeline is one
batch or less.  Performance-wise this isn't much different from the
simple staged approach.  Response-counting is changed to handle the
variation in response numbers, and the number of responses processed
is limited to one batch so we don't neglect the send side too long.

    use Socket::Stream::RESP2Client;
    my $RESP = Socket::Stream::RESP2Client->new;
    sub avail {
        my ($count, $batch) = @_;
        my $stot = my $rtot = $count;
        my $n;
        my $err = 0;
        while ($rtot) {
            if ($stot and $rtot - $stot <= $batch) {
                $n = $batch > $stot ? $stot : $batch;
                $stot -= $n;
                $RESP->request_raw(("set hoge fuga") x $n);
            }
            else { $RESP->await_response }
            for ($RESP->available_responses($batch)) {
                $rtot--;
                $err++ if ref eq 'SCALAR';
            }
        }
        return $err;
    }

The last example is asynchronous and requires L<AnyEvent>.  This is
like the staged approach, but L<AnyEvent> condition variables are used
to limit the send rate.  Note that the response handler is set up
before the requests are sent -- the textbook-correct way to prevent
communications deadlock.

    use Socket::Stream::RESP2Client;
    my $RESP = Socket::Stream::RESP2Client->new;
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
            $RESP->request_raw(("set hoge fuga") x $batch);
            shift(@block)->recv if @block > 1;
        }
        $_->recv for @block; # wait for completion
        return $err;
    }

=head1 SEE ALSO

L<Socket::Stream> performs the low-level I/O operations.

L<Socket::Stream::RESP2Parser> performs the response-parsing.

Numerous other Redis modules on CPAN, most of them patterned after
Redis.pm (L<Redis>).

L<AnyEvent> is the event-loop abstraction library required in order to
use the L</"response_cv"> async response method.

=head1 LICENSE AND COPYRIGHT

This software is Copyright (c) 2025 by Brett Watson.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
