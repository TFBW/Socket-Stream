use 5.010;
use strict;
use warnings;

package Socket::Stream::RESP2Client;
our $VERSION = '0.999';

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
    my $n = grep { ref } @_;
    _die("BUG: request() args must be all scalars or arrayrefs")
        unless $n == 0 or $n == @_;
    my @data = $n > 0 ? @_ : [@_];
    return $self->request_raw(
        map {
            $n = @$_;
            "*$n", map { defined ? ('$'.length($_), $_) : '$-1' } @$_
        } @data
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
    my $n = grep { ref } @_;
    _die("BUG: call() args must be all scalars or arrayrefs")
        unless $n == 0 or $n == @_;
    my @data = $n > 0 ? @_ : [@_];
    my $timer = _timer($self->timeout);
    return $self->request(@data)->response(scalar(@data), $timer->remaining);
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
    my $n = grep { ref } @_;
    _die("BUG: call_cv() args must be all scalars or arrayrefs")
        unless $n == 0 or $n == @_;
    my @data = $n > 0 ? @_ : [@_];
    my $cv = $self->response_cv(scalar(@data));
    $self->request(@data);
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
    $client = $client->request(\@cmd1, \@cmd2, ...);
    $client = $client->request_raw(@strings);
    $data = $client->response;
    @data = $client->response($count);
    $data = $client->call(@args);
    @data = $client->call(\@cmd1, \@cmd2, ...);
    @data = $client->available_responses($count);
    $n = $client->available_responses($count);
    $client = $client->await_response($count);
    $cv = $client->response_cv($count);
    $cv = $client->call_cv(@args);
    $client = $client->timeout($duration);
    $seconds = $client->timeout;
    $socket = $client->socket;

=head1 DESCRIPTION

This is a simple low-level RESP2 (Redis) client interface based around
L<Socket::Stream> and L<Socket::Stream::RESP2Parser>.  It is mostly a
thin convenience layer over those modules, but adds some sophisticated
queue management of asynchronous responses.  It is low-level in that
it has no knowledge of Redis commands, only the protocol (RESP2) which
conveys them.

=head2 Design Principles

This module operates on the following core principles.

=head3 Fatal Errors

Errors are unrecoverable.  Once an error occurs (timeout, connection
loss, protocol violation), the entire session is considered failed.
You must create a new client object with a fresh socket to continue.
There are no retry mechanisms or error recovery procedures: the object
becomes more or less useless and will fail any further operations.

Long-running applications should handle connection failure gracefully,
as servers restart and networks fail.  The appropriate recovery method
will vary according to the application.  This also has implications
for batching of operations, as it will generally be unclear where in
the batch the failure occurred, exactly.

=head3 Synchronous Send, Flexible Receive

Requests are sent synchronously and block until complete.  Responses
can be received either synchronously (blocking) or asynchronously (via
L<AnyEvent> condition variables). This asymmetry enables sophisticated
pipeline management while keeping the sending side simple.

Note that actual blocking on send tends to be rare, and is handled in
such a way that async response handlers can still execute, but the
possibililty of blocking generally precludes requests from being sent
in async handler code executed by the event loop.  If you violate this
rule, it may result in an exception when blocking occurs.

=head3 Batching and Pipeline Control

The module supports aggressive pipelining - you can send many requests
before reading any responses.  Unlike most Redis clients, requests and
responses are independent and can support multiple operations in one
call, so you can efficiently maintain a level of outstanding requests
in the pipeline.  You can also set up asynchronous response handlers
before sending the associated requests, which is the optimal pattern
for preventing I/O deadlock.

The L</"EXAMPLES"> section includes demonstrations of these concepts
in greater detail.

=head3 Timeouts

Time limits can be imposed on most operations via the L</"timeout">
attribute, and also via a direct argument in some cases.  The default
behaviour is no time limit.  Timeouts are L</"Fatal Errors">, and
their intended use is to induce rapid failure where that is preferable
to lengthy pauses.

Depending on the exact time limits you wish to impose, however, it may
be simpler to manage them externally.  This is particularly likely
when a group of operations, rather than an individual one, is subject
to a time limit.  This module uses L<Time::Left> to track timeout
deadlines, so consider using it if you need something like it.

=head2 Synchronous/Asynchronous Interaction

The module supports both synchronous and (if L<AnyEvent> is loaded)
asynchronous responses, even in the same session.  This creates
potentially ambiguous semantics which require clarification, such as
what happens when a synchronous response is requested while an
asynchronous one is still outstanding.  The rules are as follows.

=head3 Async Operations Queue

When you call C<response_cv()>, the operation is added to a FIFO queue
and an L<AnyEvent> condition variable is returned to convey the future
result.  Arbitrarily many C<response_cv()> calls can be queued: each
operation awaits its turn, then waits for its required number of
responses.  Note that the behaviour of synchronous responses can vary
depending on whether async operations are in progress.

=head3 Sync Operations Wait

If you call C<response()> while async operations are queued, it blocks
until all queued operations complete, then receives its responses.
That is, it joins the queue.  You can use C<< $client->response(0) >>
as a synchronization point - it returns no data but waits for all
async operations in the queue at call time to finish.

=head3 Available Responses Check

The C<available_responses()> method returns empty immediately if any
async operations are pending.  It is a non-blocking operation, and no
responses are available to it until queued operations are satisfied,
particularly given that more operations could join the queue.  There's
no good reason to mix this with async operations, but there's no harm.

=head3 Timeout Behavior

The start of the time limit varies depending on the operation.  For
blocking operations, it simply refers to the entire operation: when it
is called to when it returns.  For C<response_cv()>, it doesn't start
until the operation reaches the head of the queue, which could be some
time after the method call.  For C<call_cv()>, the timeout is applied
separately to the request and response: the method itself may block to
send the request, and the response handler may be queued.

=head1 METHODS

The module is object-oriented and has the following methods.  Bear in
mind that exceptions or failures are generally unrecoverable: once you
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
    $client = $client->request(\@cmd1, \@cmd2, ...);

Sends a request to the server in the usual "array of bulk strings"
style or dies trying.  The contents of @args should either be strings
or undef (for NULL).  Strings must be byte-strings: wide chars should
be converted to a byte encoding before sending.  You can send a batch
of requests by passing a list of arrayrefs.  The operation will throw
an exception if it fails, including if it blocks for longer than the
L</"timeout"> value.

=head2 request_raw

    $client = $client->request_raw(@strings);

Sends one or more @strings, each followed by CRLF, to the server, or
dies trying.  The request() method is written in terms of this, but
you can use it to send commands in the "inline" style if you want to.
Same timeout/exception semantics as request().  Bear in mind that the
"_raw" suffix designates a responsibility on the caller's part to
provide @strings which are protocol-appropriate.

=head2 response

    @data = $client->response($count, $timeout);
    $data = $client->response;

Blocks and waits for $count responses (default 1), returning the data
as parsed by L<Socket::Stream::RESP2Parser>, or raising an exception
if that is not possible.  Refer to that class for details on the data
representation.  In scalar context, returns the B<last> item of @data
as per Perl C<slice()> semantics.

A $count of zero always returns an empty list but has the useful side
effect of blocking until all L</"response_cv"> operations complete,
like C<wait_all_responses()> in L<Redis>.  If a $timeout is not given,
the current L</"timeout"> is used.  The timeout does not start until
any queued C<response_cv()> operations are complete.

=head2 call

    $data = $client->call(@args);
    @data = $client->call(\@cmd1, \@cmd2, ...);

Combines C<request()> and C<response()> in a single call.  Sends @args
via C<request()> and then calls C<response()> with a matching count:
if the arrayref form is used with multiple requests, the same number
of resposes are expected.  The returned @data is as per C<response()>.
The timeout applies to the operation as a whole.

=head2 available_responses

    @data = $client->available_responses($count);
    $n = $client->available_responses($count);

This is a nonblocking operation which obtains responses only if they
are immediately available.  In a list context, it receives the actual
responses; in a scalar context it returns a count of responses which
could be requested without blocking.  If $count is a true value, it
sets an upper limit on the number of responses to receive or report;
if it's false, the limit is imposed by L<Socket::Stream::RESP2Parser>.
The main difference is that the open-ended version will parse as much
data as is available, whereas the $count-limited version will stop
parsing and return if the count is satisfied.

Note that this will immediately return zero/empty if called when any
C<response_cv()> operations are in progress.

=head2 await_response

    $client = $client->await_response($count, $timeout);

This is a simple blocking operation which returns when at least $count
responses are available, defaulting to 1 if false.  If there are any
C<response_cv()> operations in progress, this will wait for them to
complete before starting the timeout.  Uses L</"timeout"> if $timeout
is omitted.  Dies on timeout or other stream errors.

Use this in conjunction with C<available_responses()> if no responses
are available and you have nothing better to do than wait for one.

=head2 response_cv

    $cv = $client->response_cv($count, $timeout);

Asynchronous response handling: only available if you have loaded the
L<AnyEvent> module.  Differs from C<response()> in that it immediately
returns $cv, an L<AnyEvent> condition variable, via which it reports
the data.  If the requested number of responses can't be obtained due
to parser or stream failure, the $cv will croak.

Calling C<response_cv()> again before the previous one is complete is
permitted: the requests are queued in the natural FIFO order.  Using a
$count of zero is permitted and acts as a synchronisation point: no
data is returned, but the $cv sends an enpty list when the operation
reaches the head of the queue.

When C<response_cv()> operations are in progress, the C<response()>
method will pause until all queued operations complete, then receive
responses.  You may use C<< $client->response(0) >> to block until all
queued operations complete without receiving any further data.

=head2 call_cv

    $cv = $client->call_cv(@args);
    $cv = $client->call_cv(\@cmd1, \@cmd2, ...);

Combines C<request()> and C<response_cv()> in a single call.  Sends
@args via C<request()> and returns an L<AnyEvent> condition variable
to deliver the corresponding responses.  The response handler is set
up before the request is sent, which is best practice for avoiding I/O
deadlock.  The L</"timeout"> is applied independently to the request
and response parts as per C<request()> and C<response_cv()>.  This
means the method itself could die (error on request), or the $cv could
croak (error on response).

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
not a soft interrupt.  See the earlier sections on L</"Timeouts"> and
L</"Timeout Behaviour"> for more detail.

=head2 socket

    $socket = $client->socket;

Provides access to the socket created at new().  If a connected socket
was provided at new(), that socket is returned.

=head1 EXAMPLES

The following examples are variations on one in the L<Redis::Fast> POD
which demonstrates pipelined performance.  The workload consists of a
large number of small, fast operations.  These examples also serve as
a short tutorial on pipeline performance.

The examples will start with the most basic and build towards more
efficient and sophisticated solutions.  Each example is a function
which takes a total $count and $batch size to use when performing a
single operation (specifically "set hoge fuga") repeatedly.  The
function returns the number of errors encountered, a feature not in
the original L<Redis::Fast> example.  The original example did not
have batching, either: all the work was processed in one huge batch.

=head2 Redis Baseline

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

=head2 Basic Blocking

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

=head2 Staged Blocking

Both these examples violate the first rule of pipelining, however:
"keep your pipeline as small as possible without letting it run dry."
Both examples run dry at the end of each batch.  This means there is
idle time at the server while it waits for the next batch.  The next
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

=head2 Opportunistic Blocking

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

=head2 Staged Asynchronous

The last example is asynchronous and requires L<AnyEvent>.  This is
like the staged approach, but L<AnyEvent> condition variables are used
to limit the send rate.  Note that the response handler is set up
before the requests are sent - the textbook-correct way to prevent
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
