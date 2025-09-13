use 5.010;
use strict;
use warnings;

package Socket::Stream::RESP2Client;
our $VERSION = '0.900';

use Socket::Stream;
use Socket::Stream::RESP2Parser;
use Time::Left qw(time_left);

# Enumerated fields for array-based object
use constant do {
    my $i = 0;
    my %enum = map { ($_ => $i++) } qw(
        SOCKET
        STREAM
        PARSER
        TIMEOUT
        TIMER
        PENDING
        _EXTEND
        );
    \%enum
};

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }

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
sub timeout { time_left($_[0][TIMEOUT] = $_[1]); $_[0] }
sub _start_timer { $_[0][TIMER] = time_left($_[0][TIMEOUT]) }

# Send one or more strings as messages; can be used for inline style.
sub request_raw {
    my $self = shift;
    $self->[STREAM]->timeout($self->[TIMEOUT]);
    $self->[STREAM]->send_msg(@_)
        or _die("Can't send request: $!");
    return $self;
}

# Send a request in the normal "array of bulk strings" style.
sub request {
    my $self = shift;
    my $n = @_;
    return $self->request_raw(
        "*$n", map { defined ? ('$'.length($_), $_) : '$-1' } @_
        );
}

sub response {
    my ($self, $count) = @_;
    $count //= 1;
    # Convert to an async request if any in progress
    return $self->response_cv($count)->recv
        if defined $self->[PENDING];
    return () unless $count > 0;
    my ($stream, $parser, $timer) = @$self[STREAM, PARSER, TIMER];
    $self->_start_timer;
    until ($parser->receive($count) == 1) {
        if (my $end = $stream->recv_end) { _die("Can't get response: $end") }
        $stream->timeout($timer->remaining);
        $stream->start_timer;
        $stream->await_data;
    }
    return $parser->take($count);
}

sub call {
    my $self = shift;
    return $self->request(@_)->response;
}

sub data_available {
    my ($self) = @_;
    $self->[STREAM]->recv_status; # obtain data if available
    return $self->[STREAM]->buffer_used;
}

### Receive data the hard way: non-blocking.

sub _next_pending {
    my ($self) = @_;
    my ($Parser, $Pending) = @$self[PARSER, PENDING]; # object attributes
    while (@$Pending) {
        my ($cv, $count) = @{shift @$Pending};
        $self->_start_timer;
        my (@result, $aet, $aeio, $done, $recurse);
        my $finish = sub { undef $aet; undef $aeio; $done = 1 };
        my $receive = sub {
            my $status;
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
        $aet = AE::timer($self->[TIMER]->remaining, 0,
                         sub { $Parser->error('timeout'); $receive->() })
            if $self->[TIMER]->is_limited;
        return;
    }
    undef $self->[PENDING];
    return;
}

sub response_cv {
    my ($self, $count) = @_;
    $count //= 1;
    _die("Invalid count '$count'")
        if $count =~ /\D/;
    my $cv = AE::cv();
    if ($self->[PENDING]) {
        push @{$self->[PENDING]}, [$cv, $count];
    }
    else {
        $self->[PENDING] = [[$cv, $count]];
        $self->_next_pending;
    }
    return $cv;
}

sub call_cv {
    my $self = shift;
    return $self->request(@_)->response_cv;
}

1;
__END__

=head1 NAME

Socket::Stream::RESP2Client - RESP2 (Redis) client using Socket::Stream

=head1 SYNOPSIS

TODO

=head1 DESCRIPTION

This is a simple low-level RESP2 (Redis) client interface based around
L<Socket::Stream> and L<Socket::Stream::RESP2Parser>.  It is low-level
in that is has no knowledge of any Redis commands, only the protocol
(RESP2) which communicates commands and responses.

Requests are always sent synchronously; responses may be received
synchronously or, with L<AnyEvent>, asynchronously.  One distinctive
feature of this module is the ability to pipeline commands to a much
greater extent than usual: you can even set up asynchronous response
handlers in advance, then send the associated requests.

=head1 METHODS

The module is object-oriented and has the following methods.  Note
that any exceptions or failures are generally unrecoverable: once you
encounter one, you should dispose of the object and its socket rather
than attempt further communication.

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
sending.  Blocking I/O is used, but the event loop will run while
waiting if L<AnyEvent> is in use.

=head2 request_raw

    $client = $client->request_raw(@strings);

Sends one or more @strings, each followed by CRLF, to the server, or
dies trying.  The request() method is written in terms of this, but
you can use it to send commands in the "inline" style if you want to.

=head2 response

    @data = $client->response($count);

Blocks and waits for $count responses (default 1), returning the data
as parsed by L<Socket::Stream::RESP2Parser>, or raising an exception
if that is not possible.  Refer to that class for details on the data
representation.  In a scalar context, returns the last item of @data.

=head2 call

    @data = $client->call(@args);

Sends @args as a request, waits for the response, and returns the
$data or raises an exception if an error prevents this.  In a scalar
context, returns the last item of @data.

=head2 response_cv

    $cv = $client->response_cv($count);

Asynchronous response handling: only available if you have loaded the
L<AnyEvent> module.  Differs from response() in that it does not block
and delivers the data via $cv, an L<AnyEvent> condition variable.

It is possible to queue response_cv() handlers: calling response_cv()
again before the previous one is complete results in the callbacks
being placed in a queue.  If any response results in a failure, all
remaining items in the queue fail in the same way.

The response() method can be called while response_cv() operations are
in progress: it will block until all pending responses are received;
you may use C<< $client->response(0) >> to wait for all asynchronous
requests to complete without fetching any more data.

=head2 timeout

    $client = $client->timeout($duration);

You can specify a time limit for request/response operations.  The
default is undef, meaning no limit.  The duration can be set to a
number of seconds or a time-unit string as accepted by to_seconds() in
L<Time::Left>; invalid values will result in an exception.  Operations
which exceed the time limit are aborted with an error.  In the case of
response_cb(), the time limit applies to each response individually,
representing the maximum wait for a callback once the operation is at
the head of the queue.  If you want to impose a time limit on a group
of operations, create a separate L<Time::Left> object or similar and
dynamically adjust this timeout.

=head2 data_available

    $count = $client->data_available;

Returns the number of bytes currently ready to read on the response
side of the socket.  This can be used as a cheap alternative to full
nonblocking operation: just do something else until data is available,
then call response().

=head2 socket

    $socket = $client->socket;

Provides access to the socket created at new().  If a connected socket
was provided at new(), this returns the same socket.

=head1 ERRORS

If a request or response method fails for any reason, consider the
whole RESP2 session failed beyond recovery.  You will need to start
from scratch with a new client object and socket if you want to
perform further operations.  Long-running applications should always
remain aware that servers need to restart occasionally, so loss of
connectivity should be handled gracefully.

