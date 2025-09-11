use 5.010;
use strict;
use warnings;

package Socket::Stream::RESP2Client;
our $VERSION = '0.001';

use Socket::Stream;
use Socket::Stream::RESP2Parser;
use Time::Left qw(time_left);

use constant {
    SOCKET  => 0,
    STREAM  => 1,
    TIMEOUT => 2,
    TIMER   => 3,
    PENDING => 4,
    _EXTEND => 5, # for inheritance
};

sub _NOOP { }

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
    my $stream = Socket::Stream->new($socket)
        ->on_send_err(sub { die "Send error: $!\n" })
        ->use_CRLF;
    return bless([$socket, $stream], ref($class)||$class);
}

sub socket { $_[0][SOCKET] }
sub timeout { time_left($_[0][TIMEOUT] = $_[1]); $_[0] }
sub _start_timer { $_[0][TIMER] = time_left($_[0][TIMEOUT]) }

# Send one or more strings as messages; can be used for inline style.
sub request_raw {
    my $self = shift;
    $self->[STREAM]->timeout($self->[TIMEOUT]);
    $self->[STREAM]->send_msg(@_)
        or die "Can't send request: $!\n";
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
    if (defined $self->[PENDING]) {
        # Convert to an async request and wait
        my $cv = AE::cv();
        $self->response_cb($cv, $count);
        return $cv->recv;
    }
    $self->_start_timer;
    my $stream = $self->[STREAM];
    my $parser = Socket::Stream::RESP2Parser->new($stream, $count);
    until ($parser->receive) {
        $stream->timeout($self->[TIMER]->remaining);
        $stream->start_timer;
        next if $stream->await_data;
        $parser->error($stream->recv_end);
    }
    return $parser;
}

sub call {
    my $self = shift;
    return $self->request(@_)->response->data_or_die;
}

sub data_available {
    my ($self) = @_;
    $self->[STREAM]->recv_status; # obtain data if available
    return $self->[STREAM]->buffer_used;
}

### Receive data the hard way: non-blocking.

sub _fail_pending {
    my ($self) = @_;
    while (my $p = shift @{$self->[PENDING]}) {
        $p->[0]->(Socket::Stream::RESP2Parser->new_failed);
    }
    undef $self->[PENDING];
    return;
}

sub _next_pending {
    my ($self) = @_;
    while (@{$self->[PENDING]}) {
        my ($cb, $count) = @{shift @{$self->[PENDING]}};
        $self->_start_timer;
        my $parser = Socket::Stream::RESP2Parser->new($self->[STREAM], $count);
        my ($timer, $watcher, $done);
        my $finish = sub { undef $timer; undef $watcher; $cb->($parser) };
        my $fail = sub { $parser->error(@_); &$finish };
        my $receive = sub { $done = $parser->receive; &$finish if $done };
        $receive->();
        next if $done > 0;
        return $self->_fail_pending if $done < 0;
        # Still here? We need to wait for data using AnyEvent.
        $watcher = AE::io($self->[SOCKET], 0, sub {
            $receive->();
            return if not $done and not $self->[STREAM]->recv_end; # continue
            return $self->_next_pending if $done > 0;              # success
            $fail->($self->_recv_err) unless $done;
            return $self->_fail_pending;
                          });
        $timer = AE::timer($self->[TIMER]->remaining, 0,
                           sub { $fail->('timeout'); $self->_fail_pending })
            if $self->[TIMER]->is_limited;
        return;
    }
    undef $self->[PENDING];
    return;
}

sub response_cb {
    my ($self, $cb, $count) = @_;
    $cb //= \&_NOOP;
    $count //= 1;
    if ($self->[PENDING]) {
        push @{$self->[PENDING]}, [$cb, $count];
    }
    else {
        $self->[PENDING] = [[$cb, $count]];
        $self->_next_pending;
    }
    return $self;
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
in that is has no knowledge of any Redis commands, only the RESP2
protocol which communicates commands and responses.

Requests are always sent synchronously; responses may be received
synchronously or, with L<AnyEvent>, asynchronously.  One distinctive
feature of this module is the ability to pipeline commands: you can
set up asynchronous response handlers in advance, then send the
associated requests.

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
sending.  Blocking IO is used, but the event loop will run while
waiting if L<AnyEvent> is in use.

=head2 request_raw

    $client = $client->request_raw(@strings);

Sends one or more @strings, each followed by CRLF, to the server, or
dies trying.  The request() method is written in terms of this, but
you can use it to send commands in the "inline" style if you want to.

=head2 response

    $parser = $client->response($count);

Blocks and waits for $count responses (default 1), returning the
L<Socket::Stream::RESP2Parser> object which received the responses.
Refer to that class for details on the parsing process, how to tell if
the process was successful, and how to access the data if it was.  Any
IO error encountered becomes an error condition on the $parser object.

=head2 call

    $data = $client->call(@args);

Sends @args as a request, waits for the response, and returns the
$data or raises an exception if an error prevents this.

=head2 response_cb

    $client = $client->response_cb($code, $count);

Asynchronous response handling: only available if you have loaded the
L<AnyEvent> module.  Differs from response() in that it does not block
and delivers the L<Socket::Stream::RESP2Parser> object as an argument
to the $code callback (i.e. C<< $code->($parser) >>) when parsing is
complete.  The callback will happen immediately (inside the method) if
possible, otherwise it will be called from an IO watcher when ready.
The IO watcher context puts restrictions on what the callback may do:
in particular it should not block or die.

It is possible to queue response_cb() handlers: calling response_cb()
again before the previous one is complete results in the callbacks
being placed in a queue.  If any response results in a failure, all
remaining items in the queue are called back immediately with a parser
object in an "aborted" error state, as recovery is not possible.

The response() method can be called while response_cb() operations are
in progress: it will block until all pending responses are received.

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

