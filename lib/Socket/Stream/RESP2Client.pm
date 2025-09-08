use 5.010;
use strict;
use warnings;

package Socket::Stream::RESP2Client;
our $VERSION = '0.001';

use if $ENV{DEBUG} => 'Debug::Comments';

use Socket::Stream;
use Socket::Stream::RESP2Parser;
use Time::Left qw(time_left);

use constant {
    SOCKET  => 0,
    STREAM  => 1,
    TIMEOUT => 2,
    TIMER   => 3,
    PENDING => 4,
};

sub _NOOP { }
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
    $self->[STREAM]->send_msg(@_);
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

sub _await_data {
    my ($self) = @_;
    $self->[STREAM]->timeout($self->[TIMER]->remaining);
    $self->[STREAM]->start_timer;
    return $self->[STREAM]->await_data;
}

sub response {
    my ($self, $count) = @_;
    $count //= 1;
    die "Can't use response() while response_cb() requests in progress.\n"
        if defined $self->[PENDING];
    $self->[STREAM]->on_recv_err(sub { die "Recv error: $!\n" });
    $self->_start_timer;
    my $parser = Socket::Stream::RESP2Parser->new($self->[STREAM], $count);
    until ($parser->receive) {
        $self->_await_data
            or die "Server closed connection\n";
    }
    $self->[STREAM]->on_recv_err();
    return $parser->data_or_die;
}

sub call {
    my $self = shift;
    return $self->request(@_)->response;
}

# Returns a byte count, but works as a boolean too, as in
# $datum = $self->response if $self->data_available;
sub data_available {
    my ($self) = @_;
    $self->[STREAM]->recv_status; # obtain data if available
    return $self->[STREAM]->buffer_used;
}

### Receive data the hard way: non-blocking.

sub _fail_pending {
    my ($self) = @_;
    while (my $p = shift @{$self->[PENDING]}) { $p->[1]->("") }
    undef $self->[PENDING];
    return;
}

# Call via _trampoline; returns continuation code (if any).  Wrap your
# head around the _trampoline approach to continuation passing before
# trying to read this code.  All the sub-subs are trampolined except
# for the timer and watcher subs, which are called by the event loop.
sub _next_pending {
    my ($self) = @_;
    if (@{$self->[PENDING]} == 0) {
        undef $self->[PENDING];
        return;
    }
    my ($okcb, $ngcb, $count) = @{shift @{$self->[PENDING]}};
    $self->_start_timer;
    my ($timer, $watcher, $resolve);
    $resolve = sub { undef $timer; undef $watcher; return @_ };
    my $parser = Socket::Stream::RESP2Parser->new($self->[STREAM], $count);
    my $succeed = sub { $okcb->($parser->data); sub { $self->_next_pending } };
    my $fail = sub { $ngcb->(@_); sub { $self->_fail_pending } };
    my $receive = sub {
        my $status = $parser->receive;
        return
            $status > 0 ? ($resolve, $succeed) :
            $status < 0 ? ($resolve, $fail, $parser->error) :
            ();
    };
    my @outcome = $receive->();
    return @outcome if @outcome;
    # Still here? We need to wait for data using AnyEvent.
    $watcher = AE::io($self->[SOCKET], 0, sub {
        if ($self->[STREAM]->recv_status == 0) {
            my $error = $self->[STREAM]->recv_err || "connection closed";
            return _trampoline($resolve, $fail, $error);
        }
        return _trampoline($receive->());
                      });
    $timer = AE::timer($self->[TIMER]->remaining, 0,
                       sub { _trampoline($resolve, $fail, 'timeout') })
        if $self->[TIMER]->is_limited;
    return;
}

# Call code which optionally returns the next code and args to call.
sub _trampoline {
    my ($code, @args) = @_;
    while ($code) { ($code, @args) = $code->(@args) } # boing!
    return;
}

sub response_cb {
    my ($self, $okcb, $ngcb, $count) = @_;
    $okcb //= \&_NOOP;
    $ngcb //= \&_NOOP;
    $count //= 1;
    if ($self->[PENDING]) {
        push @{$self->[PENDING]}, [$okcb, $ngcb, $count];
    }
    else {
        $self->[PENDING] = [[$okcb, $ngcb, $count]];
        _trampoline(sub { $self->_next_pending });
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
L<Socket::Stream>.  It is low-level in that is has no knowledge of any
Redis commands, only the RESP2 protocol which communicates commands
and responses.

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
style.  The contents of @args should be byte-strings: any strings
which may contain wide chars should be converted before sending.
Blocking IO is used, but the event loop will run while waiting if
L<AnyEvent> is in use.

=head2 request_raw

    $client = $client->request_raw(@strings);

Sends one or more @strings, each followed by CRLF, to the server.  The
request() method is written in terms of this, but you can use it to
send commands in the "inline" style if you want to.

=head2 response

    $data = $client->response;
    @data = $client->response($count);

Blocks and waits for $count responses (default 1), returning them as
Perl data.  If called in a scalar context, you get the first response.
See L<Socket::Stream::RESP2Parser> for details on how RESP2 data is
converted to Perl data, but the short version is that it's all done in
the obvious way except for error strings which are converted to scalar
references.  An exception is raised if there is any kind of protocol
or communications error.

=head2 call

    $data = $client->call(@args);

Shortcut for C<< $data = $client->request(@args)->response; >>.

=head2 response_cb

    $client->response_cb($okcb, $ngcb, $count);

Asynchronous response handling: requires L<AnyEvent>.  The first two
arguments are CODE references to call back in case of success ($okcb)
or failure ($ngcb).  If either is undef, it is considered a no-op, but
you'll generally want both.  The $count argument is the number of
responses to expect, as per response(), defaulting to 1.  The module
will call C<< $okcb->(@data) >> with @data as per the response()
method, or C<< $ngcb->($reason) >> with $reason as an error message.
The callback will happen immediately if possible; otherwise it will be
called from the event loop when ready.  The callbacks should be
exception-free because of the event loop.

It is possible to queue response_cb() handlers: calling response_cb()
again before the previous one is complete results in the callbacks
being placed in a queue.  If any response results in a failure, any
remaining items in the queue are failed immediately with empty string
as the reason.

Calls to response() are not permitted while response_cb() operations
are in progress.

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

