use 5.010;
use strict;
use warnings;

package Socket::Stream::RESP2Client;
our $VERSION = '0.001';

use if $ENV{DEBUG} => 'Debug::Comments';

use Socket::Stream;
use Time::Left qw(time_left);

use constant {
    SOCKET  => 0,
    STREAM  => 1,
    TIMEOUT => 2,
    TIMER   => 3,
    PENDING => 4,
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

sub _recv_msg {
    my ($self, $n) = @_;
    $self->[STREAM]->timeout($self->[TIMER]->remaining);
    return defined($n) ?
        $self->[STREAM]->recv_data($n) :
        $self->[STREAM]->recv_msg;
}

sub _recv_datum {
    my ($self) = @_;
    my $data = $self->_recv_msg
        or die "Empty message\n";
    #@! Received msg '$data'
    my $type = substr($data, 0, 1, '');
    return $data  if $type eq '+' or $type eq ':';
    return \$data if $type eq '-';
    die "Unrecognised data type\n"
        unless $type eq '$' or $type eq '*';
    # Array/bulk string only beyond this point
    return undef if $data eq '-1';
    die "Invalid count '$data'\n"
        if $data =~ /\D/;
    my $n = $data;
    if ($type eq '$') {
        $data = $self->_recv_msg($n);
        die "Bulk string parse error\n"
            unless $self->_recv_msg eq '';
    }
    else {
        #@! Start array size $n
        $data = [];
        push @$data, $self->_recv_datum # recurse
            for 1..$n;
        #@! End array size $n
    }
    return $data;
}

sub response {
    my ($self) = @_;
    die "Can't use response() while response_cb() requests in progress.\n"
        if defined $self->[PENDING];
    $self->[STREAM]->on_recv_err(sub { die "Recv error: $!\n" });
    $self->_start_timer;
    my $data = $self->_recv_datum;
    $self->[STREAM]->on_recv_err();
    return $data;
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

# $okcb->() when ready to read socket; $ngcb->($reason) immediately if
# recv stream has ended or time is up, later if timer expires.
sub _io_cb {
    my ($self, $okcb, $ngcb) = @_;
    return $ngcb->("end of data")
        if $self->[STREAM]->recv_eof;
    return $ngcb->($self->[STREAM]->recv_err)
        if $self->[STREAM]->recv_err;
    return $ngcb->("too late to wait")
        if $self->[TIMER]->expired;
    my ($t, $w);
    # $next keeps $t and $w alive until called
    my $next = sub { undef $t; undef $w; shift->(@_) };
    # $t and $w keep $next alive
    $t = AE::timer($self->[TIMER]->remaining, 0, sub { $next->($ngcb, "timeout") })
        if $self->[TIMER]->is_limited;
    $w = AE::io($self->[SOCKET], 0, sub { $next->($okcb) });
    return;
}

# $okcb->($data) when empty message received; $ngcb->($reason) when
# non-empty message received or other error.
sub _recv_empty_msg_cb {
    my ($self, $data, $okcb, $ngcb) = @_;
    my $msg = $self->[STREAM]->recv_msg_nb;
    return $msg eq '' ? $okcb->($data) : $ngcb->("expected CRLF")
        if defined $msg;
    #@! Still awaiting bulk string terminator
    return $self->_io_cb(
        sub { $self->_recv_empty_msg_cb($data, $okcb, $ngcb) }, # retry
        $ngcb,
        );
}

# Reads $n bytes then passes to _recv_empty_msg_cb
sub _recv_bulk_cb {
    my ($self, $n, $okcb, $ngcb) = @_;
    my $data = $self->[STREAM]->recv_data_nb($n);
    return $self->_recv_empty_msg_cb($data, $okcb, $ngcb)
        if defined $data;
    #@! recv_bulk_cb got no data
    return $self->_io_cb(
        sub { $self->_recv_bulk_cb($n, $okcb, $ngcb) }, # retry
        $ngcb,
        );
}

# Uses _recv_datum_cb to push $n items into $array, then calls
# $okcb->($array).
sub _recv_array_cb {
    my ($self, $n, $array, $okcb, $ngcb) = @_;
    return $okcb->($array)
        unless $n > 0;
    return $self->_recv_datum_cb(
        sub {
            push @$array, $_[0];
            $self->_recv_array_cb($n - 1, $array, $okcb, $ngcb);
        },
        $ngcb,
        );
}

# Read one datum then call $okcb->($datum); may use _recv_array_cb or
# _recv_bulk_cv.  Calls $ngcb->($reason) on error.
sub _recv_datum_cb {
    my ($self, $okcb, $ngcb) = @_;
    my $data = $self->[STREAM]->recv_msg_nb;
    if (defined $data) {
        #@! get_datum_cb got '$data'
        my $type = substr($data, 0, 1, '');
        return $okcb->($data)
            if $type eq '+' or $type eq ':';
        return $okcb->(\$data)
            if $type eq '-';
        return $ngcb->("unrecognised data type")
            unless $type eq '$' or $type eq '*';
        return $okcb->(undef)
            if $data eq '-1';
        return $ngcb->("invalid count")
            if $data =~ /\D/;
        return $self->_recv_bulk_cb($data, $okcb, $ngcb)
            if $type eq '$';
        #@! Start array size $data
        return $self->_recv_array_cb($data, [], $okcb, $ngcb);
    }
    #@! get_datum_cb got no data
    return $self->_io_cb(
        sub { $self->_recv_datum_cb($okcb, $ngcb) }, # retry
        $ngcb,
        );
}

sub _fail_pending {
    my ($self) = @_;
    while (my $p = shift @{$self->[PENDING]}) {
        $p->[1]->("");
    }
    undef $self->[PENDING];
    return;
}

sub _next_pending {
    my ($self) = @_;
    if (@{$self->[PENDING]} == 0) {
        undef $self->[PENDING];
        return;
    }
    my ($okcb, $ngcb) = @{shift @{$self->[PENDING]}};
    $self->_start_timer;
    return $self->_recv_datum_cb(
        sub { $okcb->(@_); $self->_next_pending }, # recurse
        sub { $ngcb->(@_); $self->_fail_pending },
        );
}

sub response_cb {
    my ($self, $okcb, $ngcb) = @_;
    $okcb //= \&_NOOP;
    $ngcb //= \&_NOOP;
    if ($self->[PENDING]) {
        push @{$self->[PENDING]}, [$okcb, $ngcb];
    }
    else {
        $self->[PENDING] = [[$okcb, $ngcb]];
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

Blocks and waits for one response, returning it as Perl data.  RESP2
has data models for arrays, null, and various scalars.  Arrays are
returned as ARRAY refs; null is returned as undef; integers and
strings are returned as scalars.  Error strings are returned as scalar
references to distinguish them from normal strings.  An exception is
raised if there is any kind of communications or protocol error.

=head2 call

    $data = $client->call(@args);

Shortcut for C<< $data = $client->request(@args)->response; >>.

=head2 response_cb

    $client->response_cb($okcb, $ngcb);

Asynchronous response handling: requires L<AnyEvent>.  The arguments
are CODE references to call back in case of success ($okcb) or failure
($ngcb).  If either is undef, it is considered a no-op, but you'll
generally want both.  The module will call C<< $okcb->($data) >> with
$data as per the response() method, or C<< $ngcb->($reason) >> with
$reason as an error message.  The callback will happen immediately if
possible; otherwise it will be called from the event loop when ready.
The callbacks should be exception-free because of the event loop.

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

