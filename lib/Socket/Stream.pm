use 5.010;
use strict;
use warnings;

package Socket::Stream;
our $VERSION = '0.900';

use if $ENV{DEBUG} => 'Debug::Comments';

use Errno ();
use IO::Socket (); # loads INET and UNIX (if supported)
use Scalar::Util qw(dualvar);
use Socket ();
use Time::Left qw(to_seconds);

use constant DEFAULT_MAX_READ => 2 ** 18; # 256K
use constant EBADF
    => exists(&Errno::EBADF) ? ($! = Errno::EBADF()) : dualvar(199, "Bad file descriptor");
use constant EMSGSIZE
    => exists(&Errno::EMSGSIZE) ? ($! = Errno::EMSGSIZE()) : dualvar(200, "Message too long");
use constant ETIMEDOUT
    => exists(&Errno::ETIMEDOUT) ? ($! = Errno::ETIMEDOUT()) : dualvar(201, "Timed out");
use constant MSG_NOSIGNAL
    => exists(&Socket::MSG_NOSIGNAL) ? Socket::MSG_NOSIGNAL() : 0;
use constant SO_NOSIGPIPE
    => exists(&Socket::SO_NOSIGPIPE) ? Socket::SO_NOSIGPIPE() : 0;

# Enumerated fields for array-based object
use constant do {
    my $i = 0;
    my %enum = map { ($_ => $i++) } qw(
        DELIM
        EOF
        MAX_READ
        ON_EOF
        ON_RERR
        ON_SERR
        RECV
        RERR
        SERR
        SOCK
        TIMEOUT
        TIMER
        VEC
        _EXTEND
        );
    \%enum
};

# Not-quite-constants
sub BLOCKED  { $!{EAGAIN} || $!{EWOULDBLOCK} }
sub USING_AE { exists(&AE::io) }

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }

### Functions

sub _make_io_socket {
    my ($class, %args) = @_;
    $args{Type} = Socket::SOCK_STREAM();
    return $class->new(%args) || _die("Can't create $class: $IO::Socket::errstr");
}

sub INET {
    unshift @_, 'PeerAddr' if @_ == 1;
    return _make_io_socket('IO::Socket::INET', @_);
}

sub UNIX {
    unshift @_, 'Peer' if @_ == 1;
    return _make_io_socket('IO::Socket::UNIX', @_);
}

sub pair {
    my ($l, $r);
    package Socket; # for the consts
    socketpair($l, $r, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or _die("Can't create socket pair: $!");
    return ($l, $r);
}

### Class methods

sub new {
    my ($class, $sock) = @_;
    $sock->blocking(0);
    $sock->sockopt(SO_NOSIGPIPE, 1)
        if SO_NOSIGPIPE; # avoid SIGPIPE if possible
    my $self = bless([], ref($class)||$class);
    vec(my $vec = '', fileno($sock), 1) = 1; # for select()
    @$self[SOCK,  RECV, VEC,  DELIM, MAX_READ,        ]
        = ($sock, '',   $vec, "\n",  DEFAULT_MAX_READ,);
    $self->start_timer; # populate TIMER
    return $self;
}

### Instance methods

sub delimiter { @_ == 1 ? $_[0][DELIM] : do { $_[0][DELIM] = $_[1]; $_[0] } }
sub use_CRLF  { $_[0][DELIM] = "\x0D\x0A"; $_[0] }

sub max_read    { @_ == 1 ? $_[0][MAX_READ] : do { $_[0][MAX_READ] = $_[1]; $_[0] } }
sub buffer_full { length($_[0][RECV]) >= $_[0][MAX_READ] }
sub buffer_used { length $_[0][RECV] }
sub buffer_free { $_[0][MAX_READ] - length($_[0][RECV]) }

# Accessor with to_seconds conversion
sub timeout {
    return $_[0][TIMEOUT]
        if @_ == 1;
    my ($self, $dur) = @_;
    if (defined $dur) {
        $dur = to_seconds($dur)
            // _die("Invalid timeout value '$_[1]'");
    }
    $self->[TIMEOUT] = $dur;
    return $self;
}

sub has_timeout   { defined $_[0][TIMEOUT] }
sub start_timer   { $_[0][TIMER] = Time::Left->new($_[0][TIMEOUT]) }
sub time_left     { $_[0][TIMER]->remaining }
sub timer_expired { $_[0][TIMER]->expired }

# Set-only
sub on_recv_eof { $_[0][ON_EOF]  = $_[1]; $_[0] }
sub on_recv_err { $_[0][ON_RERR] = $_[1]; $_[0] }
sub on_send_err { $_[0][ON_SERR] = $_[1]; $_[0] }

sub _handle_cb {
    my ($self, $set, $cb, $val) = @_;
    $self->[$set] = $val;
    if ($self->[$cb]) {
        $self->[$cb]->($val);
        undef $self->[$cb];
    }
    return $self;
}
sub set_recv_eof { $_[0]->_handle_cb(EOF,  ON_EOF,  1    ) }
sub set_recv_err { $_[0]->_handle_cb(RERR, ON_RERR, $_[1]) }
sub set_send_err { $_[0]->_handle_cb(SERR, ON_SERR, $_[1]) }

sub recv_eof { $_[0][EOF]  }
sub recv_err { $_[0][RERR] }
sub recv_end { $_[0][RERR] || ($_[0][EOF] && "connection closed") || '' }
sub send_err { $_[0][SERR] }
sub data_end { length($_[0][RECV]) == 0 && $_[0][EOF] }

sub send_msg {
    my $self = shift;
    return $self->send_data(map { ($_, $self->[DELIM]) } @_);
}

sub send_data {
    my $self = shift;
    return if $self->[SERR]; # no send possible after error
    unless ($self->[SOCK]->opened) {
        $self->set_send_err($! = EBADF);
        return;
    }
    my $data = join('', @_);
    return if $data eq '';
    my $n;
    #@! Quick send @{[length $data]} bytes
    $! = 0;
    if ($n = send($self->[SOCK], $data, MSG_NOSIGNAL)) {
        return 1 if $n == length($data); # that was easy
        substr($data, 0, $n, '');        # more to go
    }
    elsif ($! and not BLOCKED and not $!{EINTR}) {
        $self->set_send_err($!); # fast fail
        return;
    }
    #@! Slow send @{[length $data]} remaining bytes
    $self->start_timer;
    until ($data eq '' or $self->timer_expired) {
        $self->await_io(1);
        do {
            $! = 0;
            $n = send($self->[SOCK], $data, MSG_NOSIGNAL);
        } while !$n && $!{EINTR};
        if ($n) { substr($data, 0, $n, '') }
        elsif (not BLOCKED) { $data = ''; $self->set_send_err($!) }
        #@! @{[length $data]} remaining bytes
    }
    if ($data ne '') { $self->set_send_err($! = ETIMEDOUT); return }
    #@! Send complete
    return 1;
}

sub recv_msg {
    my ($self) = @_;
    my $n;
    $self->start_timer;
    while (($n = index($self->[RECV], $self->[DELIM])) < 0) {
        $self->await_data
            or return undef;
    }
    return $self->_take_msg($n);
}

sub recv_msg_nb {
    my ($self) = @_;
    $self->recv_status; # read what you can
    my $n = index($self->[RECV], $self->[DELIM]);
    return $self->_take_msg($n)
        if $n >= 0;
    $self->set_recv_err($! = EMSGSIZE)
        if $self->buffer_full;
    return;
}

# Extract and trim delimited message from RECV buffer.
sub _take_msg {
    my ($self, $n) = @_;
    my $dl = length($self->[DELIM]);
    my $msg = substr($self->[RECV], 0, $n + $dl, '');
    substr($msg, -$dl, $dl, ''); # trim DELIM
    return $msg;
}

sub recv_re {
    my ($self, $re) = @_;
    $self->start_timer;
    for ($self->[RECV]) {
        until (/$re/g) { $self->await_data or return undef }
        return substr($_, 0, pos($_), '');
    }
}

sub recv_re_nb {
    my ($self, $re) = @_;
    $self->recv_status; # read what you can
    for ($self->[RECV]) {
        if (/$re/g) { return substr($_, 0, pos($_), '') }
    }
    $self->set_recv_err($! = EMSGSIZE)
        if $self->buffer_full;
    return;
}

sub recv_data {
    my ($self, $n) = @_;
    local $self->[MAX_READ] = $n; # temporarily raise/lower limit
    $self->start_timer;
    while (length($self->[RECV]) < $n) {
        $self->await_data
            or return undef;
    }
    return substr($self->[RECV], 0, $n, '');
}

sub recv_data_nb {
    my ($self, $n) = @_;
    local $self->[MAX_READ] = $n; # temporarily raise/lower limit
    $self->recv_status; # read what you can
    return if length($self->[RECV]) < $n;
    return substr($self->[RECV], 0, $n, '');
}

sub recv_status {
    my ($self) = @_;
    return 0 if $self->[RERR] or $self->[EOF];
    unless ($self->[SOCK]->opened) {
        $self->set_recv_err($! = EBADF);
        return 0;
    }
    my $n = $self->buffer_free;
    return -2 if $n <= 0;
    $n = 0 if $n < 0;
    #@! Room for $n bytes
    my $recv;
    do {
        $! = 0;
        recv($self->[SOCK], $recv, $n, MSG_NOSIGNAL);
    } while $! && $!{EINTR};
    return -1 if $! && BLOCKED;
    if (    $!     ) { $self->set_recv_err($!); return 0 }
    if ($recv eq '') { $self->set_recv_eof;     return 0 }
    #@! Received @{[length $recv]} bytes
    $self->[RECV] .= $recv;
    return length($recv);
}

sub await_data {
    my ($self) = @_;
    return if $self->[RERR] or $self->[EOF];
    if ($self->buffer_full) { $self->set_recv_err($! = EMSGSIZE); return }
    $self->await_io(0);
    my $status = $self->recv_status;
    return 1 if $status > 0; # got data
    $self->set_recv_err($! = ETIMEDOUT)
        if $status < 0;
    return;
}

# Start timer before calling
sub await_io {
    my ($self, $mode) = @_; # $mode: 0=read, 1=write
    return unless $self->[SOCK]->opened;
    if (USING_AE) {
        #@! await_io @{[$mode?'write':'read']} using AE
        my $cv = AE::cv();
        my $w  = AE::io($self->[SOCK], $mode ? 1 : 0, $cv);
        my $t  = AE::timer($self->time_left, 0, $cv)
            if $self->has_timeout;
        $cv->recv; # pause until unblocked or timed-out
    }
    else {
        #@! await_io @{[$mode?'write':'read']} using select
        do {
            my $evec = $self->[VEC];
            my $rvec = $mode ? undef : $evec;
            my $wvec = $mode ? $evec : undef;
            $! = 0;
            select $rvec, $wvec, $evec, $self->time_left;
        } while $!{EINTR} and $self->[SOCK]->opened;
    }
    #@! await_io done
    return;
}

1;
__END__

=head1 NAME

Socket::Stream - Protocol-oriented IO for SOCK_STREAM sockets

=head1 SYNOPSIS

    use Socket::Stream;
    ($sock1, $sock2) = Socket::Stream::pair();
    $sock = Socket::Stream::INET($host_port); # simple case
    $sock = Socket::Steram::UNIX($name);      # simple case
    $stream = Socket::Stream->new($sock)
        ->delimiter($delim)
        ->max_read($int)
        ->timeout($duration)
        ->on_recv_eof(sub { ... })
        ->on_recv_err(sub { ... })
        ->on_send_err(sub { ... });

    # Send data
    $success = $stream->send_msg(@messages);
    $success = $stream->send_data(@strings);

    # Receive data
    $msg = $stream->recv_msg;
    $msg = $stream->recv_msg_nb;
    $data = $stream->recv_re($regex);
    $data = $stream->recv_re_nb($regex);
    $data = $stream->recv_data($n);
    $data = $stream->recv_data_nb($n);

    # Status
    $status = $stream->recv_status;
    $bool = $stream->buffer_full;
    $size = $stream->buffer_used;
    $size = $stream->buffer_free;
    $bool = $stream->recv_eof;
    $err = $stream->recv_err;
    $bool = $stream->recv_end;
    $bool = $stream->data_end;
    $err = $stream->send_err;

=head1 DESCRIPTION

This module provides an IO management wrapper for SOCK_STREAM sockets
(e.g. TCP/IP) intended to facilitate the implementation of protocols
over such a socket.  This frees the programmer from the minutiae of
handling partial data transfers and interrupts, and the complexity of
IO in general and sockets in particular.  Well, somewhat, at least:
implementing protocols is hard even without all that, and this module
tries to make the process only as hard as it inherently is.

The general life-cycle of the object goes like this.

=over 4

=item 1.

Create a client or server socket.  It connects or accepts as
appropriate to its role, putting it in the connected state.

=item 2.

Pass the connected socket to the new() method, returning a new
B<Socket::Stream> object which manages communication.

=item 3.

Use the various send and recv operations to communicate with the peer.
Note that all communication is byte-oriented: if you need to convey
Unicode, UTF-8 encoding or similar should be part of the protocol spec
and must be performed at a higher layer.

=item 4.

When the protocol is over, whether due to an error or negotiated
termination, discard the object and close the socket.

=back

This module is able to use L<AnyEvent> if it is loaded.  It is not
required, but if it is present then all the "blocking" operations are
executed in such a way that the main event loop runs while waiting.
This also means that the "blocking" operations can't be called from
within the event loop, such as from IO watchers: you will elecit a
"recursive blocking wait attempted" exception if you try.

Similarly, the module uses L<Carp> to complain about incorrect usage
or other fatal errors if it is already present in memory, falling back
to vanilla die() otherwise.

=head1 FUNCTIONS

A few socket-creating functions are provided for convenience.  They
are not exported: use the fully-qualified names or alias them as you
see fit.

Socket::Stream::INET() and Socket::Stream::UNIX() are wrappers around
the respective L<IO::Socket::INET> and L<IO::Socket::UNIX> new()
methods with Type set to SOCK_STREAM and an exception thrown on
failure.  Other than that, they have the same signature as the new()
methods they wrap.

Additionally, C<< ($s1, $s2) = Socket::Stream::pair(); >> is a wrapper
around the socketpair() builtin which returns two connected UNIX
SOCK_STREAM sockets, assuming your platform supports it.

=head1 METHODS

The module has a significant number of methods, but many of them are
of lesser importance.  This documentation aims to describe the more
important methods first.

Note that many methods return "self" so that methods can be chained
together in the form C<< $obj->do_this($x)->do_that($y) >>.  Objects
in this class will be designated $stream to distinguish them from the
underlying $socket.

=head2 Constructor

Note that the constructor is extremely basic.  Rather than have a lot
of options, the preferred approach is to chain together additional
methods which set attributes or callback hooks and return the same
object.

=head3 new

    $stream = Socket::Stream->new($socket);

Returns a new object associated with the $socket or dies trying.  Some
options are set on the socket, including non-blocking mode.  The class
only deals in connected sockets, so establish the connection before
calling this, or at least before calling anything message-related.

=head2 Send Data

There are two methods for sending data: one for delimited messages,
and one for raw data.  Both block until IO is complete if necessary,
subject to a time limit set by the L</"timeout"> attribute.  Every
effort is made to prevent SIGPIPE being raised if the socket is shut
down, but this behaviour is somewhat platform-specific.

If the operations fail, they return false and $! will hold relevant
information.  The error is also preserved in L</"send_err">, and both
send operations become no-ops once an error has been flagged on the
output channel (errors are unrecoverable).

=head3 send_msg

    $success = $stream->send_msg(@messages);

Attempts to send all the @messages (byte strings).  After each
message, a delimiter string is sent.  The delimiter string is an
attribute which can be set: see L</"delimiter">.  Returns true if all
data was sent; false on error (including timeout).

=head3 send_data

    $success = $stream->send_data(@strings);

As per send_msg(), but no delimiters are added.  All @strings must be
byte strings, and they are sent exactly as is.

=head2 Receive Data

There are three ways to receive data, each of which has a blocking and
nonblocking variant.  In the blocking cases, the method returns the
data or undef in case of error.  For the nonblocking variants, an
undef result may simply mean that no data was available at the time:
one must check the error status separately.

Received data is buffered in the object until sufficient to meet the
requirements of a receive method, but there is an upper limit on this
data so that it doesn't accumulate indefinitely and run your host out
of memory.  You can adjust this limit: see L</"max_read">.  The limit
only applies to messages terminated by a delimiter or regex: if you
specify data by size, the limit is temporarily adjusted to match the
request.

Note that the nonblocking variants aren't subject to timeouts because
they always return immediately.  As such, they neither alter nor use
the timeout timer.  This means you can use it independently if you
want to manage an overall receive time limit: call L</"start_timer">,
then nonblocking receives as required, testing L<"timer_expired"> as
you go.  Note that send operations will also restart the timer.

=head3 recv_msg

    $msg = $stream->recv_msg;

Blocks until a message terminated by a delimiter string has arrived.
Returns the message stripped of the delimiter, or undef on error.  The
delimiter is an attribute which can be set: see L</"delimiter">.

=head3 recv_msg_nb

    $msg = $stream->recv_msg_nb;

As per L</"recv_msg">, but returns undef immediately if no message is
available yet.

=head3 recv_re

    $data = $stream->recv_re($regex);

Blocks until a message terminated by a $regex match arrives.  Returns
all data up to and including the part which matched the $regex.  The
intended use is for messages which can't be delimited by a simple
exact-string match, but more complex expressions are allowed.  Groups
are permitted, but preservation of the group match variables is not
guaranteed; I recommend matching on the message terminator and leaving
more complex matching until later.  Undef is returned on error.

=head3 recv_re_nb

    $data = $stream->recv_re_nb($regex);

As per L</"recv_re">, but returns undef immediately if matching data
is not available yet.  Note that undef is associated with three cases:
no data yet, EOF, and error.  Consult various L</"Status"> methods to
distinguish between these cases.

=head3 recv_data

    $data = $stream->recv_data($n);

Blocks until $n bytes of data have arrived, returning those bytes or
undef on failure.  The message size limit is temporarily set to $n, so
this method never results in a message size error.

=head3 recv_data_nb

    $data = $stream->recv_data_nb($n);

As per L</"recv_data">, but returns undef immediately if insufficient
data is currently available.  Note that undef is associated with three
cases: no data yet, EOF, and error.  Consult various L</"Status">
methods to distinguish between these cases.

=head2 Status

There are numerous status conditions on the object which can be
queried with the following methods.

=head3 recv_status

    $status = $stream->recv_status;

This method returns some status information about the socket's receive
channel and appends pending data into the object's read buffer, space
permitting.  The $status is an integer with semantics as follows.

=over 4

=item $status > 0

There was pending data available and it has been added to the object's
buffer.  The value is the number of bytes read.

=item $status == 0

The channel has shut down or encountered a read error.  There will be
no further data received on this channel either way: any data still in
the buffer is all that remains.  This is a terminal status: there is
no point calling the method again once the status is zero.

=item $status == -1

There was no pending data, but the channel is still open and read
buffer space is still available.

=item $status == -2

The read buffer is already full: you will need to consume some of that
data using a receive operation before further updates are possible.

=back

=head3 buffer_full

Returns true if the object's read buffer has reached capacity.

=head3 buffer_used

Returns the number of bytes in the read buffer.

=head3 buffer_free

Returns the number of bytes still available in the read buffer, which
could be negative if the L</"max_read"> attribute is reduced after the
buffer fills.

=head3 recv_eof

True if the EOF condition has been detected on the receive stream.

=head3 recv_err

Returns the $! error value encountered on the receive stream, if one
has occurred; undef otherwise.

=head3 recv_end

True if L</"recv_eof"> is true, or if L</"recv_err"> is defined.
Either way, no more data will be received into the read buffer.  The
string context of the value is an error message if the cause is an
error, "connection closed" for EOF, empty string for false.

=head3 data_end

True if L</"recv_eof"> is true and the read buffer is empty, meaning
that the stream was properly closed and all data was consumed.  Note
that this condition isn't always reachable: if you're expecting a
delimited message and the sender closes without sending the delimiter,
then L</"recv_msg"> will fail (return undef) and L</"recv_eof"> will
be true, but B<data_end> will still be false.

=head3 send_err

Returns the $! error value encountered on the send stream, if one has
occurred; undef otherwise.

=head2 Attributes

Attribute methods have a get mode and a set mode.  When called with no
arguments, they return the current value of the attribute.  When
called with one argument, they set the attribute and return self,
allowing attribute-set methods to be chained together.

=head3 delimiter

The delimiter is a byte or byte sequence used to delimit messages for
the L</"send_msg">, L</"recv_msg">, and L</"recv_msg_nb"> methods.
The default is "/n", which is platform-specific.  TCP/IP protocols
often use CRLF as a delimiter, so a convenience method C<<
$stream->use_CRLF >> is provided as a shortcut.

=head3 max_read

The largest amount of data the object will hold in its read buffer,
and thus the largest message it can process with L</"recv_msg">,
L</"recv_re">, or their nonblocking counterparts.  This puts an upper
limit on the amount of memory a rogue sender can consume.  The default
size is 2^18 bytes (256K).  The size is more important for server-side
usage, where there are potentially many concurrent connections.

=head3 timeout

The time limit for all blocking I/O methods.  The default is undef,
meaning no limit.  You can set this to any value recognised by
to_seconds() in L<Time::Left>.  The value returned by the get mode is
undef or numeric seconds.

=head2 Hooks

The hook methods permit functions to be called when certain events
take place.  The methods all take one argument: a CODE reference to
call when the event occurs.  They all return self so that the methods
can be chained together.  Using these hooks to raise exceptions or
trigger changes in guard variables tends to be a lot more practical
than checking error state after each I/O operation.

Callbacks are flushed after use, so they will only be called once if
at all.  EOF can only happen once and all errors are fatal errors, so
there's no need to keep the callback around, and it means that any
closure resources are freed up at that time.  Avoid reference to the
$stream object in the closure, since that's circular, but direct
reference to the underlying socket is safe if needed.

You can remove an existing hook by calling the relevant method with no
arguments or undef.

=head3 on_recv_eof

Called as soon as the EOF condition on the receive stream is detected.
There may be data remaining in the read buffer, but no further data
will be added to it.

=head3 on_recv_err

Called as soon as an error is detected on the receive stream.  This
can include raw OS-provided error conditions or synthetic errors for
timeout and buffer full.  The code is called with $! as the argument.
As with the EOF condition, there may be unprocessed data in the read
buffer when this condition arises.  You can turn receive errors into
exceptions by dying in this callback.

=head3 on_send_err

As per L</"on_recv_err">, but applies to the send stream.  Methods for
sending are limited, so this callback can only occur during a call to
L</"send_msg"> or L</"send_data">.  You can grant those methods
do-or-die semantics by dying in this callback.

=head2 Internal Methods

These methods are primarily intended for internal use, but they are
available if needed.

=head3 has_timeout

Returns true if the L</"timeout"> attribute is defined.

=head3 start_timer

Starts the timer on a new timeout operation.  All the blocking
operations call this if they require I/O.  Consider creating a
separate L<Time::Left> object if you need a special timer.

=head3 time_left

Time remaining on the timeout timer.  This will be undef if the
L</"timeout"> attribute was undef when L<"start_timer"> was invoked.

=head3 timer_expired

True if L</"time_left"> is defined and zero or less.

=head3 await_io

    $stream->await_io($mode);

Blocks waiting for incoming data ($mode == 0) or outgoing write buffer
space ($mode == 1) up to the limits of the current running timer (see
above).  If L<AnyEvent> is loaded, the "blocking" is performed using a
condition variable which allows the main event loop to run.  There is
no effect other than the possible blocking: no value is returned to
indicate which condition was encountered, and no errors are raised.

=head3 await_data

Blocks waiting for incoming data up to the limit of the current timer,
as given by L</"time_left">.  Returns true if more data arrived and
was added to the read buffer.  Will return false immediately if the
receive stream has reached EOF or encountered an error.  Will raise a
synthetic receive error if called when the receive buffer is already
full, or if the timer expires before data arrives.

=head1 ERRORS

The error philosophy of this module is that only fatal errors are
reported, and there's generally no point in trying to recover from
them even if that might be possible in theory.  As such, errors are a
terminal condition for at least the half of the socket which reported
it, and the best you can do is a clean shutdown of the other half.

The I/O operations on the socket can result in various errors, most of
them defined by the POSIX standard.  Nearly all of these are inherent
to socket-based I/O, but a couple of synthetic errors have been added
by this module.  Error conditions of note are as follows.

=head2 EINTR

This "system call was interrupted by signal" error is always handled
internally -- one of the benefits of using this module.  Interrupted
calls are simply restarted, with appropriate adjustment to timeout
values where applicable.  You won't see this error.

If you actually want a signal to interrupt socket I/O in some way,
you'll need to make it happen in a signal handler.  If you just want
to abort all I/O in progress, closing the underlying socket will do
the trick.

=head2 EAGAIN, EWOULDBLOCK

These errors are returned when requests for nonblocking operation
can't be fulfilled.  These are handled internally and clients should
never encounter them as error values.

Clients which use the nonblocking receive operations need to consider
that those operations return undef in lieu of this error.  An undef
result requires further testing against L</"recv_end"> or similar to
determine whether the condition is fatal or not unless an error
handler is explicitly taking care of it.

=head2 EPIPE

The "broken pipe" error occurs when sending data is no longer possible
due to socket shutdown.  This is usually associated with the SIGPIPE
signal, but this module uses several platform-specific techniques to
prevent the signal.  The EPIPE error can instead be caught via the
L</"on_send_err"> callback hook, as with any other send error.  Niche
platforms may still require a SIGPIPE handler.

=head2 EMSGSIZE

This error is raised synthetically by this module if more data is
requested, but the read buffer is already full.  Unless you're making
use of the L</"Internal Methods">, you'll only see this error if
L</"recv_msg"> or L</"recv_msg_nb"> encounter a full buffer with no
L</"delimiter"> present, or if L</"recv_re"> or L</"recv_re_nb">
encounter a full buffer with no matching regex.  In the unlikely case
that your platform doesn't have a defined EMSGSIZE, you can test for
this condition by numeric comparison with Socket::Stream::EMSGSIZE().

Bear in mind that this is intended to be a fatal error, not simply a
trigger for additional memory allocation.  You have to draw the line
somewhere with buffer size, and this error is raised when the line is
crossed.  Terminate the session as politely as you can.

=head2 ETIMEDOUT

This error is normally associated with the connection-establishment
phase of the socket, but this module co-opts it as a general timeout
indicator.  It is raised synthetically when a L</"timeout"> has been
set and a blocking operation reaches that time limit.  It's highly
unlikely that your platform won't have this symbol, but you may test
against Socket::Stream::ETIMEDOUT() as you prefer.

As with EMSGSIZE, this is intended to be a fatal condition, not an
opportunity to consider whether to wait longer.  If you want to ask
the user whether to keep waiting, use indefinite timeouts and manage
your own timers.  You can always create a signal handler which shuts
down the socket and send the signal at any time.

=head2 EBADF

You'll get this if you attempt to send or receive on a socket which
has been closed locally, as opposed to remotely.  You can force-fail a
stream by closing the socket.  This is a standard POSIX error, but you
can compare it against Socket::Stream::EBADF() if you prefer.

=head1 INHERITING

If you want to inherit this class, perhaps to extend the I/O semantics
in some way, be aware that it's implemented as an array, not a hash.
Aside from being generally more compact, the main advantage of an
array is compile-time syntax checking on the field names, which are
symbolic constants, not strings.  The value Socket::Stream::_EXTEND()
gives the lowest array index not used by the main package, and a child
class should start its numbering there if it requires storage.

=head1 EXAMPLES

Here are some basic examples to show the system in context.  The
examples are fairly rudimentary, but they do work.

=head2 One-Shot Hello Server

This server waits for a client to connect and send a CRLF-delimited
message, then responds with a "hello" message and exits.  You can try
it out with a telnet client.  Error-handling is extremely minimal
because the protocol is minimal, but it correctly handles loss of
connection before a message is received.

    use 5.010;
    use Socket::Stream;
    my $listen = Socket::Stream::INET(
        LocalAddr => '127.0.0.1:9999',
        Listen => 1,
        );
    say "Listening";
    my $sock = $listen->accept;
    say "Accepted";
    my $stream = Socket::Stream->new($sock)
        ->use_CRLF;
    my $msg = $stream->recv_msg
        // die "No request!\n";
    say "Responding";
    $stream->send_msg("Hello, $msg!");
    exit;

=head2 Message Client

This example implements a client sufficient to connect to the above
server.  It connects to a host:port specified as a command line
argument, then alternates between reading lines from STDIN and the
socket.  It's fundamentally limited by the C<< while (<STDIN>) { ... }
>> construct which blocks indefinitely, but the connection close is
caught by C<< $stream->recv_status == 0 >>.

    use 5.010;
    use Socket::Stream;
    die "Usage: $0 <host>:<port>\n"
        unless @ARGV == 1 && $ARGV[0] =~ /^.+:.+$/;
    my $sock = Socket::Stream::INET(@ARGV);
    my $stream = Socket::Stream->new($sock)
        ->on_recv_err(sub { die "Recv err: $!\n" })
        ->on_send_err(sub { die "Send err: $!\n" })
        ->use_CRLF;
    while (<STDIN>) {
        chomp;
        last if $stream->recv_status == 0;
        $stream->send_msg($_);
        my $msg = $stream->recv_msg
            // last;
        say $msg;
    }
    say $stream->recv_eof ?
        "Server closed connection." :
        "Exiting.";
    exit;

=head2 RESP2 Client

The module L<Socket::Stream::RESP2Client>, bundled with this one, is a
good working example of a simple but real protocol.  RESP2 is used by
Redis and other work-alike systems.  The module is not a full Redis
API abstraction, but it could be used to implement one.

=head1 THREAD SAFETY

This module is not thread-safe, and the problem it aims to solve does
not lend itself to parallelism.  Individual objects and their sockets
should be confined to a single thread.  If you need to gather or fan
out messages across threads, do that in a higher layer using
L<Thread::Queue> or similar.

=head1 SEE ALSO

L<AnyEvent> is the supported event loop provider.  I don't recommend
using this module in other event loop contexts.

L<Time::Left> is used by this module to manage timeouts.  If you have
additional time limits, you may find it useful.

L<Socket::Stream::RESP2Client> uses this module to implement a RESP2
client library.  It is useful both as an example and for simple Redis
use cases.

=head1 LICENSE AND COPYRIGHT

This software is Copyright (c) 2025 by Brett Watson.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
