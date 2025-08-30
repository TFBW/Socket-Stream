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
};

sub NOOP { }

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
        ->on_recv_err(sub { die "Recv error: $!\n" })
        ->use_CRLF;
    return bless([$socket, $stream], ref($class)||$class);
}

sub socket { $_[0][SOCKET] }
sub timeout { @_ == 1 ? $_[0][TIMEOUT] : do { $_[0][TIMEOUT] = $_[1]; $_[0] } }
sub start_timer { $_[0][TIMER] = time_left($_[0][TIMEOUT]) }

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

sub recv_msg {
    my ($self, $n) = @_;
    $self->[STREAM]->timeout($self->[TIMER]->remaining);
    return defined($n) ?
        $self->[STREAM]->recv_data($n) :
        $self->[STREAM]->recv_msg;
}

sub recv_datum {
    my ($self) = @_;
    my $data = $self->recv_msg
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
        $data = $self->recv_msg($n);
        die "Bulk string parse error\n"
            unless $self->recv_msg eq '';
    }
    else {
        #@! Start array size $n
        $data = [];
        push @$data, $self->recv_datum # recurse
            for 1..$n;
        #@! End array size $n
    }
    return $data;
}

sub response {
    my ($self) = @_;
    $self->start_timer;
    return $self->recv_datum;
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

# Callback when timer expires or ready to read socket.  Fails
# immediately if recv stream has ended.
sub io_cb {
    my ($self, $okcb, $ngcb) = @_;
    return $ngcb->("time limit or end of data")
        if $self->[STREAM]->recv_end
        or $self->[TIMER]->expired;
    my ($t, $w);
    # $next keeps $t and $w alive until called
    my $next = sub { undef $t; undef $w; goto shift };
    # $t and $w keep $next alive
    $t = AnyEvent->timer(
        after => $self->[TIMER]->remaining,
        cb    => sub { $next->($ngcb, "timeout") },
        ) if $self->[TIMER]->is_limited;
    $w = AnyEvent->io(
        fh   => $self->[SOCKET],
        poll => 'r',
        cb   => sub { $next->($okcb) },
        );
    return;
}

sub recv_empty_msg_cb {
    my ($self, $data, $okcb, $ngcb) = @_;
    my $msg = $self->[STREAM]->recv_msg_nb;
    return $msg eq '' ? $okcb->($data) : $ngcb->("expected CRLF")
        if defined $msg;
    #@! Still awaiting bulk string terminator
    return $self->io_cb(
        sub { $self->recv_empty_msg_cb($data, $okcb, $ngcb) }, # retry
        $ngcb,
        );
}

sub recv_bulk_cb {
    my ($self, $n, $okcb, $ngcb) = @_;
    my $data = $self->[STREAM]->recv_data_nb($n);
    if (defined $data) {
        return $self->recv_empty_msg_cb($data, $okcb, $ngcb);
    }
    #@! recv_bulk_cb got no data
    return $self->io_cb(
        sub { $self->recv_bulk_cb($n, $okcb, $ngcb) }, # retry
        $ngcb,
        );
}

sub recv_array_cb {
    my ($self, $n, $array, $okcb, $ngcb) = @_;
    return $okcb->($array)
        unless $n > 0;
    return $self->recv_datum_cb(
        sub {
            push @$array, $_[0];
            $self->recv_array_cb($n - 1, $array, $okcb, $ngcb);
        },
        $ngcb,
        );
}

sub recv_datum_cb {
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
        return $self->recv_bulk_cb($data, $okcb, $ngcb)
            if $type eq '$';
        #@! Start array size $data
        return $self->recv_array_cb($data, [], $okcb, $ngcb);
    }
    #@! get_datum_cb got no data
    return $self->io_cb(
        sub { $self->recv_datum_cb($okcb, $ngcb) }, # retry
        $ngcb,
        );
}

sub response_cb {
    my ($self, $okcb, $ngcb) = @_;
    $okcb //= \&NOOP;
    $ngcb //= \&NOOP;
    $self->start_timer;
    return $self->recv_datum_cb($okcb, $ngcb);
}

1;
