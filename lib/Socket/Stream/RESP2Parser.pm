use 5.010;
use strict;
use warnings;

package Socket::Stream::RESP2Parser;

use constant {
    STR   => ord '+',
    ERR   => ord '-',
    INT   => ord ':',
    BULK  => ord '$',
    ARRAY => ord '*',
};

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }

sub new {
    my ($class, $stream, $expect) = @_;
    my $self = bless({ stream => $stream->use_CRLF }, ref($class)||$class);
    return $self->expect($expect);
}

# Note that the 'expect' and 'data' fields are initialised as though
# the object were told that an array of size $expect follows.  There
# is a final "unwrap" at the end of receive() which promotes the
# accumulated contents to the top level to save the extra drill-down
# when accessing the final result.
sub expect {
    my ($self, $expect) = @_;
    $expect //= 1;
    _die("Invalid response count '$expect'")
        unless $expect =~ /^\d+$/;
    @$self{qw(size error expect data)} =
        $expect > 0 ?
        (0, '', [$expect], [ [] ]) :
        (0, '', [], []);
    return $self;
}

sub data {
    my ($self) = @_;
    return () if $self->{error} or @{$self->{expect}} > 0;
    return wantarray ? @{$self->{data}} : $self->{data}->[0];
}

sub data_or_die {
    my ($self) = @_;
    _die("Parser error ($self->{error})")
        if $self->{error};
    _die("Response incomplete")
        unless @{$self->{expect}} == 0;
    return wantarray ? @{$self->{data}} : $self->{data}->[0];
}

sub error { @_ == 1 ? $_[0]->{error} : do { $_[0]->{error} = $_[1]; $_[0] } }

sub is_finished { !!($_[0]->{error} or @{$_[0]->{expect}} == 0) }

our ($Stream, $Size, $Error, @Expect, @Data);
sub _fail { ($Error) = @_; return -1 }
sub receive {
    my ($self) = @_;
    # Aliasing trick: access object contents as package globals.
    # Done primarily for readability rather than performance.
    local (*Stream, *Size, *Error) = \(@$self{qw(stream size error)});
    return -1 if $Error;
    local *Expect = $self->{expect}; # @Expect
    return 1 unless @Expect;
    local *Data = $self->{data}; # @Data
    my ($datum, $msg, $type, $val);
    while (@Expect) {
        if ($Size) {
            $datum = $Stream->recv_data_nb($Size);
            return 0 unless defined $datum;
            return _fail("no CRLF after bulk string")
                unless substr($datum, -2, 2, '') eq "\x0D\x0A";
            $Size = 0;
        }
        else {
            $msg = $Stream->recv_msg_nb;
            return 0 unless defined $msg;
            next if $msg eq ''; # skip "blank lines"
            $type = ord($msg);
            $val = substr($msg, 1);
            if ($type == STR or $type == INT) { $datum = $val }
            elsif ($type == ERR) { $datum = \(my $err = $val) }
            elsif ($type != BULK and $type != ARRAY) {
                return _fail("bad message type '$msg'")
                    unless $msg =~ /^[a-z]/i;
                $datum = [ $msg =~ /\S+/g ]; # "inline" command
            }
            # Is bulk string or array at this point
            elsif ($val eq '-1') { undef $datum }
            elsif ($val =~ /\D/) { return _fail("invalid count '$val'") }
            elsif ($type == BULK) { $Size = $val + 2; next }
            elsif ($val == 0) { $datum = [] }
            else { push @Data, []; push @Expect, $val; next }
        }
        while (@Expect) {
            push @{$Data[-1]}, $datum;
            last if --$Expect[-1] > 0;
            # Array filled: pop the zero and merge the array as datum.
            pop @Expect;
            $datum = pop @Data;
        }
    }
    $self->{data} = $datum; # unwrap final result
    return 1;
}

1;
__END__

=head1 NAME

Socket::Stream::RESP2Parser - Progressive RESP2 response parser

=head1 SYNOPSIS

    use Socket::Stream::RESP2Parser;
    $parser = Socket::Stream::RESP2Parser->new($stream, $n);
    until ($parser->receive) { $stream->await_data or last }
    if ($parser->is_finished) {
        $error = $parser->error;
        $data = $parser->data;
    }
    $data = $parser->data_or_die;

=head1 DESCRIPTION

This is a parser for RESP2 (Redis) server responses.  Server output is
fed directly via a L<Socket::Stream> object, and only nonblocking IO
methods are used.  If the entire response is not yet available in the
stream, the object can parse what's present and continue where it left
off once further data arrives.  The parser has very low overhead and
uses no recursion or closures.

Only three methods are used on the underlying L<Socket::Stream>: the
new() method invokes use_CRLF() on it to ensure that the appropriate
message delimters are in use; the receive() method calls recv_msg_nb()
and recv_data_nb() to obtain data from the server.  None of these are
subject to timeouts, so you will need to impose any timeout discipline
externally.  All methods are safe to call from an L<AnyEvent> handler.

=head2 Parsed Data Model

RESP2 can encode arrays, nulls, strings, integers, and errors.  Arrays
are returned as ARRAY refs; null is returned as undef; integers and
strings (simple and bulk) are returned as scalars.  Error strings are
returned as scalar references to distinguish them from normal strings.

No effort is made to preserve the encoding method of the scalars, and
no syntax checking is performed on integers.  Performing such a check
would be a half measure because you'd have no way of telling whether
the check had been performed.  This module only validates protocol to
the minimum extent necessary.

=head1 METHODS

This is a pure object-oriented class with methods as follows.

=head2 new

    $parser = Socket::Stream::RESP2Parser->new($stream, $count);

Creates a new $parser object which expects to see $count (default 1)
server responses on $stream, a L<Socket::Stream> object.  You may want
to have a $count greater than one if you are expecting multiple small
responses due to pipelining or similar.  There is a throughput/latency
tradeoff between receiving multiple responses and creating separate
objects for each response, but batching small responses is usually the
best approach for speed.  A $count of zero is permitted: it generates
an object containing no data and not expecting any.

=head2 expect

    $parser = $parser->expect($count);

Resets an existing object to expect $count new responses (default 1).
This is effectively the same as creating a new object with the same
stream, but saves a little overhead.  Note that this does nothing to
the underlying stream, and the operation only makes sense if the
stream is still up, working, and between responses.

=head2 receive

    $status = $parser->receive;

Parses available data on the stream.  The returned $status is one of
three values: 1 for complete and successful; 0 for incomplete; -1 for
parser errors.  Once the method has returned a nonzero response, any
further calls to the method do nothing and return the same value until
expect() is called.  If the method returns zero, you should wait for
the stream to be ready to read before calling again.  Monitoring the
stream for errors is the caller's responsibility: this method does not
distinguish between data not yet available and error/EOF conditions.
In the case of a parser error, the stream should be abandoned: there's
no way to recover the session from such a state.

=head2 data

    $data = $parser->data;
    @data = $parser->data;

If receive() has completed successfully ($status == 1), this method
returns the first response in a scalar context, or all responses in a
list context.  If receive() is incomplete or failed, an empty list is
returned.  Note that the scalar version can't distinguish between a
successful NULL response and the failure cases.

=head2 data_or_die

    $data = $parser->data_or_die;
    @data = $parser->data_or_die;

As per the data() method, except that the non-success cases raise an
exception.  Exceptions are raised via L<Carp> croak() if loaded,
vanilla die() if not.

=head2 error

    $error = $parser->error;
    $parser = $parser->error($string);

With no argument, returns the parser error string if such an error has
occurred, or the empty string otherwise.  Valid for use in a boolean
context to detect parser failures.  With one argument, sets an error
$string, which one might use to convey an IO error.

=head2 is_finished

    $bool = $parser->is_finished;

True once receive() has returned a non-zero value; false otherwise.
If true, the object either has complete data or a parser error.

=head1 EXAMPLES

The following example is a barebones Redis query tool which sends a
single request (given via @ARGV) to the local Redis and dumps the
response using L<Data::Dump>.  The request must be simple enough to
express as an inline command; "ping" is used if no arguments are
provided.  Executing this with "command" is a pretty good torture
test, as it generates a large, deeply-nested response.

    use Data::Dump qw(dd);
    use Socket::Stream;
    use Socket::Stream::RESP2Parser;
    my $socket = Socket::Stream::INET('127.0.0.1:6379');
    my $stream = Socket::Stream->new($socket);
    my $parser = Socket::Stream::RESP2Parser->new($stream);
    $stream->send_msg("@ARGV" || 'ping');
    until ($parser->receive) {
        next if $stream->await_data;
        die $stream->recv_err || "EOF encountered";
    }
    dd($parser->data_or_die);

For more realistic examples, see L<Socket::Stream::RESP2Client>.  That
module provides both synchronous and asynchronous wrappers for this
module, as well as providing request-sending methods.

=head1 SEE ALSO

L<Socket::Stream> makes this class possible: some understanding of it
is assumed in this documentation.

L<Socket::Stream::RESP2Client> uses this class to implement a fuller
RESP2 client library.

=head1 LICENSE AND COPYRIGHT

This software is Copyright (c) 2025 by Brett Watson.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
