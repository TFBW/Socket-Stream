use 5.010;
use strict;
use warnings;

package Socket::Stream::RESP2Parser;
our $VERSION = '0.990';

use constant {
    STR   => ord '+',
    ERR   => ord '-',
    INT   => ord ':',
    BULK  => ord '$',
    ARRAY => ord '*',
};

our $MaxData = 10_000; # default limit on received data

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }

sub new {
    my ($class, $stream) = @_;
    my %self = (
        stream => $stream->use_CRLF,
        data   => [], # completed data
        part   => [], # arrays in progress (count, array, ...)
        size   => 0,  # nonzero when bulk string expected
        error  => '', # becomes true on failure
        );
    return bless(\%self, ref($class)||$class);
}

sub error {
    my ($self, $error) = @_;
    return $self->{error} unless $error;
    $self->{error} = $error;
    undef $self->{stream};
    return $self;
}

sub count { 0 + @{$_[0]->{data}} }

sub take {
    my ($self, $n) = @_;
    $n //= 1;
    return $n > 0 ? splice(@{$self->{data}}, 0, $n) : ();
}

our ($Stream, $Size, $Error, @Part, @Data);
sub _fail { ($Error) = @_; undef $Stream; return -1 }
sub _errcheck { my $err = $Stream->recv_end; return $err ? _fail($err) : 0 }
sub receive {
    my ($self, $n) = @_;
    $n ||= $MaxData;
    # Aliasing trick: access object contents as package globals.
    # Improves readability and performance.
    local (*Stream, *Size, *Error) = \(@$self{qw(stream size error)});
    return -1 if $Error;
    local *Part = $self->{part}; # @Part
    local *Data = $self->{data}; # @Data
    my ($datum, $msg, $type, $val);
  DATUM: # Performance-critical loop: micro-optimisations pay off here.
    while (@Data < $n) {
        if ($Size) {
            $datum = $Stream->recv_data_nb($Size)
                // return _errcheck;
            return _fail("no CRLF after bulk string")
                unless substr($datum, -2, 2, '') eq "\x0D\x0A";
            $Size = 0;
        }
        else {
            $msg = $Stream->recv_msg_nb
                // return _errcheck;
            next if length $msg == 0; # ignore "blank lines" from clients
            $type = ord $msg;
            $val = substr $msg, 1;
            if ($type == STR or $type == INT) { $datum = $val }
            elsif ($type == BULK) {
                if ($val !~ /\D/) { $Size = $val + 2; next }
                elsif ($val eq '-1') { undef $datum }
                else { return _fail("invalid bulk count '$val'") }
            }
            elsif ($type == ARRAY) {
                if ($val !~ /\D/) {
                    if ($val == 0) { $datum = [] }
                    else { push @Part, $val, []; next }
                }
                elsif ($val eq '-1') { undef $datum }
                else { return _fail("invalid array count '$val'") }
            }
            elsif ($type == ERR) { $datum = \(my $err = $val) }
            else {
                return _fail("bad message type '$msg'")
                    unless $msg =~ /^[a-z]/i;
                $datum = [ $msg =~ /\S+/g ]; # "inline" command
            }
        }
        while (@Part) {
            push @{$Part[-1]}, $datum;
            next DATUM if --$Part[-2] > 0; # expecting more
            $datum = splice @Part, -2;
        }
        push @Data, $datum;
    }
    return 1;
}

1;
__END__

=head1 NAME

Socket::Stream::RESP2Parser - Progressive pure Perl RESP2 parser

=head1 SYNOPSIS

    use Socket::Stream::RESP2Parser;
    $parser = Socket::Stream::RESP2Parser->new($stream);
    until ($parser->receive($n)) { $stream->await_data }
    $error = $parser->error;
    @data = $parser->take($n);

=head1 DESCRIPTION

This is a pure Perl parser for RESP2 (Redis).  The general principle
of operation is simple: you give it a L<Socket::Stream> to manage and
call receive() strategically.  It will convert the incoming stream to
discrete data items which it stores in a queue for consumption via the
take() method.

Only two I/O methods are used on the underlying L<Socket::Stream>:
recv_msg_nb() and recv_data_nb().  Both are nonblocking and safe to
call from L<AnyEvent> I/O watchers if you wish to do so.  Nonblocking
operations are not subject to timeouts, so you will need to impose any
timeout discipline externally.  Partial responses are buffered with
appropriate state, allowing the parser to continue where it left off
once further data arrives.  The parser has very low overhead and uses
no recursion or closures; L<Socket::Stream> is the only dependency.

This is not a replacement for Redis.pm or any of its work-alikes: it
has a completely different API and is not a general-purpose Redis
interface.  Having said that, if you are planning a heavily pipelined
workload, sending a large number of commands and then examining the
responses, this module offers the ability to do the response-receiving
part with great speed and flexibility.

=head2 Parsed Data Model

RESP2 can encode arrays, nulls, strings, integers, and errors.  Arrays
are returned as ARRAY refs; null is returned as undef; integers and
strings (simple and bulk) are returned as scalars.  Error strings are
returned as scalar references to distinguish them from normal strings.

The parser will also recognise client-specific constructs such as
inline commands: any datum which starts with an alphabetic character
instead of one of the RESP2 type markers is converted to an array of
strings split on whitespace, which is how the command would otherwise
be packaged.  Blank lines are ignored.

No effort is made to preserve the encoding method of the scalars, and
no syntax checking is performed on integers.  Performing such a check
would be a half measure because you'd have no way of telling whether
the check had been performed.  This module only validates protocol to
the minimum extent necessary.

=head1 METHODS

This is a pure object-oriented class with methods as follows.

=head2 new

    $parser = Socket::Stream::RESP2Parser->new($stream);

Creates a new $parser object which operates on the receive side of the
given $stream, a L<Socket::Stream> object.

=head2 receive

    $status = $parser->receive($n);

Parses available data on the stream.  This only uses nonblocking read
operations, so any delay will be minimal and CPU-intensive, returning
when either $n data items are available in the queue ($status == 1),
there is no more buffered data to process ($status == 0), or an error
occurs which prevents further parsing ($status == -1).  Note that in
the latter two cases some messages may have been received and added to
the queue: the return code only indicates the reason for stopping.

If $n is undef or zero, it defaults to the package variable $MaxData,
initially 10,000.  You may tune this value to suit your application:
it exists only to put an upper limit on how much unprocessed data can
be queued.  If you are expecting very large responses, you may want to
make it smaller.

Only specify $n if you want to interrupt the parser as soon as that
number of messages is available.  For highest throughput, omit $n and
check L</"count"> on return to see if sufficient data has arrived.

The three return codes have implications for when you should next call
receive(): $status == 1 means no waiting is required, but you should
probably take some data first; $status == 0 means you should wait for
read-readiness on the stream before calling again; $status == -1 means
the stream has terminated, and any further attempts would also return
-1.  The error() method provides further detail in this case.

=head2 take

    @data = $parser->take($n);

Removes and returns the first $n messages from the queue.  If $n is
omitted, it defaults to one; if called in a scalar context, returns
the last datum in the list.  Attempting to take more data than is
available simply returns what's available.  It's acceptable to use
this as a means to take data opportunistically, but bear in mind that
a scalar context taking one item can't distinguish between NULL and
the absence of an available message.

=head2 count

    $count = $parser->count;

Returns the number of messages currently available to take().

=head2 error

    $error = $parser->error;

With no argument, returns the parser error string if such an error has
occurred, or the empty string otherwise.  Valid for use in a boolean
context to detect failures.

    $parser = $parser->error($string);

With one argument, sets an error $string, e.g. to report a timeout.
This also flushes the L<Socket::Stream> object: all errors are fatal
and prevent further I/O.  The $string must have a true value.

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
    until ($parser->receive(1)) { $stream->await_data }
    die $parser->error if $parser->error;
    dd $parser->take;

For more realistic examples, see L<Socket::Stream::RESP2Client>.  That
module provides both synchronous and asynchronous wrappers for this
module, as well as providing request-sending methods.

=head1 SEE ALSO

L<Socket::Stream> makes this class possible: some understanding of it
is assumed in this documentation.

L<Socket::Stream::RESP2Client> uses this class to implement a fuller
RESP2 client library.

Redis.pm is the baseline pure Perl Redis client library.  This module
is not intended to replace it: only a rather small subset of its total
functionality is offered.  Informal testing shows that this module is
somewhat faster in terms of parser throughput -- particularly if many
small responses are pipelined, in which case the performance can even
exceed that of L<Redis::Fast>, which is much faster at parsing large
responses than this module.

L<AnyEvent> is an event-loop abstraction library.  This module does
not require it, but has been designed for compatibility with it.

=head1 LICENSE AND COPYRIGHT

This software is Copyright (c) 2025 by Brett Watson.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
