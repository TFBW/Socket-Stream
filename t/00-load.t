#!perl
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 3;

BEGIN {
    use_ok( 'Socket::Stream' ) || print "Bail out!\n";
    use_ok( 'Socket::Stream::RESP2Parser' ) || print "Bail out!\n";
    use_ok( 'Socket::Stream::RESP2Client' ) || print "Bail out!\n";
}

diag( "Testing Socket::Stream $Socket::Stream::VERSION, Perl $], $^X" );
