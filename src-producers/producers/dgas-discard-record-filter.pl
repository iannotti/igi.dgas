#!/usr/bin/perl -w

use strict;
use Getopt::Long;

my $docBuffer;

binmode(STDIN);
open( STDIN, "< -" );
while (<STDIN>) {
	$docBuffer .= $_;
}
close(STDIN);

#The last argument from the command line is the producer command
my $command = pop(@ARGV);

foreach my $item (@ARGV) {
	if ( $item =~ /^file:(.*)$/ ) {
		my $val = $1;
		open( RULEFILE, $val ) or die "File! $val";
		while (<RULEFILE>) {
			if ( $docBuffer =~ /$_/ ) {

#The objective of the command is to have sensors discard records matching the rule,
# thus the exist status is 0
				exit 0;
			}
		}
		close RULEFILE;
	}
	else {
		if ( $docBuffer =~ /$item/ ) {

#The objective of the command is to have sensors discard records matching the rule,
# thus the exist status is 0
			exit 0;
		}
	}

}
my $status = system("echo \'$docBuffer\' | $command");
if ( $status & 127 ) {
	$status = ( $? & 127 );
}
else {
	$status = $? >> 8;
}

#if the command has been executed, return its true exit status
exit $status;

