#!/usr/bin/perl -w

#usage composerCommand | dgas-record-filter.pl [!]matchRule1 [!]matchRule2 ... [!]matchRuleN  [file:matchRuleFile] "producerCommand" [exitWith:exitStatusNumber]
#Producer command is invoked just if one of the specified rules (as regexps) is NOT matched on the record coming in stdin from the composer command.
#If no rules get matched then the UsageRecord is sent to the producer and thus arrives to the HLR. 
#If any rule is matched the record is not passed on to the producer and and 'exitStatusNumber' is returned. If exitStatusNumber is not specified, 'zero' is returned
#by default. You shold take kare of instructing sensors configuration to consider exitStatusNumber as succesfull for deletion from the queue, otherwies those records matched
#to be ignored would remain in the queue forever.
#rules can be specifiaed one per line also in files defined by file:matchRuleFile
#logic of regexp rules can be inverted prepending a ! character to the rule itself.


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
			if ( $_ =~ /^!(.*)$/ )#negate
			{
				if ( $docBuffer !~ /$1/ )
				{
					# discard records NOT matching the rule after the !
					exit 0;
				}
			}
			elsif ( $docBuffer =~ /$_/ ) 
			{

					#The objective of the command is to have sensors discard records matching the rule,
					# thus the exist status is 0
				exit 0;
			}
		}
		close RULEFILE;
	}
	else 
	{
		if ($item =~ /^!(.*)$/ )#negate
		{
			if ( $docBuffer !~ /$1/ )
			{
					# discard records NOT matching the rule after the !
					exit 0;
			}
		}
		elsif ( $docBuffer =~ /$item/ ) 
		{

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

