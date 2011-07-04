#!/usr/bin/perl -w

#usage composerCommand | dgas-record-filter.pl [!]matchRule1 [!]matchRule2 ... [!]matchRuleN  [file:matchRuleFile] "producerCommand" [exitWith:exitStatusNumber]
#Producer command is invoked just if one of the specified rules (as regexps) is matched on the record coming in stdin from the composer command.
#If no rules get matched then the UsageRecord is NOT sent to the producer and thus DO NOT arrives to the HLR and 'exitStatusNumber' is returned. 
#If exitStatusNumber is not specified, 'zero' is returned
#by default. You shold take kare of instructing sensors configuration to consider exitStatusNumber as succesfull for deletion from the queue, otherwies those records 
#not matched to be forwarded would remain in the queue forever.
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

my $discardExitStatus = 0;
my $forward = 1;

#The last argument from the command line is the producer command
my $command = pop(@ARGV);
if ( $command =~ /^exitWith:(\d+)$/)
{
        #last parameter used to change exit status on match
        $discardExitStatus=$1;
        $command= pop(@ARGV);
} 

foreach my $item (@ARGV) {
        if ( $item =~ /^file:(.*)$/ ) {
                my $val = $1;
                open( RULEFILE, $val ) or die "File! $val";
                while (<RULEFILE>) {
                        if ( $_ =~ /^!(.*)$/ )#negate rule
                        {
                                if ( $docBuffer !~ /$1/ )
                                {
                                        # FORWADR records NOT matching the rule after the !
                                        $forward = 0;
                                }
                        }
                        elsif ( $docBuffer =~ /$_/ )
                        {

                                        #FORWARD records matching the rule,
                                        # thus the exist status is discardExitsStatus
                                $forward = 0;
                        }
                }
                close RULEFILE;
        }
        else
        {
                if ($item =~ /^!(.*)$/ )#negate rule
                {
                        if ( $docBuffer !~ /$1/ )
                        {
                                        # FORWARD records NOT matching the rule after the !
                                        $forward = 0;
                        }
                }
                elsif ( $docBuffer =~ /$item/ )
                {
                        #FORWADR records matching the rule,
                        # thus the exist status is discardExitsStatus
                        $forward = 0;
                }
        }

}

if ( $forward != 0 )
{
	#Record did not match any rule: DO NOT pass it to producer and exit with exit status == 'exitWith'
	exit $discardExitStatus;
}

#Record matched one of the rules: pass it to message producer.
my $status = system("echo \'$docBuffer\' | $command");
if ( $status & 127 ) {
        $status = ( $? & 127 );
}
else {
        $status = $? >> 8;
}

#if the command has been executed, return its true exit status
exit $status;

