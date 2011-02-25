#!/usr/bin/perl -w

#Gianduia, DGAS job monitor and Usage Records collector.
use strict;
use POSIX;

my $sigset = POSIX::SigSet ->new();
my $actionHUP=POSIX::SigAction->new("sigHUP_handler",$sigset,&POSIX::SA_NODEFER);
my $actionInt=POSIX::SigAction->new("sigINT_handler",$sigset,&POSIX::SA_NODEFER);
POSIX::sigaction(&POSIX::SIGHUP, $actionHUP);
POSIX::sigaction(&POSIX::SIGINT, $actionInt);
POSIX::sigaction(&POSIX::SIGTERM, $actionInt);

my $processScript = "";
my $daemonStatus = "";

my $hostProxyScript = "";

#Parse configuration file
if(exists $ARGV[0]) 
{
    $processScript = $ARGV[0];
}
else
{
	print "You must specify the startup script to use";
}

if(exists $ARGV[1]) 
{
    $hostProxyScript = $ARGV[1];
}	

## Global variable initialization

my $T = 30;

my $keepGoing = 1;

my $iteration = 0;

while( $keepGoing )
{
	sleep($T);

	# check host cert proxy, if specified on command line
	# (every 1000 iterations; about every 50 minutes if $T==3):
	if ($hostProxyScript ne "")
	{
	    if ( ($iteration % 1000) == 0 )
	    {
		# happens the first time and then every 1000 iterations
		system("$hostProxyScript");
		$iteration = 0; # start anew
	    }

	    $iteration++;
	}


	# check daemon status (each iteration):
	$daemonStatus = system("$processScript status");
	print "The daemon status is: $daemonStatus\n";
	if ( $daemonStatus != 0 )
	{
		
		#The daemon needs to be restarted;
		system("$processScript restartMain");
	}

}


###-----------------------END-----------------------------------------###


##-------> sig handlers subroutines <---------##

sub sigHUP_handler {
        print "got SIGHUP\n";
	$keepGoing = 1;
}

sub sigINT_handler {
        print "got SIGINT\n";
        $keepGoing = 0;
}
