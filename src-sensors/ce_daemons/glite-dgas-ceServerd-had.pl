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

#Parse configuration file
if(exists $ARGV[0]) 
{
    $processScript = $ARGV[0];
}
else
{
	print "You must specify the startup script to use";
}	

## Global variable initialization

my $T = 3;

my $keepGoing = 1;

while( $keepGoing )
{
	sleep($T);
	$daemonStatus = system("$processScript status");
	print "The daemon status is: $daemonStatus\n";
	if ( $daemonStatus != 0 )
	{
		
		#The daemon need to be restarted;
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
