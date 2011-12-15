#!/usr/bin/perl -w

# DGAS Pushd.
# A. Guarise (guarise@to.infn.it)
# Condor parser based on information provided by
# Ken Schumacher (kschu@fnal.gov)
# and Philippe Canal (pcanal@fnal.gov), Fermilab, USA
# Much of the code for &parse_line() (including the primary regexp) from Joerk Behrends.

use strict;
use POSIX;
use Sys::Syslog;
use vars qw($PERL_SINGLE_QUOTE);
use Time::HiRes qw(usleep ualarm gettimeofday tv_interval);
use IO::Handle;

# turn off buffering of STDOUT
$| = 1;

my $sigset    = POSIX::SigSet->new();
my $actionHUP =
  POSIX::SigAction->new( "sigHUP_handler", $sigset, &POSIX::SA_NODEFER );
my $actionInt =
  POSIX::SigAction->new( "sigINT_handler", $sigset, &POSIX::SA_NODEFER );
POSIX::sigaction( &POSIX::SIGHUP,  $actionHUP );
POSIX::sigaction( &POSIX::SIGINT,  $actionInt );
POSIX::sigaction( &POSIX::SIGTERM, $actionInt );

my $dgasLocation = $ENV{DGAS_LOCATION};
if ( !defined($dgasLocation) || $dgasLocation eq "" ) {
	$dgasLocation = $ENV{GLITE_LOCATION};
	if ( $dgasLocation eq "" ) {
		$dgasLocation = "/usr/";
	}
}

my $configFilePath = "/etc/dgas/dgas_sensors.conf";
my %configValues   = (
	lockFileName => $dgasLocation . "/var/dgas-pushd-fileInput.lock",
	logFileName  => $dgasLocation . "/var/log/dgas/dgas_pushd_fileInput.log",
	mainPollInterval  => "5",
	systemLogLevel    => 7,
	maxThreadNumber   => "5",
	printAsciiLog     => "no",
	asciiLogFilePath  => $dgasLocation . "/var/log/dgas/pushdAscii.log",
	successTransport1 => "0",
	successTransport2 => "0",
	recordsDir        => $dgasLocation . "/var/records/",
);

my $systemLogLevel = 7;
my $logType        = 0;
my $LOGH;
my $ASCIILOG;
my $lastLog    = "";
my $logCounter = 0;
my @availableTransports;

#Parse configuration file
if ( exists $ARGV[0] ) {
	$configFilePath = $ARGV[0];
}
&parseConf($configFilePath);
&bootstrapLog( $configValues{logFileName} );

## FIXME POP LEgacy from @availabletransports, Legacy not allowed here.

## Global variable initialization

my $maxThreadNumber  = $configValues{maxThreadNumber};
my $lockFileName     = $configValues{lockFileName};
my $printAsciiLog    = 0;
my $asciiLogFilePath = $configValues{asciiLogFilePath};

if ( exists( $configValues{printAsciiLog} )
	&& $configValues{printAsciiLog} eq "yes" )
{
	&printLog( 5, "Using $asciiLogFilePath" );
	$printAsciiLog = 1;
	open( ASCIILOG, ">>$configValues{asciiLogFilePath}" )
	  || &printLog( 5, "Can't open $configValues{asciiLogFilePath}" );
}

&printLog( 4, "Daemon startup. Spawning $configValues{maxThreadNumber}" );
&printLog( 4, "Taking records from:$configValues{recordsDir}" );
&printLog( 5, "Got $configValues{asciiLogFilePath}" );
&printLog( 5, "Got $configValues{printAsciiLog}" );

my $keepGoing = 1;
my $start     = time();    ##  time init

#put lock
if ( &putLock($lockFileName) != 0 ) {
	&error("Fatal Error: Couldn't open lock file! in $lockFileName");
}
else {
	&printLog( 7, "Lock file succesfully created." );
}

my %successHoA;
foreach my $transport (@availableTransports) {
	my @successArray;
	if ( $configValues{"success$transport"} ne "" ) {
		@successArray = split( ';', $configValues{"success$transport"} );
		$successHoA{"$transport"} = [@successArray];
	}

}

foreach my $transport ( keys %successHoA ) {
	print "$transport @{ $successHoA{$transport} }\n";
}

while ($keepGoing) {

	#FIXME check for recordsDir directory existence.
	&printLog( 3, "Waiting for $configValues{recordsDir} to be created." );
	sleep 1;
}

my $limit = ( $configValues{maxThreadNumber} ) * 3;
while ($keepGoing) {
	LOGH->flush();
	my $t0             = [gettimeofday];
	my $successRecords = 0;
	my $failRecords    = 0;

	#FIXME Populate @FileList with record file names up to $limit number of files.
	my $numOfRecords = 0;
	my @fileList;
	opendir(DIR, $configValues{recordsDir}) || &error("Error: can't open dir $configValues{recordsDir}: $!");
	while( defined(my $file = readdir(DIR)) ) {
	    next if ( $file =~ /^\.\.?$/ ); # skip '.' and '..' 
	    for (my $i=0;$i<$limit;$i++)
	    {
	    	push @fileList, $file;
	    }
	}
	closedir DIR;
	if (@fileList) {
		$numOfRecords = @fileList;
	}
	#FIXME for each file in fileList produce a an executable command string one per each requested transport in transportList and push it into @commandlist
	my @commandList;
	while ($keepGoing && @fileList )
	{
		my $fileBuff = pop(@fileList);
		foreach my $transport ( @availableTransports )
		{
			my $commandBuff = "cat $fileBuff | $configValues{recordProducer$transport}";
			print "command: $commandBuff, protocol: $transport";
			push @commandList, ($fileBuff,$commandBuff,$transport);
		}
	}
	# FIXME process commands from commandList in threaded dispatcher:
	my $threadNumber = 0;
	while ($keepGoing
		&& @commandList
		&& ( $numOfRecords >= $maxThreadNumber ) )
	{
		my @file;
		my @executable;
		my @childs;
		my @command;
		my @transport;
		my @status;
		my @commandThreadNumber;

		for ( my $i = 0 ; $i < $maxThreadNumber ; $i++ ) {
			($file[$i],$executable[$i],$transport[$i]) = pop (@commandList);
			$status[$i]        = 0;
		}
		$numOfRecords = @commandList;
		for (
			$threadNumber = 0 ;
			$threadNumber < $maxThreadNumber ;
			$threadNumber++
		  )
		{
			my $pid = fork();
			if ($pid) {

				#parent
				push( @childs, $pid );
				$commandThreadNumber[$pid] = $threadNumber;
			}
			elsif ( $pid == 0 ) {
				##child
				if ( $file[$threadNumber] ) {
					&printLog( 8,
"Spawning new process:$threadNumber on record: $file[$threadNumber]"
					);
	
					&printLog( 8,
						"command:$executable[$threadNumber]" );
					&execCommand(
						$executable[$threadNumber],
						$status[$threadNumber],  $file[$threadNumber]
					);
				}
				LOGH->flush();
				exit( $status[$threadNumber] );
			}
			else {
				##no resource
				&printLog( 2,
					"Not enough system resources to spawn new process." );
			}
		}
		foreach (@childs) {

			#$counter++;
			my $numChilds = @childs;
			&printLog( 8, "$numChilds threads" );
			my $result = 0;
			waitpid( $_, 0 );
			if ( $? & 127 ) {
				$result = ( $? & 127 );
			}
			else {
				$result = $? >> 8;
			}
			&printLog( 9, "thread pid:$_,ThreadNumber:$commandThreadNumber[$_],status:$result");
			my $threadNumber  = $commandThreadNumber[$_];
			my $isSuccessfull = 1;
			foreach my $transportBuff (@availableTransports) {
				if ( $transport[$threadNumber] eq $transportBuff ) {
					foreach my $successValue ( @{ $successHoA{$transportBuff} } ) {
						if ( $result == $successValue ) {
							$isSuccessfull = 0;
						}
					}
				}
			}
			if ( $isSuccessfull == 0 ) {
				$successRecords++;
				&printLog( 9, "DELETE $file[$threadNumber]" );
				&delCommand( $file[$threadNumber] );
			}
			else {
				$failRecords++;
				&printLog( 9, "QUEUE $file[$threadNumber]" );
				&pushToQueue( $file[$threadNumber]);
			}
		}
	}
	for (
		my $i = 0 ;
		$keepGoing && ( $i < $configValues{mainPollInterval} ) ;
		$i++
	  )
	{
		usleep(10000);
	}
	my $elapsed = tv_interval( $t0, [gettimeofday] );
	&printLog( 9, "ELAPSED:$elapsed" );
	my $success_min  = ( $successRecords / $elapsed ) * 60;
	my $failure_min  = ( $failRecords / $elapsed ) * 60;
	my $total_min    = ( ( $failRecords + $successRecords ) / $elapsed ) * 60;
	my $min_krecords = 0.0;
	if ( $total_min > 0 ) {
		$min_krecords = 1000.0 / $total_min;
	}
	$success_min  = sprintf( "%.2f", $success_min );
	$failure_min  = sprintf( "%.2f", $failure_min );
	$total_min    = sprintf( "%.2f", $total_min );
	$min_krecords = sprintf( "%.1f", $min_krecords );
	if ( $failRecords + $successRecords > 0 ) {
		&printLog( 5,
"Success/min:$success_min, Fail/min:$failure_min, tot/min:$total_min, min/KRec:$min_krecords"
		);
	}
}
&Exit();

sub Exit () {
	&printLog( 8, "Exiting..." );
	if ( &delLock($lockFileName) != 0 ) {
		&printLog( 2, "Error removing lock file." );
	}
	else {
		&printLog( 5, "Lock file removed." );
	}
	&printLog( 4, "Exit." );
	exit(0);
}

sub bootstrapLog {
	my $logFilePath = $_[0];
	if ( $logFilePath =~ /SYSLOG(\d)/ ) {
		my $facility = "LOG_LOCAL" . $1;
		$logType = 1;
		openlog( "DGAS", 'ndelay', $facility );
	}
	else {
		open( LOGH, ">>$logFilePath" );
	}
	return 0;
}

sub printLog {
	my $logLevel = $_[0];
	my $log      = $_[1];
	if ( $logLevel <= $systemLogLevel ) {
		if ( $logType == 1 ) {
			my $pri = "";
		  SWITCH: {
				if ( $logLevel == 0 ) { $pri = 'crit';    last SWITCH; }
				if ( $logLevel == 1 ) { $pri = 'err';     last SWITCH; }
				if ( $logLevel == 2 ) { $pri = 'warning'; last SWITCH; }
				if ( $logLevel == 3 ) { $pri = 'warning'; last SWITCH; }
				if ( $logLevel == 4 ) { $pri = 'notice';  last SWITCH; }
				if ( $logLevel == 5 ) { $pri = 'notice';  last SWITCH; }
				if ( $logLevel == 6 ) { $pri = 'info';    last SWITCH; }
				if ( $logLevel == 7 ) { $pri = 'info';    last SWITCH; }
				if ( $logLevel == 8 ) { $pri = 'debug';   last SWITCH; }
				if ( $logLevel == 9 ) { $pri = 'debug';   last SWITCH; }
				my $nothing = 1;
			}
			syslog( $pri, $log );
		}
		else {
			my $localtime = localtime();
			if ( $log ne $lastLog ) {
				if ( $logCounter != 0 ) {
					print LOGH
					  "$localtime: Last message repeated $logCounter times.\n";
				}
				$logCounter = 0;
				print LOGH "$localtime: " . $log . "\n";
			}
			else {
				$logCounter++;
				if ( $logCounter == 20 ) {
					print LOGH "$localtime: Last message repeated 20 times.\n";
					$logCounter = 0;
				}
			}
			$lastLog = $log;
		}

	}
}

sub parseConf {
	my $fconf = $_[0];
	open( FILE, "$fconf" )
	  || die "Error: Cannot open configuration file: $fconf";
	while (<FILE>) {
		if (/\$\{(.*)\}/) {
			my $value = $ENV{$1};
			s/\$\{$1\}/$value/g;
		}
		if (/^pushdFileInputLockFileName\s*=\s*\"(.*)\"$/) {
			$configValues{lockFileName} = $1;
		}
		if (/^pushdFileInputLogFileName\s*=\s*\"(.*)\"$/) {
			$configValues{logFileName} = $1;
		}
		if (/^systemLogLevel\s*=\s*\"(.*)\"$/) {
			$configValues{systemLogLevel} = $1;
		}
		if (/^maxThreadNumber\s*=\s*\"(.*)\"$/) {
			$configValues{maxThreadNumber} = $1;
		}
		if (/^printAsciiLog\s*=\s*\"(.*)\"$/) {
			$configValues{printAsciiLog} = $1;
		}
		if (/^asciiLogFilePath\s*=\s*\"(.*)\"$/) {
			$configValues{asciiLogFilePath} = $1;
		}
		if (/^mainPollInterval\s*=\s*\"(.*)\"$/) {
			$configValues{mainPollInterval} = $1;
		}
		if (/^recordsDir\s*=\s*\"(.*)\"$/) {
			$configValues{recordsDir} = $1;
		}
		if (/^dgasDB\s*=\s*\"(.*)\"$/) { $configValues{dgasDB} = $1; }
		if (/^success(.*)\s*=\s*\"(.*)\"$/) {
			my $transportBuff        = $1;
			my $transportSuccessBuff = $2;
			$transportBuff =~ s/\s//g;
			$configValues{"success$transportBuff"} = $transportSuccessBuff;
			if ( $transportBuff ne "Legacy" ) {
				push @availableTransports, $transportBuff;
			}
		}
		if (/^recordProducer(.*)\s*=\s*\"(.*)\"$/) {
				my $producerNameBuff = $1;
				my $producerBuff     = $2;
				$producerNameBuff =~ s/\s//g;
				$configValues{"recordProducer$producerNameBuff"} =
				  $producerBuff;
			}
	}
	close(FILE);

	if ( exists( $configValues{systemLogLevel} )
		&& $configValues{systemLogLevel} ne "" )
	{
		$systemLogLevel = int( $configValues{systemLogLevel} );
	}
}


sub execCommand {
	my $executable  = $_[0];
	#my $statusParam = $_[1];
	my $file      = $_[3];
	$executable =~ s/\\"/"/g;
	my $status = -1;
	eval {
		local $SIG{ALRM} = sub { die "alarm\n" };    # NB: \n required
		alarm 10;
		$status = system("$executable &>/dev/null");
		alarm 0;
	};
	if ($@) {
		&printLog( 4, "Timeout on file:$file" );
		$status = -2;
		$_[2] = $status;
		return $status;
	}
	if ( $status & 127 ) {
		$status = ( $? & 127 );
	}
	else {
		$status = $? >> 8;
	}
	&printLog( 8,
		"Executing: file:$file :$executable EXIT_STATUS=$status" );
	&printLog( 5, "Executing:file:$file, EXIT_STATUS=$status" );
	if ( $printAsciiLog == 1 ) {
		print ASCIILOG "$executable;$file;#STATUS=$status\n";
	}
	$_[2] = $status;
	return $status;
}

sub delCommand {
	my $file    = $_[0];
	my $status = 0;
	&printLog( 7, "Deleting:$file" );
	&printLog( 8, "DELETE $file" );
	$status = unlink($configValues{recordsDir}."/".$file);
	if ( $status != 0)
	{
		&printLog( 9, "SUCCESS DELETING $file" );
	}
	return $status;
}

sub pushToQueue {
	#placeholder
	my $file           = $_[0];
	my $status        = 0;
	&printLog( 5, "Updating status:$file" );
	return $status;
}

sub putLock {
	my $lockName = $_[0];
	open( IN, "< $lockName" ) && return 1;
	close(IN);

	open( OUT, "> $lockName" ) || return 2;
	print OUT $$;    ## writes pid
	close(OUT);
	return 0;
}

sub existsLock {
	my $lockName = $_[0];
	if ( open( IN, "< $lockName" ) != 0 ) {
		close(IN);
		return 0;
	}
	return 1;
}

sub delLock {
	my $lockName = $_[0];
	open( IN, "< $lockName" ) || return 1;
	close(IN);
	my $status = system("rm -f $lockName");
	return $status;
}

sub sigHUP_handler {
	&printLog( 3, "got SIGHUP" );
	$keepGoing = 1;
	goto START;
}

sub sigINT_handler {
	&printLog( 3, "got SIGINT" );
	$keepGoing = 0;
}

sub error {
	if ( scalar(@_) > 0 ) {
		&printLog( 2, "$_[0]" );
	}
	exit(1);
}
