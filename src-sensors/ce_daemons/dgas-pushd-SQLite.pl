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
use DBI;
use Time::HiRes qw(usleep ualarm gettimeofday tv_interval);
use IO::Handle;

# turn off buffering of STDOUT
#$| = 1;


my $sigset = POSIX::SigSet ->new();
my $actionHUP=POSIX::SigAction->new("sigHUP_handler",$sigset,&POSIX::SA_NODEFER);
my $actionInt=POSIX::SigAction->new("sigINT_handler",$sigset,&POSIX::SA_NODEFER);
POSIX::sigaction(&POSIX::SIGHUP, $actionHUP);
POSIX::sigaction(&POSIX::SIGINT, $actionInt);
POSIX::sigaction(&POSIX::SIGTERM, $actionInt);

my $dgasLocation = $ENV{DGAS_LOCATION};
if ( !defined($dgasLocation) ||  $dgasLocation eq "" )
{
	$dgasLocation = $ENV{GLITE_LOCATION};
	if ( $dgasLocation eq "" )
	{
		$dgasLocation = "/usr/";
	}
}

my $configFilePath = "/etc/dgas/dgas_sensors.conf"; 
my %configValues = (
		    lockFileName      =>  "/var/lock/dgas/dgas-pushd.lock",
		    logFileName       =>  "/var/log/dgas/dgas_pushd.log",
		    mainPollInterval  => "5",
		    systemLogLevel	      => 7,
		    maxThreadNumber           => "5",
		    printAsciiLog => "no",
		    asciiLogFilePath => "/var/log/dgas/pushdAscii.log",
		    dgasDB 	=> "/var/spool/dgas/dgas.sqlite",			
		    successLegacy 	=> "0;64;65;68;69;70;71;73",			
		    successTransport1 	=> "0",			
		    successTransport2 	=> "0",			
	commandTimeout    => "30",
);

my $systemLogLevel = 7;
my $logType = 0;
my $LOGH;
my $ASCIILOG;
my $lastLog = "";
my $logCounter = 0;
my @availableTransports;
push @availableTransports, "Legacy";

#Parse configuration file
if(exists $ARGV[0]) 
{
    $configFilePath = $ARGV[0];
}	
&parseConf($configFilePath); 
&bootstrapLog($configValues{logFileName});


## Global variable initialization


my $maxThreadNumber = $configValues{maxThreadNumber};
my $lockFileName = $configValues{lockFileName};
my $printAsciiLog = 0;
my $asciiLogFilePath =$configValues{asciiLogFilePath};


if (exists($configValues{printAsciiLog})
    && $configValues{printAsciiLog} eq "yes") {
        &printLog ( 5, "Using $asciiLogFilePath");
    $printAsciiLog = 1;
    open ( ASCIILOG, ">>$configValues{asciiLogFilePath}" ) || &printLog ( 5, "Can't open $configValues{asciiLogFilePath}");
}

&printLog(4, "Daemon startup. Spawning $configValues{maxThreadNumber}");
&printLog(4, "Using Database file:$configValues{dgasDB}");
&printLog ( 5, "Got $configValues{asciiLogFilePath}");
&printLog ( 5, "Got $configValues{printAsciiLog}");
my $dbh = DBI->connect( "dbi:SQLite:$configValues{dgasDB}" ) || &printLog(1, "Error opening database.") && &Exit(); 
$dbh->{AutoCommit} = 0;  # enable transactions, if possible
  $dbh->{RaiseError} = 1;

my $keepGoing = 1;
my $start = time();   ##  time init

#put lock
if ( &putLock($lockFileName) != 0 )
{
    &error("Fatal Error: Couldn't open lock file! in $lockFileName");
}
else
{
	&printLog ( 7, "Lock file succesfully created.");
}

my %successHoA;
foreach my $transport ( @availableTransports )
{
	my @successArray;
	if ( $configValues{"success$transport"} ne "" )
	{
        	@successArray = split(';',$configValues{"success$transport"});
        	$successHoA{"$transport"} = [ @successArray ];
	}

}

foreach my $transport ( keys %successHoA )
{
	print "$transport @{ $successHoA{$transport} }\n";
}

while ($keepGoing )
{
	my $sth = $dbh->table_info( '%', '%', 'commands', '' );
    my $table = $sth->fetchall_arrayref;
    $dbh->commit;
    if (@$table) 
    { 
		last;
    }
    &printLog ( 3, "Waiting for $configValues{dgasDB}:commands table to be created by urcollector.");
    sleep 1;
} 


my $limit = ($configValues{maxThreadNumber})*3;
#my $counter =0; 
while( $keepGoing )
{
    LOGH->flush();
    my $t0 = [gettimeofday];
    my $successRecords = 0;
    my $failRecords = 0;       
    # first get all interesting keys from dgasDB.commands:
	#FIXME Populate @commandKeys with DB query
    my $commandKeys = $dbh->selectall_arrayref( " SELECT key,transport,composer,arguments,producer,commandStatus FROM commands ORDER by commandStatus desc limit $limit" );
    $dbh->commit;
    my $numOfRecords = 0;
    if (@$commandKeys) { 
		$numOfRecords = @$commandKeys;
	};
    # process files (threaded dispatcher):
    my $threadNumber = 0;
    while ($keepGoing && @$commandKeys && ( $numOfRecords >= $maxThreadNumber ))
    {
    	my @childs;
	my @key;
	my @protocol;
	my @composer;
	my @args;
	my @producer;
	my @command;
	my @commandStatus;
	my @status;
	my @commandThreadNumber;
	for ( my $i = 0; $i < $maxThreadNumber; $i++)
	{
		my $keyBuffer = pop (@$commandKeys);
		$key[$i] = $$keyBuffer[0];
		$protocol[$i] = $$keyBuffer[1];
		$composer[$i] = $$keyBuffer[2];
		$args[$i] = $$keyBuffer[3];
		$producer[$i] = $$keyBuffer[4];
		$commandStatus[$i] = $$keyBuffer[5];
		$status[$i] = 0;
	}
	$numOfRecords = @$commandKeys;
	for ( $threadNumber = 0; $threadNumber < $maxThreadNumber; $threadNumber++)
	{
		my $pid = fork();	
		if ( $pid )
		{
			#parent
			push(@childs, $pid);
			$commandThreadNumber[$pid] = $threadNumber;
		}
		elsif ( $pid == 0 )
		{
			##child
			if ( $key[$threadNumber] )
			{
					&printLog( 8,
"Spawning new process:$threadNumber on record: $key[$threadNumber]"
					);
					$command[$threadNumber] = &getCommand(
						$key[$threadNumber],  $composer[$threadNumber],
						$args[$threadNumber], $producer[$threadNumber]
					);
					&printLog( 8,
						"command:$threadNumber = $command[$threadNumber]" );
					&execCommand(
						$command[$threadNumber], $protocol[$threadNumber],
						$status[$threadNumber],  $key[$threadNumber], $configValues{commandTimeout}
					);
			}
			LOGH->flush();
			exit($status[$threadNumber]);
		}
		else
		{
			##no resource
			&printLog ( 2, "Not enough system resources to spawn new process." );
		}
	}
	foreach (@childs)
	{
		#$counter++;
		my $numChilds = @childs;
		&printLog ( 8, "$numChilds threads");
		my $result = 0;
		waitpid($_,0);
		if ($? & 127) 
		{
                	$result = ($? & 127);
            	}
            	else 
		{
                	$result = $? >> 8;
            	}
		&printLog (9,"thread pid:$_,ThreadNumber:$commandThreadNumber[$_],status:$result");
		my $threadNumber = $commandThreadNumber[$_];
		my $isSuccessfull = 1;
		foreach my $transport ( @availableTransports )
		{
			if ( $protocol[$threadNumber] eq $transport )
			{
				foreach my $successValue ( @{ $successHoA{$transport} } )
				{
					if ( $result == $successValue )
					{
						$isSuccessfull = 0;
					}
				}
			}
		}
		if ( $isSuccessfull == 0 )
		{
			$successRecords++;
			&printLog (9, "DELETE $key[$threadNumber]");
			&delCommand($key[$threadNumber]);
		}
		else
		{	$failRecords++;
			&printLog (9, "QUEUE $key[$threadNumber]");
			&pushToQueue($key[$threadNumber],$commandStatus[$threadNumber]);
		}
	}
    }
for (my $i = 0; $keepGoing && ( $i < $configValues{mainPollInterval} ); $i++)
{
	usleep(100000);
}
my $elapsed = tv_interval ($t0, [gettimeofday]);
&printLog ( 9,"ELAPSED:$elapsed");
my $success_min;
my $failure_min;
my $total_min;
if ( $elapsed != 0 )
{
        $success_min = ($successRecords/$elapsed)*60;
        $failure_min = ($failRecords/$elapsed)*60;
        $total_min = (($failRecords+$successRecords)/$elapsed)*60;
}
else
{
        $success_min = ($successRecords/1)*60;
        $failure_min = ($failRecords/1)*60;
        $total_min = (($failRecords+$successRecords)/1)*60;
}
my $min_krecords = 0.0;
if ( $total_min > 0 )
{
        $min_krecords = 1000.0/$total_min;
}
$success_min = sprintf("%.2f", $success_min);
$failure_min = sprintf("%.2f", $failure_min);
$total_min = sprintf("%.2f", $total_min);
$min_krecords = sprintf("%.1f", $min_krecords);
if ( $failRecords+$successRecords > 0 )
{
	&printLog ( 5,"Success/min:$success_min, Fail/min:$failure_min, tot/min:$total_min, min/KRec:$min_krecords");
}
}
&Exit();

sub Exit ()
{
	&printLog ( 8 , "Exiting..." );
	if ( &delLock($lockFileName) != 0 )
	{	
	    &printLog ( 2, "Error removing lock file." );
	}
	else
	{
		&printLog ( 5, "Lock file removed." );
	}
	&printLog ( 4, "Exit." );
	exit(0);
}

sub bootstrapLog 
{
	my $logFilePath = $_[0];
	if ( $logFilePath =~ /SYSLOG(\d)/ )
	{
		my $facility = "LOG_LOCAL".$1; 
		$logType = 1;
		openlog("DGAS", 'ndelay', $facility);
	}
	else
	{
		open ( LOGH, ">>$logFilePath" );
	}
	return 0;
}

sub printLog 
{
        my $logLevel = $_[0];
        my $log = $_[1];
        if ( $logLevel <= $systemLogLevel )
        {
		if ( $logType == 1 )
		{
			my $pri = "";
			SWITCH: {
               			if ($logLevel == 0) { $pri = 'crit'; last SWITCH; }
               			if ($logLevel == 1) { $pri = 'err'; last SWITCH; }
               			if ($logLevel == 2) { $pri = 'warning'; last SWITCH; }
               			if ($logLevel == 3) { $pri = 'warning'; last SWITCH; }
               			if ($logLevel == 4) { $pri = 'notice'; last SWITCH; }
               			if ($logLevel == 5) { $pri = 'notice'; last SWITCH; }
               			if ($logLevel == 6) { $pri = 'info'; last SWITCH; }
               			if ($logLevel == 7) { $pri = 'info'; last SWITCH; }
               			if ($logLevel == 8) { $pri = 'debug'; last SWITCH; }
               			if ($logLevel == 9) { $pri = 'debug'; last SWITCH; }
               			my $nothing = 1;
           		}
			syslog ( $pri, $log );
		}
		else
		{	
			my $localtime = localtime();
                        if ( $log ne $lastLog ) {
                                if ( $logCounter != 0 ) {
                                        print LOGH
                                          "$localtime: Last message repeated $logCounter times.\n";
                                }
                                $logCounter = 0;
                                print LOGH "$localtime: " . $log . "\n";
                                if ( $_[2] )
                                {
                                        select(LOGH);
                                        $|++;
                                }
                        }
                        else {
                                $logCounter++;
                                if ( $logCounter == 20 ) {
                                        print LOGH "$localtime: Last message repeated 20 times.\n";
                                        select(LOGH);
                                        $|++;
                                        $logCounter = 0;
                                }
                        }
                        select(LOGH);
                        $|=0;
                        $lastLog = $log;
		}

        }
}

sub parseConf 
{
    my $fconf = $_[0];
    open(FILE, "$fconf") || die "Error: Cannot open configuration file: $fconf";
    while(<FILE>)
    {
	if (/\$\{(.*)\}/)
        {
                my $value=$ENV{$1};
                s/\$\{$1\}/$value/g;
        }
	if(/^pushdLockFileName\s*=\s*\"(.*)\"$/){$configValues{lockFileName}=$1;}
	if(/^pushdLogFileName\s*=\s*\"(.*)\"$/){$configValues{logFileName}=$1;}
	if(/^systemLogLevel\s*=\s*\"(.*)\"$/){$configValues{systemLogLevel}=$1;}
	if(/^maxThreadNumber\s*=\s*\"(.*)\"$/){$configValues{maxThreadNumber}=$1;}
	if(/^printAsciiLog\s*=\s*\"(.*)\"$/){$configValues{printAsciiLog}=$1;}
	if(/^asciiLogFilePath\s*=\s*\"(.*)\"$/){$configValues{asciiLogFilePath}=$1;}
	if(/^mainPollInterval\s*=\s*\"(.*)\"$/){$configValues{mainPollInterval}=$1;}
	if (/^commandTimeout\s*=\s*\"(.*)\"$/){$configValues{commandTimeout} = $1;}
	if(/^dgasDB\s*=\s*\"(.*)\"$/){$configValues{dgasDB}=$1;}
	if(/^success(.*)\s*=\s*\"(.*)\"$/)
	{
		my $transportBuff = $1;
		my $transportSuccessBuff = $2;
		$transportBuff =~ s/\s//g; 
		$configValues{"success$transportBuff"}=$transportSuccessBuff;
		if ( $transportBuff ne  "Legacy" )
		{
			push @availableTransports, $transportBuff;
		}
	}
    }
    close(FILE);

    if (exists($configValues{systemLogLevel})
	       && $configValues{systemLogLevel} ne "") {
	$systemLogLevel = int($configValues{systemLogLevel});
    }
}

sub getCommand
{
	my $key = $_[0];
	my $composer = $_[1];
	my $args = $_[2];
	my $producer = $_[3];
	my $command;
        $command = "set -o pipefail; $composer $args";
        	if ( $producer ne "" )
        	{
                	$command .= " | $producer";
        	}
	return $command;
}

sub execCommand
{
	my $command = $_[0];
	my $protocol = $_[1];
	my $key = $_[3];
	my $timeout  = $_[4];
	$command =~ s/\\"/"/g;
	my $status = -1;
	if ( $timeout == -1 )
	{
		$status = system("$command &>/dev/null");
	}
	else
	{
	eval {
		local $SIG{ALRM} = sub { die "alarm\n" }; # NB: \n required
			alarm $timeout;
        	$status = system("$command &>/dev/null");
		alarm 0;
	};
	if ( $@ )
	{
		&printLog ( 4, "Timeout, $protocol, key=:$key" );
		$status = -2;
		$_[2] = $status;
        	return $status;
	}
	}
	if ($status & 127) 
	{
               	$status = ($? & 127);
        } 
        else 
	{
               	$status = $? >> 8;
        }
	&printLog ( 8, "Executing:$protocol, key=$key :$command EXIT_STATUS=$status" );
	&printLog ( 5, "Executing:$protocol, key=$key, EXIT_STATUS=$status" );
	if ( $printAsciiLog == 1)
        {
                print ASCIILOG "$command;$protocol;#STATUS=$status\n";
        }
	$_[2] = $status;
	return $status;
}

sub delCommand
{
	my $key = $_[0];
	my $status = 0;
	&printLog ( 7, "Deleting:$key" );
	my $delString = "DELETE FROM commands WHERE key=$key";
	&printLog ( 8, "DELETE FROM commands WHERE key=$key" );
	my $querySuccesfull = 1;
        my $queryCounter = 0;
        while ($keepGoing && $querySuccesfull)
        {
		eval {
                        my $res = $dbh->do( $delString );
                };
                if ( $@ )
                {
                        &printLog ( 3, "WARN: ($queryCounter) $@" );
                        print "Retrying in $queryCounter\n";
                        for ( my $i =0; $keepGoing && ( $i < $queryCounter ) ; $i++ )
                        {
                                sleep $i;
                        }
                        $queryCounter++;
                }
                else
                {
                        $querySuccesfull = 0;
                        &printLog ( 9, "SUCCESS: $delString" );
                }
                last if ( $queryCounter >= 10 );
	}
	return $status;
}

sub pushToQueue
{
	my $key = $_[0];
	my $commandStatus = $_[1];
	my $status = 0;
	&printLog ( 5, "Updating status:$key" );
	eval {
    		my $res = $dbh->selectall_arrayref( "UPDATE commands SET commandStatus=$commandStatus-1 WHERE key=$key" );
		&printLog ( 8, "UPDATE commands SET commandStatus=$commandStatus-1 WHERE key=$key" );
	};
	if ($@)
	{
		&printLog ( 4, "Error:$@" );
	}
	return $status;
}

sub putLock
{
    my $lockName = $_[0];
    open(IN,  "< $lockName") && return 1;
    close(IN);

    open(OUT, "> $lockName") || return 2;
    print OUT  $$;    ## writes pid   
    close(OUT);
    return 0;
}

sub existsLock
{
    my $lockName = $_[0];
    if ( open(IN,  "< $lockName") != 0 )
    {
    	close(IN);
	return 0;
    }
    return 1;
}

sub delLock
{
    my $lockName = $_[0];
    open(IN,  "< $lockName") || return 1;
    close(IN);
    my $status = system("rm -f $lockName");
    return $status;
}

sub sigHUP_handler {
        &printLog (3, "got SIGHUP");
	$keepGoing = 1;
	goto START;
}

sub sigINT_handler {
        &printLog (3, "got SIGINT");
        $keepGoing = 0;
}

sub error {
    if (scalar(@_) > 0) {
	&printLog (2, "$_[0]");
    }
    exit(1);
}
