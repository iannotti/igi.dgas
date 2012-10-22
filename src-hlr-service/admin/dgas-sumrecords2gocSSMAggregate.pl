#!/usr/bin/perl -w -I /usr/lib/perl5/vendor_perl/5.8.5 -I /usr/lib/perl5/vendor_perl/5.8.0

use strict;
use POSIX;
use Getopt::Std;
use DBI();
use Time::Local;
use strict;
use Digest::MD5 qw(md5 md5_hex md5_base64);

#no buffering of output:
select STDERR;
$| = 1;
select STDOUT;
$| = 1;

my $keepGoing = 1;
my $systemLogLevel = 7;
my $prgname        = "dgas-jts2goc_summary.pl";
my $voSinglePassage = 1;
my $voString;
my $logType = 0;
my $LOGH;
my $lastLog = "";
my $logCounter = 0;

my $sigset = POSIX::SigSet ->new();
my $actionHUP=POSIX::SigAction->new("sigHUP_handler",$sigset,&POSIX::SA_NODEFER);
my $actionInt=POSIX::SigAction->new("sigINT_handler",$sigset,&POSIX::SA_NODEFER);


if ( !exists( $ENV{GLITE_LOCATION} ) || $ENV{GLITE_LOCATION} eq "" )
{
	$ENV{GLITE_LOCATION} = "/usr/";
	print "Environment variable GLITE_LOCATION not defined! Trying /usr/ ...\n";
}

my $conffile = "/etc/dgas-sumrecords2goc.conf";

my %workparameters;
my @voList;
my @siteList;

# -------------------
# getting options ...
# -------------------

my %opts;
$opts{h} = "";
$opts{c} = "";

getopts( 'hc:', \%opts );

if ( $opts{h} eq "1" )
{
	&printUsageInfo();
}

if ( $opts{c} ne "" )
{
	$conffile = $opts{c};
}

# defaults for DGAS HLR db:
# This is the database where the original HLR accounting information is taken
# from.
$workparameters{"hlrDbServer"}   = "localhost";
$workparameters{"hlrDbPort"}     = 3306;
$workparameters{"hlrDbUser"}     = "root";
$workparameters{"hlrDbPassword"} = "";
$workparameters{"hlrDbName"}     = "hlr";
$workparameters{"hlrDbTable"}    = "jobTransSummary";
$workparameters{"endTimeStart"}  =
  "2011-01-01";    #DEFAULT: start from this date to process records.
$workparameters{"endTimeStop"} =
  "2021-12-31";    #DEFAULT: stop on this date to precess records.
$workparameters{"lockFile"} = "$ENV{GLITE_LOCATION}/var/lock/dgas2gocSSM.lock";
$workparameters{"logFile"}  = "$ENV{GLITE_LOCATION}/var/log/dgas2gocSSM.log";
$workparameters{"processLocalJobs"} =
  0;               #DEFAULT: only jobs with voOrigin=[fqan,pool]
$workparameters{"SSMOutputDir"}         = "$ENV{GLITE_LOCATION}/var/messages/";
$workparameters{"maxRecordsPerMessage"} = 1000;
$workparameters{"voListFile"}           = "$ENV{GLITE_LOCATION}/etc/voList.txt";
$workparameters{"siteListFile"} = "$ENV{GLITE_LOCATION}/etc/siteList.txt";
$workparameters{"minStartDate"} = "";
$workparameters{"maxEndDate"} = "";
$workparameters{"commandTimeOut"} = 600;
$workparameters{"senderTimeZone"} = "Europe/Rome";
$workparameters{"receiverTimeZone"}  = "UTC";

# read configuration from file:
&readConfiguration();

# ------------------
# starting procedure
# ------------------

my $exitStatus = 0;    # 0 if ok

if ( &putLock($workparameters{lockFile}) != 0 )
{
    &printLog(1,"Fatal Error: Couldn't open lock file! in $workparameters{lockFile}");
}
else
{
	&printLog ( 4, "Lock file succesfully created: $workparameters{lockFile}");
}

&printLog( 4, "Starting writing SSM records messages in $workparameters{SSMOutputDir}" );
	my $setTZ = "SET time_zone='$workparameters{receiverTimeZone}'";
	#In the query below 900000 = 3600*250 is used to roughly convert specInts to HEP specs.
	my $queryString = "SELECT siteName,
		year(CONVERT_TZ(endDate,'$workparameters{senderTimeZone}','$workparameters{receiverTimeZone}')) AS year_tz,
		month(CONVERT_TZ(endDate,'$workparameters{senderTimeZone}','$workparameters{receiverTimeZone}')) AS month_tz,
		userVo,userFqan,
		gridUser,
		count(*),
		UNIX_TIMESTAMP(min(CONVERT_TZ(endDate,'$workparameters{senderTimeZone}','$workparameters{receiverTimeZone}'))),
		UNIX_TIMESTAMP(max(CONVERT_TZ(endDate,'$workparameters{senderTimeZone}','$workparameters{receiverTimeZone}'))),
		sum(wallTime)/3600,
		sum(cpuTime)/3600,
		sum(wallTime*iBench)/900000,     
		sum(cpuTime*iBench)/900000 
		FROM $workparameters{hlrDbTable} 
		WHERE endDate>=\"$workparameters{endTimeStart}\" ";
		if ( $workparameters{processLocalJobs} == 0 )
		{
			#process only grid jobs (voOrigin=[fqan,pool])
			$queryString .= "AND ( voOrigin=\"fqan\" OR voOrigin=\"pool\" ) ";
		}
		$queryString .= "GROUP BY siteName,year_tz,month_tz,userVo,userFqan,gridUser";
	
	my $dsn = "dbi:mysql:$workparameters{hlrDbName}:$workparameters{hlrDbServer}:$workparameters{hlrDbPort}";
	&printLog(7, "Database: $dsn" );
	&printLog(7, "Query: $queryString" );
	my $dbh = DBI->connect($dsn,$workparameters{hlrDbUser}, $workparameters{hlrDbPassword}
	           ) || die "Could not connect to database: $DBI::errstr";
	my $sthTZ = $dbh->prepare($setTZ);
	$sthTZ->execute() || die "Couldn't execute statement: " . $sthTZ->errstr;
	my $sth = $dbh->prepare($queryString);
	$sth->execute() || die "Couldn't execute statement: " . $sth->errstr;
	POSIX::sigaction(&POSIX::SIGHUP, $actionHUP);
	POSIX::sigaction(&POSIX::SIGINT, $actionInt);
	POSIX::sigaction(&POSIX::SIGTERM, $actionInt);
	my @data;
	my $apelMessage;
	my $messageCounter = 0;
	while ($keepGoing && (@data = $sth->fetchrow_array())) {
            my $siteName = $data[0];
            my $year = $data[1];
            my $month = $data[2];
            my $userVo = $data[3];
            my $userFqan = $data[4];
            my $gridUser = $data[5];
	    my $numRecords = $data[6];
	    my $earliestEndTime = $data[7];
	    my $latestEndTime = $data[8];
	    my $wallDuration = sprintf "%.0f", $data[9]; #Round half to even
	    my $cpuDuration = sprintf "%.0f", $data[10];
	    my $normalisedWallDuration = sprintf "%.0f", $data[11]; #HEP SPEC = si2k/250, h hepspec = si2k/(3600*250) = si2k/900000
	    my $normalisedCpuDuration = sprintf "%.0f", $data[12];
	    my $VOGroup = "";
	    my $VORole = "";
	    my @fqanList = split(';',$userFqan);
	    if ( @fqanList )
	    {
	    my $primaryFqan = $fqanList[0];
		    ($VOGroup,$VORole) = ($primaryFqan =~ /^(.*)\/Role=(.*)\/(.*)$/);
	    }
	    my ($vofound, $voItem, $sitefound, $siteItem);
	    foreach $voItem (@voList)
	    {
	    	if ( $userVo eq $voItem )
	    	{
	    		$vofound = 1;
	    		last;
	    	}
	    }
	    if ( !$vofound )
	    {
	    	&printLog(7, "Skipping record with vo: $userVo" );
	    	next;
	    }
	    
	    foreach $siteItem (@siteList)
	    {
	    	if ( $siteName eq $siteItem )
	    	{
	    		$sitefound = 1;
	    		last;
	    	}
	    }
	    if ( $sitefound )
	    {
	    	&printLog(7, "Record: $siteName: $year $month $userVo" );
	    }
	    else
	    {
	    	&printLog(7, "Skipping record with sitename: $siteName" );
	    	next;
	    }
        if ( $messageCounter == 0 )
        {
        	$apelMessage = "APEL-summary-job-message: v0.2\n";
        }
        $messageCounter++;
        $apelMessage .= "Site: $siteName\n";
        $apelMessage .= "Month: $month\n";
        $apelMessage .= "Year: $year\n";
        $apelMessage .= "GlobalUserName: $gridUser\n";
        $apelMessage .= "Group: $userVo\n";
        $apelMessage .= "VOGroup: $VOGroup\n";
        $apelMessage .= "VORole: $VORole\n";
        $apelMessage .= "EarliestEndTime: $earliestEndTime\n";
        $apelMessage .= "LatestEndTime: $latestEndTime\n";
        $apelMessage .= "WallDuration: $wallDuration\n";
        $apelMessage .= "CpuDuration: $cpuDuration\n";
        $apelMessage .= "NormalisedWallDuration: $normalisedWallDuration\n";
        $apelMessage .= "NormalisedCpuDuration: $normalisedCpuDuration\n";
        $apelMessage .= "NumberOfJobs: $numRecords\n";
        $apelMessage .= "%%\n";
        if ( $messageCounter == $workparameters{maxRecordsPerMessage} )
        {
        	&printLog( 7, "Write Message to disk" );
        	#Do write the message and reset the counter and the string.
        	$messageCounter = 0;
        	my $res = &writeMessage($apelMessage,$workparameters{SSMOutputDir});
        	if ( $res != 0 )
        	{
        		&printLog( 4, "Warning could not write message to disk." );
        	}
        	$apelMessage = "APEL-summary-job-message: v0.2\n";	
        }
    }
    if ( $messageCounter != 0 )
    {
    	&writeMessage($apelMessage,$workparameters{SSMOutputDir});
    }
    $sthTZ->finish();
	$dbh->disconnect();
	&printLog( 1, "SSM messages written." );
&Exit($exitStatus);

# ===========================================================================
#                               FUNCTIONS
# ===========================================================================

sub writeMessage
{
	my $messageString = $_[0];
	my $messageDir = $_[1];
	my $messageFileName = time() . "_" . md5_hex($messageString);
	&printLog( 3, "Writing $messageDir/$messageFileName" );;
	open(MESSAGE, "> $messageDir/$messageFileName") || 
			&printLog( 3, "Warning could not write $messageDir/$messageFileName message to disk." );
    print MESSAGE  $messageString;    ## writes message
    close(MESSAGE);
    return 0;
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

# ---------------------------------------------------------------------------
#
# read configuration file and parse parameters:
# arguments: none
#
# ---------------------------------------------------------------------------
sub readConfiguration()
{

	# first read the file:
	my $line = "";
	
	print "Using configuration file: $conffile\n";
	unless ( -e $conffile )
	{
		print "Error: configuration file $conffile not found\n";
		exit(1);
	}

	unless ( -r $conffile )
	{
		print "Error: configuration file $conffile not readable\n";
		exit(1);
	}

	unless ( open( IN, "< $conffile" ) )
	{
		print "Error: cannot open configuration file $conffile\n";
		exit(1);
	}

	my %confparameters;

	# reading the single lines
	while ( $line = <IN> )
	{
		chomp($line);

		$line || next;

		next if ( $line =~ /^\s*\#+/ );

		if ( $line =~ /([^\s]*)\s*=\s*\"(.*)\"/ )
		{

			$confparameters{$1} = $2;

		}

	}

	# close the configuration file
	close(IN);

	# now parse the parameters:

	if ( exists( $confparameters{"systemLogLevel"} )
		 && $confparameters{"systemLogLevel"} ne "" )
	{
		$systemLogLevel = int( $confparameters{"systemLogLevel"} );
	}

	if ( exists( $confparameters{"LOG_FILE"} ) )
	{
		$workparameters{"logFile"} = $confparameters{"LOG_FILE"};
	}
	else
	{
		print "Configuration parameter LOG_FILE missing, using " . $workparameters{logFile};
	}
	&bootstrapLog($workparameters{logFile});	

	if ( exists( $confparameters{"LOCK_FILE"} ) )
	{
		$workparameters{"lockFile"} = $confparameters{"LOCK_FILE"};
	}
	else
	{
		&printLog(
			5,
"Configuration parameter LOCK_FILE missing, using " . $workparameters{"lockFile"}
		);
	}

	if ( exists( $confparameters{"SSM_OUTPUT_DIR"} ) )
	{
		$workparameters{"SSMOutputDir"} = $confparameters{"SSM_OUTPUT_DIR"};
	}
	else
	{
		&printLog(
			5,
"Configuration parameter SSM_OUTPUT_DIR missing, trying " . $workparameters{"SSMOutputDir"}
		);
	}

	# get DGAS HLR db info:

	if ( exists( $confparameters{"HLR_DB_SERVER"} ) )
	{
		$workparameters{"hlrDbServer"} = $confparameters{"HLR_DB_SERVER"};
	}
	else
	{
		&printLog(
			5,
"Configuration parameter HLR_DB_SERVER missing, trying ". $workparameters{hlrDbServer}
		);
	}

	if ( exists( $confparameters{"HLR_DB_PORT"} ) )
	{
		$workparameters{"hlrDbPort"} = $confparameters{"HLR_DB_PORT"};
	}
	else
	{
		&printLog(5, "Configuration parameter HLR_DB_PORT missing, trying " .
			  $confparameters{"hlrDbPort"} );
	}

	if ( exists( $confparameters{"HLR_DB_USER"} ) )
	{
		$workparameters{"hlrDbUser"} = $confparameters{"HLR_DB_USER"};
	}
	else
	{
		&printLog(5, " Configuration parameter HLR_DB_USER missing,
			trying " . $workparameters{"hlrDbUser"} );
	}
	if ( exists( $confparameters{"HLR_DB_PASSWORD"} ) )
	{
		$workparameters{"hlrDbPassword"} = $confparameters{"HLR_DB_PASSWORD"};
	}
	else
	{
		&printLog(5, " Configuration parameter HLR_DB_PASSWORD missing,
			trying" . $workparameters{hlrDbPassword} );
	}

	if ( exists( $confparameters{"HLR_DB_NAME"} ) )
	{
		$workparameters{"hlrDbName"} = $confparameters{"HLR_DB_NAME"};
	}
	else
	{
		&printLog(5, " Configuration parameter HLR_DB_NAME missing,
			trying " . $workparameters{hlrDbName} );
	}
	
	if ( exists( $confparameters{"HLR_DB_TABLE"} ) )
	{
		$workparameters{"hlrDbTable"} = $confparameters{"HLR_DB_TABLE"};
	}
	else
	{
		&printLog(5, " Configuration parameter HLR_DB_TABLE missing,
			trying ". $workparameters{hlrDbTable} );
	}

	# get list of accepted VOs if specified:
	if ( exists( $confparameters{"VO_LIST_LOCATION"} ) )
	{
		$workparameters{"voListFile"} = $confparameters{"VO_LIST_LOCATION"};

		my $vListStr = "";

		# try to read the VO list from the location:
		if ( $workparameters{"voListFile"} =~ /^http:\/\// )
		{
			$vListStr =
			  `wget -q --output-document=- $workparameters{"voListFile"}`;
		}
		else
		{
			$vListStr = `cat $workparameters{"voListFile"}`;
		}

		if ( $vListStr ne "" )
		{
			my @voTokens = split( /\n/, $vListStr );
			my $counter;
			my $voListLog = " Producing usage record messages for VOs listed in '$workparameters{voListFile}': ";

			for ( $counter = 0 ; $counter < scalar(@voTokens) ; $counter++ )
			{
				if ( $voTokens[$counter] =~ /^([\._\-a-zA-Z0-9]+)\s*.*$/ )
				{
					
						push( @voList, $1 );
				}
			}
			$voListLog .= "Number of VOs loaded: " . scalar(@voList);

			&printLog( 5, $voListLog );
		}
		else
		{
			&printLog(
				1,
" ERROR: Configuration parameter VO_LIST_LOCATION pointing to an empty
			  location, Exiting!"
			);
			Exit(1);
		}
	}

	# get list of accepted sites if specified:
	if ( exists( $confparameters{"SITE_LIST_LOCATION"} ) )
	{
		$workparameters{"siteListFile"} = $confparameters{"SITE_LIST_LOCATION"};

		my $sListStr = "";

		# try to read the site list from the location:
		if ( $workparameters{"siteListFile"} =~ /^http:\/\// )
		{
			$sListStr =
			  `wget -q --output-document=- $workparameters{"siteListFile"}`;
		}
		else
		{
			$sListStr = `cat $workparameters{"siteListFile"}`;
		}

		if ( $sListStr ne "" )
		{
			my @siteTokens = split( /\n/, $sListStr );
			my $counter;
			my $siteListLog = " Aggregating usage records for sites listed in '$workparameters{siteListFile}': ";

			for ( $counter = 0 ; $counter < scalar(@siteTokens) ; $counter++ )
			{
				if (
					$siteTokens[$counter] !~ /^\#/    # not a commented line!
					&& $siteTokens[$counter] =~ /^([^\s]*)\s*.*$/
				  )
				{
					my $thisSiteString = $1;

					$siteListLog .= "" . $thisSiteString . ", ";
					push( @siteList, $thisSiteString );
				}
			}
			$siteListLog .= " Number of sites loaded: " . scalar(@siteList);

			&printLog( 5, $siteListLog );
		}
		else
		{
			&printLog(
				1, " Error: Configuration parameter SITE_LIST_LOCATION pointing to an empty location, exiting."
			);
			Exit(1);
		}
	}

	# process also local jobs (voOrigin=map)?
	if ( exists( $confparameters{"PROCESS_LOCAL_JOBS"} ) )
	{
		if ( $confparameters{"PROCESS_LOCAL_JOBS"} =~ /^yes$/i )
		{
			$workparameters{"processLocalJobs"} = 1;
		}
		elsif ( !( $confparameters{"PROCESS_LOCAL_JOBS"} =~ /^no$/i ) )
		{
			&printLog(
				1,
"ERROR: Configuration parameter PROCESS_LOCAL_JOBS has to be either \" yes \" or \"no\", quitting!"
			);
			Exit(1);
		}
	}

	# process only jobs from a certain date (EndTime) or later?
	if ( exists( $confparameters{"PROCESS_FROM_JOB_ENDTIME"} )
		 && $confparameters{"PROCESS_FROM_JOB_ENDTIME"} ne "" )
	{

		&printLog(
				  5,
				  "Reading configuration parameter PROCESS_FROM_JOB_ENDTIME: \""
					. $confparameters{"PROCESS_FROM_JOB_ENDTIME"} . "\""
		);
		$workparameters{"endTimeStart"} =
		  $confparameters{"PROCESS_FROM_JOB_ENDTIME"};

		&printLog(
			5, "processing jobs with execution _end_ timestamp >= $workparameters{endTimeStart}"
		);
	}

	# process only jobs before a certain date (EndTime)?
	if ( exists( $confparameters{"PROCESS_BEFORE_JOB_ENDTIME"} )
		 && $confparameters{"PROCESS_BEFORE_JOB_ENDTIME"} ne "" )
	{

		&printLog(
				5,
				"Reading configuration parameter PROCESS_BEFORE_JOB_ENDTIME: \""
				  . $confparameters{"PROCESS_BEFORE_JOB_ENDTIME"} . "\""
		);
		$workparameters{"endTimeStop"} = $confparameters{"PROCESS_BEFORE_JOB_ENDTIME"};
		
			&printLog(
				5,"Processing only jobs with execution _end_ timestamp < $workparameters{endTimeStop}"
			);
		
	}
	
	if ( exists( $confparameters{"COMMAND_TIMEOUT"} )
		 && $confparameters{"COMMAND_TIMEOUT"} ne "" )
	{

		&printLog(
				5,
				"Reading configuration parameter COMMAND_TIMEOUT: \""
				  . $confparameters{"COMMAND_TIMEOUT"} . "\""
		);
		$workparameters{"commandTimeOut"} = $confparameters{"COMMAND_TIMEOUT"};
		
			&printLog(
				5,"Command execution time out set to $workparameters{commandTimeOut} seconds"
			);
		
	}
	
	if ( exists( $confparameters{"MAX_RECORDS_PER_MESSAGE"} )
		 && $confparameters{"MAX_RECORDS_PER_MESSAGE"} ne "" )
	{

		&printLog(
				5,
				"Reading configuration parameter MAX_RECORDS_PER_MESSAGE: \""
				  . $confparameters{"MAX_RECORDS_PER_MESSAGE"} . "\""
		);
		$workparameters{"maxRecordsPerMessage"} = $confparameters{"MAX_RECORDS_PER_MESSAGE"};
		
			&printLog(
				5,"Max number of records per message set to:$workparameters{maxRecordsPerMessage}"
			);
		
	}
	
	if ( exists( $confparameters{"SENDER_TIMEZONE"} )
		 && $confparameters{"SENDER_TIMEZONE"} ne "" )
	{

		&printLog(
				5,
				"Reading configuration parameter SENDER_TIMEZONE: \""
				  . $confparameters{"SENDER_TIMEZONE"} . "\""
		);
		$workparameters{"senderTimeZone"} = $confparameters{"SENDER_TIMEZONE"};
		
			&printLog(
				5,"sender time zone set to:$workparameters{senderTimeZone}"
			);
		
	}
	
	if ( exists( $confparameters{"RECEIVER_TIMEZONE"} )
		 && $confparameters{"RECEIVER_TIMEZONE"} ne "" )
	{

		&printLog(
				5,
				"Reading configuration parameter RECEIVER_TIMEZONE: \""
				  . $confparameters{"RECEIVER_TIMEZONE"} . "\""
		);
		$workparameters{"receiverTimeZone"} = $confparameters{"RECEIVER_TIMEZONE"};
		
			&printLog(
				5,"receiver time zone set to:$workparameters{receiverTimeZone}"
			);
		
	}

}    # readConfiguration() ...


# ---------------------------------------------------------------------------
#
# Print usage information (help).
# arguments: none
#
# ---------------------------------------------------------------------------
sub printUsageInfo()
{
	print
"Authors: Andrea Guarise (guarise\@to.infn.it)\n\n";
	print "Usage:\n";
	print "$prgname [OPTIONS]\n";
	print "\nWhere OPTIONS are:\n";
	print "-c <file>    Specifies the configuration file.\n";
	print "             Default: \$GLITE_LOCATION/etc/$conffile\n";
	print
"-v           Verbose log file: additional information of processing of the\n";
	print
"             single SumCPU records will be printed to the standard output.\n";
	exit(1);

}    # printUsageInfo() ...

#----------------------------------------------------------------------------
#
# Lock Functions
#
#----------------------------------------------------------------------------

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
}

sub sigINT_handler {
        &printLog (3, "got SIGINT");
        $keepGoing = 0;
}


sub Exit ()
{
	&printLog ( 8 , "Exiting..." );
	if ( &delLock($workparameters{lockFile}) != 0 )
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

