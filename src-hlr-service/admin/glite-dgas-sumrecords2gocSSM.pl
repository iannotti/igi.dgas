#!/usr/bin/perl -w -I /usr/lib/perl5/vendor_perl/5.8.5 -I /usr/lib/perl5/vendor_perl/5.8.0

use strict;
use POSIX;
use Getopt::Std;
use DBI();
use Time::Local;
use strict;

# no buffering of output:
select STDERR;
$| = 1;
select STDOUT;
$| = 1;
my $keepGoing = 1;
my $sqliteDbH;
my $systemLogLevel = 7;
my $prgname        = "glite-dgas-jts2goc.pl";
my $voSinglePassage = 1;
my $voString;
my $logType = 0;
my $LOGH;
my $lastLog = "";
my $logCounter = 0;

my $sigset = POSIX::SigSet ->new();
my $actionHUP=POSIX::SigAction->new("sigHUP_handler",$sigset,&POSIX::SA_NODEFER);
my $actionInt=POSIX::SigAction->new("sigINT_handler",$sigset,&POSIX::SA_NODEFER);
POSIX::sigaction(&POSIX::SIGHUP, $actionHUP);
POSIX::sigaction(&POSIX::SIGINT, $actionInt);
POSIX::sigaction(&POSIX::SIGTERM, $actionInt);

if ( !exists( $ENV{GLITE_LOCATION} ) || $ENV{GLITE_LOCATION} eq "" )
{
	$ENV{GLITE_LOCATION} = "/opt/glite/";
	print "Environment variable GLITE_LOCATION not defined! Trying /opt/glite/ ...\n";
}

my $conffile = "$ENV{GLITE_LOCATION}/etc/glite-dgas-jts2goc.conf";

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
$workparameters{"sqliteDB"} = "$ENV{GLITE_LOCATION}/var/dgas2gocSSM.sqlite";
$workparameters{"processLocalJobs"} =
  0;               #DEFAULT: only jobs with voOrigin=[fqan,pool]
$workparameters{"SSMOutputDir"}         = "$ENV{GLITE_LOCATION}/var/messages/";
$workparameters{"maxRecordsPerMessage"} = 1000;
$workparameters{"voListFile"}           = "$ENV{GLITE_LOCATION}/etc/voList.txt";
$workparameters{"siteListFile"} = "$ENV{GLITE_LOCATION}/etc/siteList.txt";
$workparameters{"minStartDate"} = "";
$workparameters{"maxEndDate"} = "";
$workparameters{"commandTimeOut"} = 600;

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
&sqliteDbPrepare();
my $isNewMonth = &prepareTables();
while ($keepGoing)
{
	foreach my $vo (@voList )
	{
		foreach my $site ( @siteList )
		{
			if ( !$keepGoing ){ &Exit(0);}
			&printLog(4, "Processing vo:$vo;site:$site");
			&getQueryParams($isNewMonth,$site,$vo);
			&printLog(4, "Processing $site from $workparameters{minStartDate} to $workparameters{maxEndDate}");
			my $queryCommand = &composeQueryCommand($site,$vo);
			my @ltime = localtime(time);
			my $year= $ltime[5]+1900;
			my $month = $ltime[4]+1;
			my $tableName = "$site". "_" . "$year"."$month";#siteName_YYYYMM
			&printLog(4, "Writing message info in table: $workparameters{sqliteDB}:$tableName");
			my $id;
			my $querySuccesfull = &tableInsert ($sqliteDbH, $vo,  $site, $tableName, $id);
			&printLog(4, "Inserted id:$id in $workparameters{sqliteDB}:$tableName");
			my $status;
			my $uniqueChecksum;
			my $lastId;
			my $writtenRecords;
			my $lastRecordDate;
			my $query;
			my $res = &execCommand ( $queryCommand, $uniqueChecksum, $status,  $lastId, $lastRecordDate, $writtenRecords, $query );
			if ( $res == 0 )
			{
				&printLog(4,"Correctly executed command: $queryCommand");
				&printLog(4,"Correctly executed query: $query");
				&printLog(4,"Last inserted uniqueChecksum for $vo;$site is:$uniqueChecksum");
				my $dateString = gmtime($lastRecordDate);
				&printLog(4,"Last HLR site:$site id:$lastId date:$dateString records in message:$writtenRecords");
				
				
					$res = &tableUpdate ( $sqliteDbH, $vo,  $site, $tableName,$id, $uniqueChecksum, $lastRecordDate );
					if ($res != 0 )
					{
						&printLog(3,"Error inserting status 0 for id:$id, uniqueChecksum:$uniqueChecksum in table: $workparameters{sqliteDB}:$tableName");
						#ERROR in UPDATE
					}
				
				
			}
			else
			{
				if ( $res == -1 )
				{
					#last item reached
					&tableDelRow($sqliteDbH, $vo,  $site, $tableName,$id);
				}
				else
				{
					&printLog(3,"Error executing command: $queryCommand");
				}
				#ERROR executing command
			}
		}
	}
	&printLog( 1, "SSM messages written.\n" );
}
&Exit($exitStatus);

# ===========================================================================
#                               FUNCTIONS
# ===========================================================================

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
		print "Configuration parameter LOG_FILE missing, using '$workparameters{logFile}'\n";
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
"Configuration parameter LOCK_FILE missing, using '$workparameters{lockFile}'"
		);
	}

	if ( exists( $confparameters{"SQLITE_DB_FILE"} ) )
	{
		$workparameters{"sqliteDB"} = $confparameters{"SQLITE_DB_FILE"};
	}
	else
	{
		&printLog(
			5,
"Configuration parameter SQLITE_DB_FILE missing, trying '$workparameters{sqliteDB}'"
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
"Configuration parameter SSM_OUTPUT_DIR missing, trying '$workparameters{SSMOutputDir}'"
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
"Configuration parameter HLR_DB_SERVER missing, trying '$workparameters{hlrDbServer}'"
		);
	}

	if ( exists( $confparameters{"HLR_DB_PORT"} ) )
	{
		$workparameters{"hlrDbPort"} = $confparameters{"HLR_DB_PORT"};
	}
	else
	{
		&printLog(5, "Configuration parameter HLR_DB_PORT missing, trying '
			  $confparameters{hlrDbPort}'" );
	}

	if ( exists( $confparameters{"HLR_DB_USER"} ) )
	{
		$workparameters{"hlrDbUser"} = $confparameters{"HLR_DB_USER"};
	}
	else
	{
		&printLog(5, " Configuration parameter HLR_DB_USER missing,
			trying '$workparameters{hlrDbUser}'" );
	}

	if ( exists( $confparameters{"HLR_DB_PASSWORD"} ) )
	{
		$workparameters{"hlrDbPassword"} = $confparameters{"HLR_DB_PASSWORD"};
	}
	else
	{
		&printLog(5, " Configuration parameter HLR_DB_PASSWORD missing,
			trying '$workparameters{hlrDbPassword}'" );
	}

	if ( exists( $confparameters{"HLR_DB_NAME"} ) )
	{
		$workparameters{"hlrDbName"} = $confparameters{"HLR_DB_NAME"};
	}
	else
	{
		&printLog(5, " Configuration parameter HLR_DB_NAME missing,
			trying '$workparameters{hlrDbName}'" );
	}
	
	if ( exists( $confparameters{"HLR_DB_TABLE"} ) )
	{
		$workparameters{"hlrDbTable"} = $confparameters{"HLR_DB_TABLE"};
	}
	else
	{
		&printLog(5, " Configuration parameter HLR_DB_TABLE missing,
			trying '$workparameters{hlrDbTable}'" );
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
			my $voListLog = " Producing usage record messages
			  for VOs listed in '$workparameters{voListFile}': ";

			for ( $counter = 0 ; $counter < scalar(@voTokens) ; $counter++ )
			{
				if ( $voTokens[$counter] =~ /^([\._\-a-zA-Z0-9]+)\s*.*$/ )
				{
					if ( $voSinglePassage )
					{
						$voString .= $1 . ",";
					}
					else
					{
						push( @voList, $1 );
					}
					$voListLog .= "" . $1 . ", ";
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
		if ( $voSinglePassage )
		{
			push (@voList, $voString);
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
			my $siteListLog = " Aggregating usage records
			  for sites listed in '$workparameters{siteListFile}': ";

			for ( $counter = 0 ; $counter < scalar(@siteTokens) ; $counter++ )
			{
				if (
					$siteTokens[$counter] !~ /^\#/    # not a commented line!
					&& $siteTokens[$counter] =~ /^([^\s]*)\s*.*$/
				  )
				{
					my $thisSiteString = $1;

					$siteListLog .= "" . $thisSiteString . ", ";

					# not passed to MySQL anymore
					# # replace configuration wildcards for MySQL regexp
					#$thisSiteString =~ s/\*/%/g;
					push( @siteList, $thisSiteString );
				}
			}
			$siteListLog .= " ... Number of sites loaded
			: " . scalar(@siteList);

			&printLog( 5, $siteListLog );
		}
		else
		{
			&printLog(
				1, " Error
			: Configuration parameter SITE_LIST_LOCATION pointing to an empty
			  location, exiting."
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
				5,"CMax number of records per message set to:$workparameters{maxRecordsPerMessage}"
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

#----------------------------------------------------------------------------
#
# sqliteDbPrepare
#
#----------------------------------------------------------------------------

sub sqliteDbPrepare
{
	$sqliteDbH = DBI->connect( "dbi:SQLite:$workparameters{sqliteDB}" ) || &printLog(1, "Error opening database in $workparameters{sqliteDB}") && &Exit(1); 
	$sqliteDbH->{AutoCommit} = 0;  # enable transactions, if possible
  	$sqliteDbH->{RaiseError} = 1;
	
}

sub prepareTables
{
	my $tableBaseDef = "(id INTEGER PRIMARY KEY, startDate TEXT, endDate TEXT, tableName TEXT, vo TEXT, status TEXT, uniqueChecksum KEY)";
	my $newMonth;
	foreach my $i ( @siteList )
	{	
		my @ltime = localtime(time);
		my $year= $ltime[5]+1900;
		my $month = $ltime[4]+1;
		my $tableName = "$i". "_" . "$year"."$month";
		&printLog(6,"Using table: $tableName");
		my $sth = $sqliteDbH->table_info( '', '',$tableName, '' );
    	my $table = $sth->fetchall_arrayref;
    	$sqliteDbH->commit;
    	if (@$table) 
    	{ 
    		&printLog(7, "Table: $tableName is present");
			$newMonth = 0;
    	}
    	else
    	{
    		my $oldMonth = $month-1;
    		my $oldTableName = "$i". "_" . "$year"."$oldMonth";
    		my $sth = $sqliteDbH->table_info( '', '',$oldTableName, '' );
    		my $oldTable = $sth->fetchall_arrayref;
    		$sqliteDbH->commit;
    		if (@$oldTable) 
    		{ 
    			&printLog(7, "Checking if previous month exists.");
    			&printLog(7, "OLD Table: $tableName is present.");
				$newMonth = 1;
    		}
    		else
    		{
    			$newMonth = 0;
    			&printLog(7, "Table: $oldTableName is not present too. First run probably.");
    		}
    		eval {
    			my $createTableQuery  = "CREATE TABLE IF NOT EXISTS \"" . $tableName . "\" " . $tableBaseDef;
    			my $res = $sqliteDbH->selectall_arrayref(  $createTableQuery );
    		};
    		if ($@)
			{
				&printLog ( 2, "Error creating table:$@" );
			}
			else
			{
				&printLog(8, "Table: $tableName correctly created.");
			}
    	}
	}
	return $newMonth;
}

sub getQueryParams
{
	my $newMonth = $_[0];
	my $site = $_[1];
	my $vo = $_[2];
	my @ltime = localtime(time);
	my $month = $ltime[4]+1;
	my $year = $ltime[5]+1900;
	if ( $newMonth == 1 )
	{
		#First day of new month
		if ( $month == 1 )
		{
			#it's January: Previous month is December of last year.
			&printLog(9,"It's January: Previous month is December of last year.");
			$month = 12;
			$year = $year -1;
		}
		else
		{
			&printLog(9,"It isn't January. Previous month of the same year.");
			#it isn't January. Previous month of the same year.
			$month = $month-1;
		}
	}
	else
	{
		&printLog(9,"no new month. This month, this year. simple.");
		#no new month. This month, this year. simple.
	}
	my $queryStr;
	if ( $voSinglePassage )
	{
		$queryStr = "SELECT * FROM \"" . $site . "_" . $year . $month . "\" ORDER by id DESC LIMIT 1";
	}
	else
	{
		$queryStr = "SELECT * FROM \"" . $site . "_" . $year . $month . "\" WHERE vo = '" . $vo . "' ORDER by id DESC LIMIT 1";
	}
	my $result = $sqliteDbH->selectall_arrayref(  $queryStr);
	$sqliteDbH->commit;
    my $numOfRecords = 0;
    if (@$result) { 
		$numOfRecords = @$result;
	};
	&printLog(8,"Executed query: $queryStr, got num:$numOfRecords results");
	if ( $numOfRecords == 1 )
	{
		&printLog(9,"ENTRY EXISTS");
		#ENTRY EXISTS
		my $entryBuffer = pop (@$result);
		my $status = $$entryBuffer[5];
		my $uniqueChecksum = $$entryBuffer[6];
		if ( ( $uniqueChecksum ne "" ) && ( $status == 0 ) )
		{
			&printLog(9,"ENTRY EXISTS HAS STATUS == 0 AND HAS UNIQUE CHECKSUM");
			#ENTRY EXISTS HAS STATUS == 0 AND HAS UNIQUE CHECKSUM
			$workparameters{"minStartDate"} = $$entryBuffer[1];
			$workparameters{"maxEndDate"} = $workparameters{endTimeStop};
			$workparameters{"uniqueChecksum"} = $$entryBuffer[6];
			
		}
		else
		{
			&printLog(9,"ENTRY EXISTS BUT HAS NOT UNIQUE CHECKSUM OR HAS STATUS != 0");
			#ENTRY EXISTS BUT HAS NOT UNIQUE CHECKSUM OR HAS STATUS != 0
			$workparameters{"minStartDate"} = $$entryBuffer[1];
			$workparameters{"maxEndDate"} = $workparameters{endTimeStop};
			$workparameters{"uniqueChecksum"} = "";
		}
	}
	else
	{
		&printLog(9,"ENTRY DOES NOT EXISTS");
		#ENTRY DOES NOT EXISTS
		$workparameters{"minStartDate"}=$workparameters{endTimeStart};
		$workparameters{"maxEndDate"}=$workparameters{endTimeStop};
		$workparameters{"uniqueChecksum"}= "";
	}
	return;
}

sub composeQueryCommand
{
	my $site = $_[0];
	my $vo = $_[1];
	my $time = time();
	my $queryCmd = "$ENV{GLITE_LOCATION}/sbin/glite-dgas-hlr-JTS2CSV ";
	$queryCmd .= "-H $workparameters{hlrDbServer} ";
	$queryCmd .= "-u $workparameters{hlrDbUser} ";
	$queryCmd .= "-p $workparameters{hlrDbPassword} ";
	$queryCmd .= "-d $workparameters{hlrDbName} ";
	$queryCmd .= "-t $workparameters{hlrDbTable} ";
	$queryCmd .= "-o APELSSM ";
	if ( $voSinglePassage )
	{
		$queryCmd .= "-F $workparameters{SSMOutputDir}/" . $site . "_all_" . $time . " ";
	}
	else
	{
		$queryCmd .= "-F $workparameters{SSMOutputDir}/" . $site . "_" . $vo . "_" . $time . " ";
	}
	$queryCmd .= "-s $workparameters{minStartDate} ";
	$queryCmd .= "-e $workparameters{maxEndDate} ";
	$queryCmd .= "-S \"list:$site\" ";
	$queryCmd .= "-V \"list:$vo\" ";
	$queryCmd .= "-L $workparameters{maxRecordsPerMessage} ";
	if ( $workparameters{uniqueChecksum} ne "" )
	{
		$queryCmd .= "-U $workparameters{uniqueChecksum} ";	
	}
	if ( $workparameters{processLocalJobs} == 0 )
	{
		#process only grid jobs (voOrigin=[fqan,pool])
		$queryCmd .= "-G";
	}
	&printLog(8,"commandLine: $queryCmd");
	return $queryCmd;
}


sub tableInsert
{
	my $sqliteDbH = $_[0];
	my $vo        = $_[1];
	my $site      = $_[2];
	my $tableName = $_[3];
	my $id; #returned in $_[4];
	if ( $voSinglePassage )
	{
		$vo = "all";
	}

	my $queryAStr = "INSERT INTO " . $tableName;
	$queryAStr .=
	  " " . "(id, startDate, endDate, tableName, vo, status, uniqueChecksum ) ";
	$queryAStr .=
"VALUES (NULL, $workparameters{minStartDate}, $workparameters{maxEndDate}, $workparameters{hlrDbTable}, $vo, 1, \'\')"
	  ;    #FOR LOG
	my $queryA = "INSERT INTO \"" . $tableName . "\"";
	$queryA .=
	  " " . "(id, startDate, endDate, tableName, vo,  status, uniqueChecksum ) ";
	$queryA .= "VALUES (NULL, ?, ?, ?, ?, 1, \'\')";
	my $sth = $sqliteDbH->prepare($queryA);
	my $querySuccesfull = 1;
	my $queryCounter    = 0;
	while ( $keepGoing && $querySuccesfull )
	{
		eval {
			my $res = $sth->execute(
										   $workparameters{minStartDate},
										   $workparameters{maxEndDate},
										   $workparameters{hlrDbTable},
										   $vo						
			);
		};
		if ($@)
		{
			&printLog( 3, "WARN: ($queryCounter) $@" );
			print "Retrying in $queryCounter\n";
			for ( my $i = 0 ; $keepGoing && ( $i < $queryCounter ) ; $i++ )
			{
				sleep $i;
			}
			$queryCounter++;
		}
		else
		{
			$querySuccesfull = 0;
			#$id = $sqliteDbH->sqlite_last_insert_rowid();
			$id = $sqliteDbH->last_insert_id("","","","");
			$_[4] = $id;
			&printLog( 9, "Executed query: $queryAStr : inesrted id; $_[4]" );
		}
		last if ( $queryCounter >= 10 );
	}
	return $querySuccesfull;
}

sub tableUpdate
{
	my $sqliteDbH = shift;
	my $vo        = shift;
	my $site      = shift;
	my $tableName = shift;
	my $id = shift;
	my $uniqueChecksum = shift;
	my $lastRecordDate = shift;
	&printLog( 9, "lastRecordDate: ($lastRecordDate)" );
	my @rtime = gmtime($lastRecordDate);
			my $year= $rtime[5]+1900;
			my $month = $rtime[4]+1;
			my $day =  $rtime[3];
	my $recordDateSring = "$year-$month-$day";
	my $queryAStr = "UPDATE " . $tableName;
	my $queryA;
	if ( $lastRecordDate )
	{
		&printLog( 9, "lastRecordDate: OK" );
		$queryAStr .=
	  " " . "SET status = 0, uniqueChecksum = \"$uniqueChecksum\", startDate = \"$recordDateSring\"";
	$queryAStr .= " WHERE id=$id"; #FOR LOG
		$queryA = "UPDATE \"" . $tableName . "\" " . "SET status = 0, uniqueChecksum = \"$uniqueChecksum\", startDate = \"$recordDateSring\"" . " WHERE id=?";
	}
	else
	{
		$queryAStr .=
	  " " . "SET status = 0, uniqueChecksum = \"$uniqueChecksum\"";
	$queryAStr .= " WHERE id=$id"; #FOR LOG
		&printLog( 9, "lastRecordDate: NOT FOUND" );
		$queryA = "UPDATE \"" . $tableName . "\" " . "SET status = 0, uniqueChecksum = \"$uniqueChecksum\"" . " WHERE id=?";
	}
	my $sth = $sqliteDbH->prepare($queryA);
	my $querySuccesfull = 1;
	my $queryCounter    = 0;

	while ( $keepGoing && $querySuccesfull )
	
	{
		eval {
			&printLog( 9, "Executing Query: $queryAStr" );
			my $res = $sth->execute(
										   $id
			);
		};
		if ($@)
		{
			&printLog( 3, "WARN: ($queryCounter) $@" );
			print "Retrying in $queryCounter\n";
			for ( my $i = 0 ; $keepGoing && ( $i < $queryCounter ) ; $i++ )
			{
				sleep $i;
			}
			$queryCounter++;
		}
		else
		{
			$querySuccesfull = 0;
			&printLog( 9, "Executing Query: $queryAStr" );
		}
		last if ( $queryCounter >= 10 );
	}
	return $querySuccesfull;
}

sub tableDelRow
{
	my $sqliteDbH = shift;
	my $vo        = shift;
	my $site      = shift;
	my $tableName = shift;
	my $id = shift;
	my $queryAStr = "DELETE FROM " . $tableName;
	
	$queryAStr .= " WHERE id=$id"; #FOR LOG
	my $queryA = "DELETE FROM \"" . $tableName. "\" WHERE id=?";
	
	my $sth = $sqliteDbH->prepare($queryA);
	my $querySuccesfull = 1;
	my $queryCounter    = 0;

	while ( $keepGoing && $querySuccesfull )
	
	{
		eval {
			&printLog( 9, "Executing Query: $queryAStr" );
			my $res = $sth->execute(
										   $id
			);
		};
		if ($@)
		{
			&printLog( 3, "WARN: ($queryCounter) $@" );
			print "Retrying in $queryCounter\n";
			for ( my $i = 0 ; $keepGoing && ( $i < $queryCounter ) ; $i++ )
			{
				sleep $i;
			}
			$queryCounter++;
		}
		else
		{
			$querySuccesfull = 0;
			&printLog( 9, "Executing Query: $queryAStr" );
		}
		last if ( $queryCounter >= 10 );
	}
	return $querySuccesfull;
}

sub execCommand
{
	my $command = $_[0];
	my $lastChecksum = "";
	my $status = -1;
	my $lastId;
	my $writtenRecords;
	my $lastRecordDate;
	my $query;
	
	eval {
		local $SIG{ALRM} = sub { die "timeout on command alrm\n"; };
		alarm $workparameters{commandTimeOut};
        	open ( COMMAND, "$command 2>/dev/null |") || &printLog(1, "can't fork: $command");
        	while (<COMMAND> )
        	{
        		if ( $_ =~ /^LastChecksum:(.*)$/ ) 
        		{
        			$lastChecksum = $1;
        			$status = 0;
        		}
        		if ( $_ =~ /^LastId:(.*)$/ ) 
        		{
        			$lastId = $1;
        		}
        		if ( $_ =~ /^LastRecordDate:(.*)$/ ) 
        		{
        			$lastRecordDate = $1;
        		}
        		if ( $_ =~ /^WrittenRecords:(.*)$/ ) 
        		{
        			$writtenRecords = $1;
        		}
        		if ( $_ =~ /^Query:(.*)$/ ) 
        		{
        			$query = $1;
        		}
        	}
		alarm 0;
	};
	if ( $@ )
	{
		&printLog ( 4, "Timeout." );
		$status = -2;
		$_[2] = $status;
        return $status;
	}
	&printLog ( 7, "Executid: $command EXIT_STATUS=$status" );
	$_[1] = $lastChecksum;
	$_[3] = $lastId;
	$_[2] = $status;
	$_[4] = $lastRecordDate;
	$_[5] = $writtenRecords;
	$_[6] = $query;
	return $status;
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
