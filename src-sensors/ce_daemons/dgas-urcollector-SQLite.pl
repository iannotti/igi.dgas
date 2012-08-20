#!/usr/bin/perl -w
# glite-urcollector, DGAS job monitor and Usage Records collector.
# written by R.M. Piro (piro@to.infn.it) with code
# A. Guarise (guarise@to.infn.it)
# and G. Patania (patania@to.infn.it).
#
# Condor parser based on information provided by
# Ken Schumacher (kschu@fnal.gov)
# and Philippe Canal (pcanal@fnal.gov), Fermilab, USA
#
# SGE part from P.Veronesi and A.Cristofori
# written by R.M. Piro (piro@to.infn.it),
# A. Guarise (guarise@to.infn.it)
# and G. Patania (patania@to.infn.it), INFN Torino, Italy.
# Condor parser based on information provided by
# Ken Schumacher (kschu@fnal.gov)
# and Philippe Canal (pcanal@fnal.gov), Fermilab, USA
# Much of the code for &parse_line() (including the primary regexp) from Joerk Behrends.

use strict;
use POSIX;
use Sys::Syslog;
use Text::ParseWords;
use Time::Local;
use File::Basename;

#use vars qw($VERSION @ISA @EXPORT $PERL_SINGLE_QUOTE);
use vars qw($PERL_SINGLE_QUOTE);
use DBI;
use Time::HiRes qw(usleep ualarm gettimeofday tv_interval);

# turn off buffering of STDOUT
#$| = 1;

# my $MAX_LSF_RECORDLENGTH_FOR_QUOTEWORDS = 4096;
# this is the maximum LSF record length that we'll parse with
# quotewords, otherwise we risk a segmentation fault. Longer
# records qill be parsed by a costum function (splitCVSstring)

START:

my $sigset    = POSIX::SigSet->new();
my $actionHUP =
  POSIX::SigAction->new( "sigHUP_handler", $sigset, &POSIX::SA_NODEFER );
my $actionInt =
  POSIX::SigAction->new( "sigINT_handler", $sigset, &POSIX::SA_NODEFER );
POSIX::sigaction( &POSIX::SIGHUP,  $actionHUP );
POSIX::sigaction( &POSIX::SIGINT,  $actionInt );
POSIX::sigaction( &POSIX::SIGTERM, $actionInt );

my $dgasLocation = $ENV{DGAS_LOCATION};
if ( !defined($dgasLocation) || $dgasLocation eq "" )
{
	$dgasLocation = $ENV{GLITE_LOCATION};
	if ( $dgasLocation eq "" )
	{
		$dgasLocation = "/usr/";
	}
}

my %processedLogFileInodes = ();
my %logFMod    = ();
my %logFModPreviousRun = ();

my $TSTAMP_ACC =
  86400;    # timestamps between CE log and LRMS log can differ for up to a day.
my $DEF_LDIF_VALIDITY = 86400
  ; # assume the GLUE attributes taken from the LDIF didn't change within the last day.
my $DEF_IGNORE_JOBS_LOGGED_BEFORE = "2009-01-01";
my $DEF_WAITFOR                   = 5;
my $DEF_MAXNUMRECORDS             = 10000;
my $onlyOneIteration              = 0;              # default is run as daemon!
my $useCElog         = 1;  # default is use the CE's map: grid job <-> local job
my $processGridJobs  = 1;  # default, changed through jobsToProcess
my $processLocalJobs = 1;  # default, changed through jobsToProcess

my $configFilePath = "/etc/dgas/dgas_sensors.conf";
my %configValues   = (
	siteName                  => "",
	localHostNameAsCEHostName => "no",
	useCEHostName             => "",
	localUserGroup2VOMap      => "/etc/dgas/dgas_localUserGroup2VOMap.conf",
	havePoolAccounts          => "yes",
	poolAccountPatternFile    => "",
	systemLogLevel            => 7,
	gipDynamicTmpCEFiles      => "ldap://`hostname`:2170",
	useUrKeyDefFile           => "no",
	urKeyDefFile              => "/etc/dgas/dgas_sensors.conf",
	voToProcess               => "",
	transportLayer            => "Legacy",
	recordComposer1      => $dgasLocation . "/libexec/dgas-legacyCpuComposer",
	recordProducer1      => $dgasLocation . "/libexec/dgas-amqProducer",
	recordComposer2      => $dgasLocation . "/libexec/ogfurComposer",
	recordProducer2      => $dgasLocation . "/libexec/dgas-amqProducer",
	lrmsType             => "pbs",
	pbsAcctLogDir        => "/var/spool/pbs/server_priv/accounting",
	lsfAcctLogDir        => "",
	sgeAcctLogDir        => "",
	condorHistoryCommand => "",
	ceJobMapLog          => "/var/log/cream/accounting/blahp.log",
	useCEJobMap          => "yes",
	jobsToProcess        => "all",
	keyList              =>
"GlueHostBenchmarkSF00,GlueHostBenchmarkSI00,GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU,GlueHostApplicationSoftwareRunTimeEnvironment: SF00MeanPerCPU",
	ldifDefaultFiles        => "",
	glueLdifFile            => "ldap://`hostname`:2170",
	collectorLockFileName   => "/var/lock/dgas/dgas_urcollector.lock",
	collectorLogFileName    => "/var/log/dgas/dgas_urcollector.log",
	collectorBufferFileName => "/var/spool/dgas/dgasCollectorBuffer",
	mainPollInterval        => "5",
	collectorPollInterval   => "5",
	timeInterval            => "1",
	jobPerTimeInterval      => "500",
	ignoreJobsLoggedBefore  => $DEF_IGNORE_JOBS_LOGGED_BEFORE,
	maxNumRecords           => $DEF_MAXNUMRECORDS,
	limiterWaitFor          => $DEF_WAITFOR,
	dgasDB                  => "/var/spool/dgas/dgas.sqlite",
);

# get command line arguments (if any):
my $clinearg = "";
while (@ARGV)
{
	$clinearg = shift @ARGV;
	if ( $clinearg eq "--nodaemon" )
	{
		$onlyOneIteration = 1;    # one iteration then quit! (for cronjobs)
	}
	else
	{

		# take it as configuration file name
		$configFilePath = $clinearg;
	}
}

# Parse configuration file
&parseConf($configFilePath);

my @transportLayers = split( /;/, $configValues{transportLayer} );
my $systemLogLevel  = $configValues{systemLogLevel};
my $logType         = 0;
my $LOGH;
my $lastLog    = "";
my $logCounter = 0;
&bootstrapLog( $configValues{collectorLogFileName} );

my $dbh = DBI->connect("dbi:SQLite:$configValues{dgasDB}")
  || die "Cannot connect: $DBI::errstr";
$dbh->{AutoCommit} = 0;    # enable transactions, if possible
$dbh->{RaiseError} = 1;

my $hostName = `hostname -s`;
chomp($hostName);

my %urGridInfo;
my $UR;
my %numCpus;
my %numCpusTS;

my $localUserGroup2VOMap   = $configValues{localUserGroup2VOMap};
my $poolAccountPatternFile = $configValues{poolAccountPatternFile};
my $transportLayer         = $configValues{transportLayer};

# make sure we don't use the local hostname for the CE if another one is
# specified!
if ( exists( $configValues{useCEHostName} )
	&& $configValues{useCEHostName} ne "" )
{

	$configValues{localHostNameAsCEHostName} = "no";
}

#This is needed by 'amq' transport layer.
my $ceCertificateSubject =
  `openssl x509 -subject -in /etc/grid-security/hostcert.pem -noout`;
if ( $ceCertificateSubject =~ /subject=\s(.*)/ )
{
	$ceCertificateSubject = $1;
}

my $havePoolAccounts = 1;    # default is "yes"!

if ( $configValues{havePoolAccounts} =~ /^no$/i )
{
	$havePoolAccounts = 0;
	&printLog( 5,
		"NOT considering pool accounts determining the VOs of out-of-band jobs."
	);
}
else
{
	&printLog( 3,
		"Considering pool accounts determining the VOs of out-of-band jobs." );
}

my $glueStoredTS = 0;

my $voToProcess = $configValues{voToProcess};
my $keepGoing   = 1;
my $start       = time();                       ##  time init

my $sqlStatement =
"CREATE TABLE IF NOT EXISTS commands (key INTEGER PRIMARY KEY, transport TEXT KEY, composer TEXT, arguments TEXT, producer TEXT, recordDate TEXT, lrmsId TEXT, commandStatus INT)";
my $sqliteCmd = "/usr/bin/sqlite3 $configValues{dgasDB} \"$sqlStatement\"";
my $status    = system("$sqliteCmd");

my @ATMDefinitions;
if ( $configValues{useUrKeyDefFile} eq "yes" )
{
	&printLog( 5, "Using record definitions from:$configValues{urKeyDefFile}" );
	&populateATMDef( $configValues{urKeyDefFile}, \@ATMDefinitions );
}

my $lrmsType                = $configValues{lrmsType};
my $pbsAcctLogDir           = $configValues{pbsAcctLogDir};
my $lsfAcctLogDir           = $configValues{lsfAcctLogDir};
my $sgeAcctLogDir           = $configValues{sgeAcctLogDir};
my $condorHistoryCommand    = $configValues{condorHistoryCommand};
my $ceJobMapLog             = $configValues{ceJobMapLog};
my $useCEJobMap             = $configValues{useCEJobMap};
my $jobsToProcess           = $configValues{jobsToProcess};
my $keyList                 = $configValues{keyList};
my $ldifDefaultFiles        = $configValues{ldifDefaultFiles};
my $glueLdifFile            = $configValues{glueLdifFile};
my $collectorLockFileName   = $configValues{collectorLockFileName};
my $collectorBufferFileName = $configValues{collectorBufferFileName};
my $mainPollInterval        = $configValues{mainPollInterval};
my $collectorPollInterval   = $configValues{collectorPollInterval};
my $timeInterval            = $configValues{timeInterval};
my $jobPerTimeInterval      = $configValues{jobPerTimeInterval};
my $maxNumRecords           = $configValues{maxNumRecords};
my $waitFor                 = $configValues{limiterWaitFor};
my $siteName                = $configValues{siteName};
if ( $siteName eq "" )
{
	$siteName = `/bin/hostname -d`;
	chomp($siteName);
}
&printLog( 4, "Publishing records for site::$siteName" );

# put lock
if ( &putLock($collectorLockFileName) != 0 )
{
	&errorNoRemoveLock(
"Fatal Error: Couldn't open lock file! in $collectorLockFileName\nExiting ..."
	);
}
else
{
	&printLog( 4, "Daemon started. Lock file succesfully created." );
}

if ( $jobsToProcess =~ /^grid$/i )
{
	$processGridJobs  = 1;
	$processLocalJobs = 0;    # only grid jobs
	&printLog( 4,
		"Processing only _grid_ jobs (with entries in ceJobMapLog)!" );
}
elsif ( $jobsToProcess =~ /^local$/i )
{
	$processGridJobs  = 0;    # only local jobs
	$processLocalJobs = 1;
	&printLog( 4,
		"Processing only _local_ jobs (without entries in ceJobMapLog)!" );
}
elsif ( $jobsToProcess =~ /^all$/i )
{
	$processGridJobs  = 1;    # default
	$processLocalJobs = 1;    # default
	&printLog( 4, "Processing _all_ jobs (grid and local)!" );
}
else
{
	&printLog( 3,
"Warning: In conf: wrong value for 'jobsToProcess = \"$jobsToProcess\"', possible values are: \"grid\", \"local\" and \"all\". Using default: \"all\"!"
	);
	$processGridJobs  = 1;    # default
	$processLocalJobs = 1;    # default
	&printLog( 4, "Processing _all_ jobs (grid and local)!" );
}

# ignore old jobs? default is: ignore before $DEF_IGNORE_JOBS_LOGGED_BEFORE
if ( $configValues{ignoreJobsLoggedBefore} !~
	/^(\d{4})[-\/](\d{2})[-\/](\d{2})$/ )
{
	&printLog( 3,
"Attention: Cannot interpret value specified for ignoreJobsLoggedBefore in configuration file: \"$configValues{ignoreJobsLoggedBefore}\" ... using default!"
	);
	$configValues{ignoreJobsLoggedBefore} = $DEF_IGNORE_JOBS_LOGGED_BEFORE;
}

my $ignoreJobsLoggedBefore =
  &date2Timestamp( $configValues{ignoreJobsLoggedBefore} );

&printLog( 4,
"Ignoring jobs logged before $configValues{ignoreJobsLoggedBefore} 0:00 am (in UTC => $ignoreJobsLoggedBefore)"
);

# check wether to use the CE log and get name of directory:
my $ceJobMapLogDir = "";

if ( $useCEJobMap ne "yes" && $useCEJobMap ne "YES" )
{
	$useCElog = 0;    # don't use it, treat all jobs as local
	&printLog( 3,
"Warning: Not using the CE's job map log file for retrieving grid-related information. All jobs treated as local jobs!"
	);

	# must be set if processing _only_ grid jobs or _only_ local jobs
	if ( !$processGridJobs || !$processLocalJobs )
	{
		&error(
"Error in configuration file: 'jobsToProcess = \"$jobsToProcess\"' requires useCEJobMap and ceJobMapLog to be set!"
		);
	}

}
else
{
	if ( -d $ceJobMapLog )
	{

		# specified in conf file: directory, not a file
		$ceJobMapLogDir = $ceJobMapLog;
	}
	else
	{

		# if $ceJobMapLog is a file, not a directory, get the directory:
		$ceJobMapLogDir = dirname($ceJobMapLog) . "/";
	}

	# check whether what was specified exists:
	if ( !-d $ceJobMapLogDir )
	{
		&error(
"Fatal error: directory with ceJobMapLog doesn't exist: '$ceJobMapLog'"
		);
	}
}

my $timeZone = `date +%z`;
chomp($timeZone);
my $domainName = `hostname -d`;
chomp($domainName);

# This is for parsing LDIF files:
my %glueAttributes   = ();
my $ldifModTimestamp = 0;

my $jobsThisStep = 0;    # used to process only bunchs of jobs, as specified in
                         # the configuration file

# determine the LRMS type we have to treat:
my $lrmsLogDir = "";
if ( $lrmsType eq "pbs" )
{
	$lrmsLogDir = $pbsAcctLogDir;
}
elsif ( $lrmsType eq "lsf" )
{
	$lrmsLogDir = $lsfAcctLogDir;
}
elsif ( $lrmsType eq "condor" )
{

	# nothing to do
}
elsif ( $lrmsType eq "sge" )
{
	$lrmsLogDir = $sgeAcctLogDir;
}
elsif ( $lrmsType eq "" )
{
	&error("Error: LRMS type not specified in configuration file!");
}
else
{
	&error("Error: Unknown LRMS type specified in configuration file!");
}

if ( $lrmsType ne "condor" )
{

	# check LRMS log directory:
	if ( !-d $lrmsLogDir || !-r $lrmsLogDir || !-x $lrmsLogDir )
	{
		&error(
			"Error: Directory for LRMS log ($lrmsLogDir) cannot be accessed!");
	}
}
else
{    # Condor
	    # try to find executable:
	if ( $condorHistoryCommand eq "" )
	{
		&printLog( 4,
"Condor history command not specified in configuration file, trying to find \$CONDOR_LOCATION/bin/condor_history:"
		);

		# not specified in conf file, try default:
		$condorHistoryCommand = "$ENV{CONDOR_LOCATION}/bin/condor_history";
		if ( !-e $condorHistoryCommand )
		{
			&printLog( 4,
				print
"$condorHistoryCommand not found, trying 'which condor_history'."
			);
			$condorHistoryCommand = `which condor_history`;
		}
	}
	if ( !-e $condorHistoryCommand || !-x $condorHistoryCommand )
	{
		&error(
"Error: Cannot find executable for retrieving condor history: '$condorHistoryCommand'"
		);
	}
}

# append LRMS type to buffer file name:
$collectorBufferFileName = $collectorBufferFileName . "." . $lrmsType;

# check whether we can use the 'less' command (should be present on each
# Linux system ...) this is usefull for gzipped log files!
my $have_less = 1;
my $less_cmd  = `which less`;
chomp($less_cmd);
if ( !-x $less_cmd )
{
	$have_less = 0;
}

# this is used for parsing log file forwards ...
my $have_cat = 1;
my $cat_cmd  = `which cat`;
chomp($cat_cmd);
if ( !-x $cat_cmd )
{
	$have_cat = 0;
}

if ( !$have_cat )
{
	&error("Error: commands 'cat' required for parsing log files! Quitting!");
}

if ( !$have_less )
{
	&printLog( 3,
"Warning: command 'less' required for parsing compressed log files! Compressed log files will be skipped!"
	);
}

# first get info on last job processed:
my $startJob       = "";
my $startTimestamp = 0;
my $lastJob        = "";
my $lastTimestamp  = 0;

my $mainRecordsCounter = 0;
my $firstRun           = 0;    #assume this isn't the first run.
MAIN: while ($keepGoing)
{

	# first get info on last job processed:
	if ( &readBuffer( $collectorBufferFileName, $startJob, $startTimestamp ) !=
		0 )
	{

		# we have no regular buffer containing the start Job
		# this is the very first run, not even a temporary buffer is written:
		$firstRun = 1;
	}

	if ( &numRecords() > $maxNumRecords )
	{
		&printLog( 3,
"There are more than $maxNumRecords records in database, waiting $waitFor seconds."
		);
		my $secsWaited = 0;
		while ( $keepGoing && $secsWaited < $waitFor )
		{
			my $randomSleepTime =
			  0.25 - log( rand() );    #0.25 + v.a. exp unilatera, media = 1
			select( undef, undef, undef, $randomSleepTime )
			  ;                        #equivale a sleep per valore float
			$secsWaited++;
		}
		next MAIN;
	}

	# see whether the GLUE attributes are available and have changed
	if ( $glueLdifFile =~ /^ldap:\/\/(.*):(\d*)$/ )
	{
		my $currentTS = time();
		if ( $currentTS - $glueStoredTS > 7200 )    #every two hours
		{
			$glueStoredTS = &getGLUEAttributesFromLdap( $1, $2 );
			$ldifModTimestamp = $glueStoredTS;
		}
	}
	else
	{
		$ldifModTimestamp = &getGLUEAttributesFromLdif();
	}

	# this is the main processing part:
	if ( $lrmsType ne "condor" )
	{

		# process LRMS log
		&processLrmsLogs( $startJob, $startTimestamp, $lastJob, $lastTimestamp,
			$ignoreJobsLoggedBefore );
	}
	else
	{

		# process Condor history starting from startJob
		&processCondorJobHistory( $startJob, $startTimestamp, $lastJob,
			$lastTimestamp, $ignoreJobsLoggedBefore );
	}

	# write buffer (lastJob and lastTimestamp), if necessary!

	if ( $keepGoing && ( $lastJob ne "" ) && ( $lastJob ne $startJob ) )
	{

		#commit transaction
		&printLog( 6, "Commit..." );
		my $commitSuccesfull = 1;
		while ( $commitSuccesfull == 1 )
		{
			eval { $dbh->commit; };
			if ($@)
			{
				&printLog( 3, "DB Locked:$@" );
				my $randomSleepTime =
				  0.25 - log( rand() );    #0.25 + v.a. exp unilatera, media = 1
				select( undef, undef, undef, $randomSleepTime )
				  ;                        #equivale a sleep per valore float
			}
			else
			{
				&printLog( 6, "...succesfull" );
				$commitSuccesfull = 0;
			}
		}
		&printLog( 5,
"Processed from $startJob (timestamp $startTimestamp) to $lastJob (timestamp $lastTimestamp). Updating buffer $collectorBufferFileName."
		);

		&putBuffer( $collectorBufferFileName, $lastJob, $lastTimestamp );
		$startJob       = $lastJob;
		$startTimestamp = $lastTimestamp;
	}

	if ($onlyOneIteration)
	{

		# not run as a daemon, stop after first round!
		print ""
		  . localtime()
		  . ": Not run as a daemon. Single iteration completed!";
		$keepGoing = 0;
	}

	if ($keepGoing)
	{

# print "".localtime().": Waiting for new jobs to finish. Sleeping for $mainPollInterval seconds.";
		my $secsWaited = 0;
		while ( $keepGoing && $secsWaited < $mainPollInterval )
		{
			usleep(100000);
			$secsWaited++;
		}
		#additional sleep for fine tuning of resource consumption use collectorPollInterval to set this (this is far to be optimal FIXME)
	    $secsWaited = 0;
		while ( $keepGoing && $secsWaited < $collectorPollInterval )
		{
			usleep(100000);
			$secsWaited++;
		}
	}

}

&printLog( 7, "Exiting..." );
if ( &delLock($collectorLockFileName) != 0 )
{
	&printLog( 2, "Error removing lock file." );
}
else
{
	&printLog( 7, "Lock file removed." );
}
&printLog( 4, "Commit." );
$dbh->commit;
&printLog( 4, "Exit." );

exit(0);

sub parse_line
{
	my ( $delimiter, $keep, $line ) = @_;
	my ( $word, @pieces );

	no warnings 'uninitialized';    # we will be testing undef strings

	while ( length($line) )
	{

	# This pattern is optimised to be stack conservative on older perls.
	# Do not refactor without being careful and testing it on very long strings.
	# See Perl bug #42980 for an example of a stack busting input.
		$line =~ s/^
                    (?: 
                        # double quoted string
                        (")                             # $quote
                        ((?>[^\\"]*(?:\\.[^\\"]*)*))"   # $quoted 
                    |   # --OR--
                        # singe quoted string
                        (')                             # $quote
                        ((?>[^\\']*(?:\\.[^\\']*)*))'   # $quoted
                    |   # --OR--
                        # unquoted string
                        (                               # $unquoted 
                            (?:\\.|[^\\"'])*?           
                        )               
                        # followed by
                        (                               # $delim
                            \Z(?!\n)                    # EOL
                        |   # --OR--
                            (?-x:$delimiter)            # delimiter
                        |   # --OR--                    
                            (?!^)(?=["'])               # a quote
                        )  
                    )//xs or return;    # extended layout
		my ( $quote, $quoted, $unquoted, $delim ) =
		  ( ( $1 ? ( $1, $2 ) : ( $3, $4 ) ), $5, $6 );

		return ()
		  unless ( defined($quote) || length($unquoted) || length($delim) );

		if ($keep)
		{
			$quoted = "$quote$quoted$quote";
		}
		else
		{
			$unquoted =~ s/\\(.)/$1/sg;
			if ( defined $quote )
			{
				$quoted =~ s/\\(.)/$1/sg if ( $quote eq '"' );
				$quoted =~ s/\\([\\'])/$1/g
				  if ( $PERL_SINGLE_QUOTE && $quote eq "'" );
			}
		}
		$word .= substr( $line, 0, 0 );    # leave results tainted
		$word .= defined $quote ? $quoted : $unquoted;

		if ( length($delim) )
		{
			push( @pieces, $word );
			push( @pieces, $delim ) if ( $keep eq 'delimiters' );
			undef $word;
		}
		if ( !length($line) )
		{
			push( @pieces, $word );
		}
	}
	return (@pieces);
}

sub quotewords
{
	my ( $delim, $keep, @lines ) = @_;
	my ( $line, @words, @allwords );

	foreach $line (@lines)
	{
		@words = parse_line( $delim, $keep, $line );
		return () unless ( @words || !length($line) );
		push( @allwords, @words );
	}
	return (@allwords);
}

sub Exit ()
{
	&printLog( 8, "Exiting..." );
	if ( &delLock($collectorLockFileName) != 0 )
	{
		&printLog( 2, "Error removing lock file." );
	}
	else
	{
		&printLog( 5, "Lock file removed." );
	}
	&printLog( 4, "Commit" );
	&dbh->commit;
	&printLog( 4, "Exit." );
	exit(0);
}

sub bootstrapLog
{
	my $logFilePath = $_[0];
	if ( $logFilePath =~ /SYSLOG(\d)/ )
	{
		my $facility = "LOG_LOCAL" . $1;
		$logType = 1;
		openlog( "DGAS", 'ndelay', $facility );
	}
	else
	{
		open( LOGH, ">>$logFilePath" );
	}
	return 0;
}

sub printLog
{
	my $logLevel = $_[0];
	my $log      = $_[1];
	if ( $logLevel <= $systemLogLevel )
	{
		if ( $logType == 1 )
		{
			my $pri = "";
		  SWITCH:
			{
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
		else
		{
			my $localtime = localtime();
			if ( $log ne $lastLog )
			{
				if ( $logCounter != 0 )
				{
					print LOGH
					  "$localtime: Last message repeated $logCounter times.\n";
				}
				$logCounter = 0;
				print LOGH "$localtime: " . $log . "\n";
			}
			else
			{
				$logCounter++;
				if ( $logCounter == 20 )
				{
					print LOGH "$localtime: Last message repeated 20 times.\n";
					$logCounter = 0;
				}
			}
			$lastLog = $log;
		}

	}
}

sub parseRecord    #WAS parseFile
{
	my $recordString = $_[0];

	# empty these from the previous processed record:
	$UR         = "";
	%urGridInfo = ();
	&printLog( 8, "Trying to parse Record." );
	my @lines = split( /\n/, $recordString );
	foreach my $line (@lines)
	{
		if ( $line =~ /^ACCTLOG:(.*)$/ )
		{
			$UR = $1;
			last;    # stop here
		}
		if ( $line =~ /^([^=]*)=(.*)$/ )
		{
			$urGridInfo{$1} = $2;
		}
	}

	if ( $UR eq "" )
	{
		&printLog( 2, "Error: UR has wrong format: skipping!" );
		return 2;
	}

	# try to determine the LRMS type.
	my $lrmsType = "";
	if ( exists( $urGridInfo{"LRMS_TYPE"} )
		&& ( $urGridInfo{"LRMS_TYPE"} ne "" ) )
	{

		# give priority to what is stated by Gianduia
		$lrmsType = $urGridInfo{"LRMS_TYPE"};
	}
	else
	{

		#otherwise use the same lrmsType specified in the configuration file
		$lrmsType = $configValues{lrmsType};
	}

	# verify that we can treat this LRMS type!
	if (   $lrmsType ne "pbs"
		&& $lrmsType ne "lsf"
		&& $lrmsType ne "sge"
		&& $lrmsType ne "condor" )
	{
		&printLog( 2, "Error: cannot determine LRMS type for UR skipping!" );
		return 3;
	}
	else
	{
		$urGridInfo{lrmsType} = $lrmsType;
		&printLog( 9, "LRMS type: $lrmsType" );
	}
	$urGridInfo{siteName} = $siteName;
	&parseUR( $lrmsType, $UR );

	return 0;
}

sub parseUR
{
	if ( $_[0] eq "pbs" )
	{
		return &parseUR_pbs( $_[1] );
	}
	elsif ( $_[0] eq "lsf" )
	{
		return &parseUR_lsf( $_[1] );
	}
	elsif ( $_[0] eq "condor" )
	{
		return &parseUR_condor( $_[1] );
	}
	elsif ( $_[0] eq "sge" )
	{
		return &parseUR_sge( $_[1] );
	}
}

sub parseUR_pbs
{
	my $URString = $_[0];
	&printLog( 8, "UR string:\n$URString" );

	my @URArray  = split( ' ', $URString );
	my @tmpArray = split( ';', $URArray[1] );
	$_ = $tmpArray[3];
	if (/^user=(.*)$/) { $urGridInfo{user} = $1; }
	$urGridInfo{lrmsId} = $tmpArray[2];
	$_ = $tmpArray[2];
	if (/^(\d*)\.(.*)$/) { $urGridInfo{server} = $2; }
	foreach (@URArray)
	{
		if (/^queue=(.*)$/) { $urGridInfo{queue} = $1; }
		if (/^resources_used.cput=(.*)$/)
		{
			$_ = $1;
			$_ =~ /(\d*):(\d\d):(\d\d)$/;
			$urGridInfo{cput} = $3 + $2 * 60 + $1 * 3600;
		}
		if (/^resources_used.walltime=(.*)$/)
		{
			$_ = $1;
			$_ =~ /(\d*):(\d\d):(\d\d)$/;
			$urGridInfo{walltime} = $3 + $2 * 60 + $1 * 3600;
		}
		if (/^resources_used.vmem=(.*)$/)
		{
			$_ = $1;
			$_ =~ /(\d*[M.k]b)$/;
			$urGridInfo{vmem} = $1;
		}
		if (/^resources_used.mem=(.*)$/)
		{
			$_ = $1;
			$_ =~ /(\d*[M.k]b)$/;
			$urGridInfo{mem} = $1;
		}
		if (/^Resource_List.neednodes=(\d*)$/)
		{
			$urGridInfo{processors} = $1;

			# attention! might also be list of hostnames,
			# in this case the number of hosts should be
			# counted!? What about SMP machines; is their
			# hostname listed N times or only once??
		}
		if (/^group=(.*)$/)
		{
			$urGridInfo{group} = $1;
		}
		if (/^jobname=(.*)$/)
		{
			$urGridInfo{jobName} = $1;
		}
		if (/^ctime=(\d*)$/)
		{
			$urGridInfo{ctime} = $1;
		}
		if (/^qtime=(\d*)$/)
		{
			$urGridInfo{qtime} = $1;
		}
		if (/^etime=(\d*)$/)
		{
			$urGridInfo{etime} = $1;
		}
		if (/^start=(\d*)$/)
		{
			$urGridInfo{start} = $1;
		}
		if (/^end=(\d*)$/)
		{
			$urGridInfo{end} = $1;
		}
		if (/^exec_host=(.*)$/)
		{
			$urGridInfo{execHost} = $1;
		}
		if (/^Exit_status=(\d*)$/)
		{
			$urGridInfo{exitStatus} = $1;
		}
	}

}

sub parseUR_lsf
{
	my $URString = $_[0];
	&printLog( 8, "UR string:\n$URString" );

	my @new = ();

	#	if (length($URString) <= $MAX_LSF_RECORDLENGTH_FOR_QUOTEWORDS) {
	@new = quotewords( " ", 0, $URString );

	#	}
	#	else {
	#	    # we use this instead for extreme records.
	#	    @new = &splitCVSstring(" ", "\"", $URString);
	#	}
	#$shift1 = new[22]: numAskedHosts
	#$shift2 = $new[23+$shift1]: numExHosts
	my $shift1 = $new[22];
	my $shift2 = $new[ 23 + $shift1 ];
	my $shift3 = $shift1 + $shift2;
	$urGridInfo{server} = $new[16];
	if ( $urGridInfo{server} =~ /^([^\.]*)\.(.*)$/ )
	{

		#server hostname has domain.
		#PLACEHOLDER
	}
	elsif ( $domainName ne "" )
	{

		# if no domain name in LSF log: add the CE's domain name
		$urGridInfo{server} .= ".$domainName";
	}

	$urGridInfo{queue} = $new[12];
	$urGridInfo{user}  = $new[11];

	# we now know the user's UNIX login, try to find out the group, that
	# isn't available in the LSF log file:
	my $groupOutput = `groups $urGridInfo{user}`;
	if ( $groupOutput =~ /^$urGridInfo{user} : (.+)$/ )
	{
		$urGridInfo{group} = $1;
	}

#We could add check for conf variable to use LSF fairshare group here. (Se A.Neal mail)

	$urGridInfo{lrmsId}     = $new[3];
	$urGridInfo{processors} = $new[6];
	if ( $new[10] == 0 )
	{    # happens if job was cancelled before executing?
		$urGridInfo{walltime} = 0;
	}
	else
	{
		$urGridInfo{walltime} = $new[2] - $new[10];
	}
	if ( $new[ 28 + $shift3 ] == -1 )
	{    # indicates that the value is not available!
		$new[ 28 + $shift3 ] = 0;
	}
	if ( $new[ 29 + $shift3 ] == -1 )
	{    # indicates that the value is not available!
		$new[ 29 + $shift3 ] = 0;
	}
	$urGridInfo{cput} =
	  int( $new[ 28 + $shift3 ] ) + int( $new[ 29 + $shift3 ] );
	$urGridInfo{mem}        = $new[ 54 + $shift3 ] . "k";
	$urGridInfo{vmem}       = $new[ 55 + $shift3 ] . "k";
	$urGridInfo{start}      = $new[10];
	$urGridInfo{end}        = $new[2];
	$urGridInfo{ctime}      = $new[7];
	$urGridInfo{jobName}    = $new[ 26 + $shift3 ];
	$urGridInfo{exitStatus} = $new[ 49 + $shift3 ];

	if ( $shift2 != 0 )
	{
		$urGridInfo{execHost} = "";
		for ( my $i = 1 ; $i <= $shift2 ; $i++ )
		{
			$urGridInfo{execHost} = $urGridInfo{execHost} . $new[ 23 + $i ];
			if ( $i != $shift2 )
			{
				$urGridInfo{execHost} = $urGridInfo{execHost} . ";";
			}
		}
	}
}

sub parseUR_sge
{
	my $URString = $_[0];
	my @URArray = split( ':', $URString );
	$urGridInfo{queue}      = $URArray[0];
	$urGridInfo{execHost}   = $URArray[1];
	$urGridInfo{group}      = $URArray[2];
	$urGridInfo{user}       = $URArray[3];
	$urGridInfo{jobName}    = $URArray[4];
	$urGridInfo{lrmsId}     = $URArray[5];
	$urGridInfo{ctime}      = $URArray[8];
	$urGridInfo{qtime}      = $URArray[9];
	$urGridInfo{etime}      = $URArray[9];
	$urGridInfo{start}      = $URArray[9];
	$urGridInfo{end}        = $URArray[10];
	$urGridInfo{exitStatus} = $URArray[12];
	$urGridInfo{walltime}   = $URArray[13];
	$urGridInfo{proessors}  = $URArray[34];
	$urGridInfo{cput}       = $URArray[36];
	$urGridInfo{mem}        = $URArray[17] + $URArray[18] + $URArray[19];
	$urGridInfo{vmem}       = $URArray[42];
}

sub parseUR_condor
{
	my $URString = $_[0];
	&printLog( "" . localtime() . ": UR string:$URString", 7 );

	my %classadItems = ();

	my @URArray = split( '=====', $URString );

	foreach my $urPart (@URArray)
	{
		if (   $urPart =~ /^(\S+)\s?=\s?\"(.*)\"$/
			|| $urPart =~ /^(\S+)\s?=\s?(.*)$/ )
		{

			# something like 'ClusterId = 12345' or 'Owner = "cms001"'
			my $item  = $1;
			my $value = $2;
			$classadItems{$item} = $value;
		}
	}

	if ( exists( $classadItems{ClusterId} ) )
	{

		# example: ClusterId = 6501
		$urGridInfo{lrmsId} = $classadItems{ClusterId};
	}

	if ( exists( $classadItems{Owner} ) )
	{

		# example: Owner = "cdf"
		$urGridInfo{user} = $classadItems{Owner};
	}

	# we now know the user's UNIX login, try to find out the group, that
	# isn't available in the Condor log file:
	my $groupOutput = `groups $urGridInfo{user}`;
	if ( $groupOutput =~ /^$urGridInfo{user} : (.+)$/ )
	{
		$urGridInfo{group} = $1;
	}

	if ( exists( $classadItems{x509userproxysubject} )
		&& !exists( $urGridInfo{USER_DN} ) )
	{

		# example: x509userproxysubject = "/C=IT ..."
		$urGridInfo{USER_DN} = $classadItems{x509userproxysubject};
	}

	if ( exists( $classadItems{RemoteWallClockTime} )
		&& $classadItems{RemoteWallClockTime} =~ /^(\d+)(\.\d*)?$/ )
	{

		# example: RemoteWallClockTime = 4264.000000
		$urGridInfo{walltime} = int($1);
	}

	$urGridInfo{cput} = 0;
	if ( exists( $classadItems{RemoteUserCpu} )
		&& $classadItems{RemoteUserCpu} =~ /^(\d+)(\.\d*)?$/ )
	{
		$urGridInfo{cput} += int($1);
	}
	if ( exists( $classadItems{LocalUserCpu} )
		&& $classadItems{LocalUserCpu} =~ /^(\d+)(\.\d*)?$/ )
	{
		$urGridInfo{cput} += int($1);
	}
	if ( exists( $classadItems{RemoteSysCpu} )
		&& $classadItems{RemoteSysCpu} =~ /^(\d+)(\.\d*)?$/ )
	{
		$urGridInfo{cput} += int($1);
	}
	if ( exists( $classadItems{LocalSysCpu} )
		&& $classadItems{LocalSysCpu} =~ /^(\d+)(\.\d*)?$/ )
	{
		$urGridInfo{cput} += int($1);
	}

	# no memory information in Condor history, but
	# mandatory for atmClient! Ugly hack: set to zero ...
	$urGridInfo{mem}  = 0;
	$urGridInfo{vmem} = 0;

	if ( exists( $classadItems{CompletionDate} ) )
	{

		# example: CompletionDate = 1126899118
		$urGridInfo{end} = $classadItems{CompletionDate};
	}

	if ( exists( $classadItems{JobStartDate} ) )
	{

		# example: JobStartDate = 1126878488
		$urGridInfo{start} = $classadItems{JobStartDate};
	}

	if ( exists( $classadItems{LastRemoteHost} )
		&& $classadItems{LastRemoteHost} =~ /^(.*)$/ )
	{

		# example: LastRemoteHost = "vm1@fnpc212.fnal.gov"
		$urGridInfo{execHost} = $1;
	}

	if ( exists( $classadItems{GlobalJobId} )
		&& $classadItems{GlobalJobId} =~ /^([^\#]*)\#.*$/ )
	{

		# example: GlobalJobId = "fngp-osg.fnal.gov#1126868442#6501.0"
		$urGridInfo{server} = $1;

		# WARNING: take this also ad dgJobId or is it better to construct
		#          a unique ID???
	}

	if ( exists( $classadItems{Cmd} ) )
	{

		# use as jobName!
		$urGridInfo{jobName} = $classadItems{Cmd};
	}

	if ( exists( $classadItems{JobUniverse} ) )
	{

		# example: JobUniverse = 5
		$urGridInfo{queue} = $classadItems{JobUniverse};
	}

	if ( !exists( $classadItems{ExitBySignal} ) )
	{

		# old classad format, use ExitStatus:
		if ( exists( $classadItems{ExitStatus} ) )
		{

			# example: ExitStatus = 0
			$urGridInfo{exitStatus} = $classadItems{ExitStatus};
		}
	}
	elsif ( $classadItems{ExitBySignal} =~ /false/i )
	{

		# new classad format: "clean" exit of job, take returned exit code:
		if ( exists( $classadItems{ExitCode} ) )
		{

			# example: ExitCode = 0
			$urGridInfo{exitStatus} = $classadItems{ExitCode};
		}
	}
	else
	{

		# new classad format: job crashed due to an unhandled signal,
		# take signal code as exit code:
		if ( exists( $classadItems{ExitSignal} ) )
		{

			# example: ExitCode = 0
			$urGridInfo{exitStatus} = $classadItems{ExitSignal};
		}
	}

}

#FIXME Spostare urGridInfo da variabile globale ad argomento della funzione.
sub callAtmClient
{
	my $now_string = localtime();
	my $cmd        = "";
	my $status     = -1;

	my $forceThisJobLocal = "no";
	if ( exists( $urGridInfo{JOB_TYPE} ) && $urGridInfo{JOB_TYPE} eq "local" )
	{
		&printLog( 5,
"JOB_TYPE=local found in UR file. Storing UR on Resource HLR _only_ and as LOCAL job!"
		);
		$forceThisJobLocal = "yes";
	}
	my $localHostname = $hostName . "." . $domainName;

	# Get submission host (for LSF):
	if ( $urGridInfo{lrmsType} eq "lsf" )
	{
		$urGridInfo{submitHost} = $urGridInfo{server};
	}
	elsif ( $urGridInfo{lrmsType} eq "pbs" )
	{
		$urGridInfo{lrmsServer} = $urGridInfo{server};
	}
	if ( $urGridInfo{lrmsType} eq "sge" ) #SGE doesn't provide LRMS server info.
	{
		$urGridInfo{server} = $localHostname;  #default. Not working on multi CE
		if ( exists( $configValues{useCEHostName} )
			&& $configValues{useCEHostName} ne "" )
		{

			#sggested configuration on multiCe and single LRMS deployments.
			$urGridInfo{server} = $configValues{useCEHostName};
		}
	}

	# CE ID:
	if ( exists( $urGridInfo{CE_ID} ) && $urGridInfo{CE_ID} ne "" )
	{
		$urGridInfo{resGridId} = $urGridInfo{"CE_ID"};
		&printLog( 6, "CE_ID from UR file: $urGridInfo{resGridId}" );

		# get also ceHostName!
		if ( $urGridInfo{CE_ID} =~ /^([^:]*):.*/ )
		{
			$urGridInfo{ceHostName} = $1;
		}
		$urGridInfo{execCE} = $urGridInfo{"CE_ID"};
	}
	elsif ($forceThisJobLocal
		&& exists( $urGridInfo{server} )
		&& $urGridInfo{server} ne ""
		&& exists( $urGridInfo{queue} )
		&& $urGridInfo{queue} ne "" )
	{
		$urGridInfo{resGridId} = "$urGridInfo{server}:$urGridInfo{queue}";
		&printLog( 5,
			"Constructed resource ID for local job: $urGridInfo{resGridId}" );
	}
	else
	{
		&printLog( 2, "Error: cannot determine CE/resource ID! Skipping job!" );
		return -3;
	}

	if (   exists( $configValues{useCEHostName} )
		&& $configValues{useCEHostName} ne ""
		&& exists( $urGridInfo{queue} )
		&& $urGridInfo{queue} ne "" )
	{

		$urGridInfo{resGridId} =
		  "$configValues{useCEHostName}:$urGridInfo{queue}";
		&printLog( 4,
"useCEHostName specified in configuration file! Using as resource ID: $urGridInfo{resGridId}"
		);
	}
	elsif ( $configValues{localHostNameAsCEHostName} eq "yes" )
	{
		$urGridInfo{resGridId} = "$localHostname:$urGridInfo{queue}";
		&printLog( 4,
"localHostNameAsCEHostName =\"yes\" in configuration file! Using as resource ID: $urGridInfo{resGridId}"
		);
	}

	# does the CE ID finish with the queue name???
	if ( $urGridInfo{resGridId} !~ /[-:]$urGridInfo{queue}\W*$/ )
	{
		$urGridInfo{resGridId} =
		  $urGridInfo{resGridId} . "-" . $urGridInfo{queue};
		$urGridInfo{execCE} = $urGridInfo{execCE} . "-" . $urGridInfo{queue};
		&printLog( 5, "Added queue name: CE_ID = $urGridInfo{resGridId}" );
	}
	if (   $urGridInfo{execCE} ne ""
		&& $urGridInfo{execCE} !~ /[-:]$urGridInfo{queue}\W*$/ )
	{
		$urGridInfo{execCE} = $urGridInfo{execCE} . "-" . $urGridInfo{queue};
	}

	# User DN:
	my $userDN = "";
	if ( exists( $urGridInfo{USER_DN} ) && $urGridInfo{USER_DN} ne "" )
	{
		$userDN = $urGridInfo{USER_DN};
		&printLog( 8, "USER_DN from UR file: $userDN" );
	}
	else
	{
		&printLog( 3, "Couldn't determine USER_DN." );
	}

	# FQAN:
	if ( exists( $urGridInfo{USER_FQAN} ) && $urGridInfo{USER_FQAN} ne "" )
	{
		$urGridInfo{fqan} = $urGridInfo{USER_FQAN};
		&printLog( 5, "USER_FQAN from UR file: $urGridInfo{fqan}" );
	}
	else
	{
		&printLog( 3, "Couldn't determine USER_FQAN." );
	}

	# VO:
	&determineUserVO(
		$urGridInfo{fqan},   $urGridInfo{user}, $havePoolAccounts,
		$urGridInfo{userVo}, $urGridInfo{voOrigin}
	);

	# grid Job ID:
	my $gridJobId = "";
	if ( exists( $urGridInfo{GRID_JOBID} ) && $urGridInfo{GRID_JOBID} ne "" )
	{
		$gridJobId = $urGridInfo{GRID_JOBID};
		&printLog( 5, "GRID_JOBID from UR file: $gridJobId" );
	}
	elsif (exists( $urGridInfo{server} )
		&& $urGridInfo{server} ne ""
		&& exists( $urGridInfo{lrmsId} )
		&& $urGridInfo{lrmsId} ne ""
		&& exists( $urGridInfo{start} )
		&& $urGridInfo{start} ne "" )
	{

		$gridJobId =
		    $urGridInfo{server} . ":"
		  . $urGridInfo{lrmsId} . "_"
		  . $urGridInfo{start};

		&printLog( 4, "No grid job ID. Constructed a unique ID: $gridJobId" );
	}
	else
	{
		&printLog( 2,
			"Error: cannot retrieve or construct a unique job ID! Skipping job"
		);
		return -4;
	}

	# retrieving GlueCEInfoTotalCPUs

	if ( exists( $urGridInfo{queue} ) )
	{
		&searchForNumCpus( $configValues{gipDynamicTmpCEFiles},
			$urGridInfo{queue}, $urGridInfo{end}, $urGridInfo{numCPUs} );
	}

	# building command:
	my $legacyCmd = "$dgasLocation/libexec/dgas-atmClient";

	$cmd = "";

	if ( $gridJobId ne "" )
	{
		$cmd .= " --jobid \"$gridJobId\"";
	}
	if ( $urGridInfo{resGridId} ne "" )
	{
		$cmd .= " --resgridid \"$urGridInfo{resGridId}\"";
	}
	if ( $userDN ne "" )
	{
		$cmd .= " --usrcert \"$userDN\"";
	}

	if (
		exists(
			$urGridInfo{
				"GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU"}
		)
		&& $urGridInfo{
			"GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU"} ne
		""
	  )
	{
		my $si2k =
		  $urGridInfo{
			"GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU"};
		$cmd .= " \"si2k=$si2k\"";
		&printLog( 8,
"Got 'GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU' from UR file: $si2k"
		);
		&printLog( 9,
			"(note: Taking mean SI00, ignoring GlueHostBenchmarkSI00)" );
	}
	elsif ( exists( $urGridInfo{GlueHostBenchmarkSI00} )
		&& $urGridInfo{GlueHostBenchmarkSI00} ne "" )
	{
		$cmd .= " \"si2k=$urGridInfo{GlueHostBenchmarkSI00}\"";
		&printLog( 8,
"Got 'GlueHostBenchmarkSI00' from UR file: $urGridInfo{GlueHostBenchmarkSI00}"
		);
	}

	if (
		exists(
			$urGridInfo{
				"GlueHostApplicationSoftwareRunTimeEnvironment: SF00MeanPerCPU"}
		)
		&& $urGridInfo{
			"GlueHostApplicationSoftwareRunTimeEnvironment: SF00MeanPerCPU"} ne
		""
	  )
	{
		my $sf2k =
		  $urGridInfo{
			"GlueHostApplicationSoftwareRunTimeEnvironment: SF00MeanPerCPU"};
		$cmd .= " \"sf2k=$sf2k\"";
		&printLog( 8,
"Got 'GlueHostApplicationSoftwareRunTimeEnvironment: SF00MeanPerCPU' from UR file: $sf2k"
		);
		&printLog( 9,
			"(note: Taking mean SF00, ignoring GlueHostBenchmarkSF00)" );
	}
	elsif ( exists( $urGridInfo{GlueHostBenchmarkSF00} )
		&& $urGridInfo{GlueHostBenchmarkSF00} ne "" )
	{
		$cmd .= " \"sf2k=$urGridInfo{GlueHostBenchmarkSF00}\"";
		&printLog( 8,
"Got 'GlueHostBenchmarkSF00' from UR file: $urGridInfo{GlueHostBenchmarkSF00}"
		);
	}

	# Is this a strictly local job? Let the HLR server know!
	if ( exists( $urGridInfo{JOB_TYPE} ) && $urGridInfo{JOB_TYPE} eq "local" )
	{
		$cmd .= " \"accountingProcedure=outOfBand\"";
	}
	elsif ( exists( $urGridInfo{JOB_TYPE} ) && $urGridInfo{JOB_TYPE} eq "grid" )
	{
		$cmd .= " \"accountingProcedure=grid\"";
	}
	$cmd .= " \"URCREATION=$now_string\"";
	if ( $configValues{useUrKeyDefFile} eq "yes" )
	{
		$cmd .= " -3";
		my @atmTags;
		&composeATMtags( \@ATMDefinitions, \%urGridInfo, \@atmTags );
		foreach my $tag (@atmTags)
		{
			$cmd .= " \"$tag\"";
		}
	}
	else
	{

		#this is to ensure record consistency for backward compatibility
		if ( exists( $urGridInfo{cput} ) )
		{
			$cmd .= " \"CPU_TIME=$urGridInfo{cput}\"";
		}
		if ( exists( $urGridInfo{walltime} ) )
		{
			$cmd .= " \"WALL_TIME=$urGridInfo{walltime}\"";
		}
		if ( exists( $urGridInfo{mem} ) )
		{
			$cmd .= " \"PMEM=$urGridInfo{mem}\"";
		}
		if ( exists( $urGridInfo{vmem} ) )
		{
			$cmd .= " \"VMEM=$urGridInfo{vmem}\"";
		}
		if ( exists( $urGridInfo{queue} ) )
		{
			$cmd .= " \"QUEUE=$urGridInfo{queue}\"";
		}
		if ( exists( $urGridInfo{user} ) )
		{
			$cmd .= " \"USER=$urGridInfo{user}\"";
		}
		if ( exists( $urGridInfo{lrmsId} ) )
		{
			$cmd .= " \"LRMSID=$urGridInfo{lrmsId}\"";
		}
		if ( exists( $urGridInfo{processors} ) )
		{
			$cmd .= " \"PROCESSORS=$urGridInfo{processors}\"";
		}
		if ( exists( $urGridInfo{group} ) )
		{
			$cmd .= " \"group=$urGridInfo{group}\"";
		}
		if ( exists( $urGridInfo{jobName} ) )
		{
			$cmd .= " \"jobName=$urGridInfo{jobName}\"";
		}
		if ( exists( $urGridInfo{start} ) )
		{
			$cmd .= " \"start=$urGridInfo{start}\"";
		}
		if ( exists( $urGridInfo{end} ) )
		{
			$cmd .= " \"end=$urGridInfo{end}\"";
		}
		if ( exists( $urGridInfo{ctime} ) )
		{
			$cmd .= " \"ctime=$urGridInfo{ctime}\"";
		}
		if ( exists( $urGridInfo{qtime} ) )
		{
			$cmd .= " \"qtime=$urGridInfo{qtime}\"";
		}
		if ( exists( $urGridInfo{etime} ) )
		{
			$cmd .= " \"etime=$urGridInfo{etime}\"";
		}
		if ( exists( $urGridInfo{exitStatus} ) )
		{
			$cmd .= " \"exitStatus=$urGridInfo{exitStatus}\"";
		}
		if ( exists( $urGridInfo{execHost} ) )
		{
			$cmd .= " \"execHost=$urGridInfo{execHost}\"";
		}
		if ( exists( $urGridInfo{ceHostName} ) )
		{
			$cmd .= " \"ceHostName=$urGridInfo{ceHostName}\"";
		}
		if ( exists( $urGridInfo{execCE} ) )
		{
			$cmd .= " \"execCe=$urGridInfo{execCE}\"";
		}
		if ( exists( $urGridInfo{submitHost} ) )
		{
			$cmd .= " \"submitHost=$urGridInfo{submitHost}\"";
		}
		if ( exists( $urGridInfo{lrmsServer} ) )
		{
			$cmd .= " \"lrmsServer=$urGridInfo{lrmsServer}\"";
		}
		if ( exists( $urGridInfo{numCPUs} ) )
		{
			$cmd .= " \"GlueCEInfoTotalCPUs=$urGridInfo{numCPUs}\"";
		}
		if ( exists( $urGridInfo{timeZone} ) )
		{
			$cmd .= " \"tz=$urGridInfo{timeZone}\"";
		}
		if ( $urGridInfo{fqan} ne "" )
		{
			$cmd .= " \"fqan=$urGridInfo{fqan}\"";
		}
		if ( $urGridInfo{userVo} ne "" )
		{
			$cmd .= " \"userVo=$urGridInfo{userVo}\"";
		}
		if ( $urGridInfo{voOrigin} ne "" )
		{
			$cmd .= " \"voOrigin=$urGridInfo{voOrigin}\"";
		}
		if ( $urGridInfo{siteName} ne "" )
		{
			$cmd .= " \"SiteName=$urGridInfo{siteName}\"";
		}
		if ( $ceCertificateSubject ne "" )
		{
			$cmd .= " \"ceCertificateSubject=$ceCertificateSubject\"";
		}
	}

	# running the command:
	if ( $voToProcess ne "" )
	{
		my $processRecord = 0;
		my @voList = split( /;/, $voToProcess );
		foreach my $vo (@voList)
		{
			if ( $urGridInfo{userVo} eq $vo )
			{
				&printLog( 4,
					"Record for VO:$urGridInfo{userVo} found. forwarding it." );
				$processRecord = 1;
			}
		}
		if ( $processRecord == 0 )
		{

			&printLog( 8,
				"Record for VO:$urGridInfo{userVo} found. SKIPPING." );
			return 0;
		}
	}

	my $exe = "";
	foreach my $transportLayer (@transportLayers)
	{
		if ( ( $transportLayer ne "Legacy" ) )
		{
			my $recordComposer = $configValues{"recordComposer$transportLayer"};
			my $recordProducer = $configValues{"recordProducer$transportLayer"};
			my $arguments      = $cmd;
			$exe = $recordComposer . " " . $arguments . " | " . $recordProducer;
			&printLog( 8, "Writing: $exe" );

			#$arguments =~ s/\"/\\\"/g;
			my $sqlStatement =
"INSERT INTO commands (key, transport, composer, arguments, producer, recordDate, lrmsId, commandStatus) VALUES (NULL,'$transportLayer','$recordComposer','$arguments','$recordProducer','$urGridInfo{start}', '$urGridInfo{lrmsId}',0)";
			my $sth =
			  $dbh->prepare(
"INSERT INTO commands (key, transport, composer, arguments, producer, recordDate, lrmsId, commandStatus) VALUES (NULL,'$transportLayer',?,?,?,'$urGridInfo{start}', '$urGridInfo{lrmsId}',0)"
			  );
			my $querySuccesfull = 1;
			my $queryCounter    = 0;
			while ( $keepGoing && $querySuccesfull )
			{
				eval {
					my $res =
					  $sth->execute( $recordComposer, $arguments,
						$recordProducer );
				};
				if ($@)
				{
					&printLog( 3, "WARN: ($queryCounter) $@" );
					print "Retrying in $queryCounter\n";
					for (
						my $i = 0 ;
						$keepGoing && ( $i < $queryCounter ) ;
						$i++
					  )
					{
						sleep $i;
					}
					$queryCounter++;
				}
				else
				{
					$querySuccesfull = 0;
					&printLog( 9, "$sqlStatement" );
				}
				last if ( $queryCounter >= 10 );
			}
		}
		else
		{

			#Legacy
			$exe = $legacyCmd . $cmd;
			&printLog( 8, "Writing: $exe" );
			my $arguments = $cmd;

			#$arguments =~ s/\"/\\\"/g;
			my $sqlStatement =
"INSERT INTO commands (key, transport, composer, arguments, producer, recordDate, lrmsId, commandStatus) VALUES (NULL,'Legacy','$legacyCmd','$arguments','','$urGridInfo{start}', '$urGridInfo{lrmsId}','')";
			my $sth =
			  $dbh->prepare(
"INSERT INTO commands (key, transport, composer, arguments, producer, recordDate, lrmsId, commandStatus) VALUES (NULL,'Legacy',?,?,'','$urGridInfo{start}', '$urGridInfo{lrmsId}','')"
			  );
			my $querySuccesfull = 1;
			my $queryCounter    = 0;
			while ( $keepGoing && $querySuccesfull )
			{
				eval { my $res = $sth->execute( $legacyCmd, $arguments ); };
				if ($@)
				{
					&printLog( 3, "WARN: ($queryCounter) $@" );
					print "Retrying in $queryCounter\n";
					for (
						my $i = 0 ;
						$keepGoing && ( $i < $queryCounter ) ;
						$i++
					  )
					{
						sleep $i;
					}
					$queryCounter++;
				}
				else
				{
					$querySuccesfull = 0;
					&printLog( 9, "$sqlStatement" );
				}
				last if ( $queryCounter >= 10 );
			}
		}
	}
}

sub determineUserVO()
{

	my $retVal = 1;    # not found

	my $uFqan       = $_[0];
	my $uid         = $_[1];
	my $havePoolAcc = $_[2];

	my $voName = "";

	my @fqans;
	if ( $uFqan ne "" )
	{
		@fqans = split( /;/, $uFqan );
	}
	my @fqanParts;
	if ( scalar(@fqans) > 0 && $fqans[0] ne "" )
	{
		@fqanParts = split( /\//, $fqans[0] );
	}

	if ( scalar(@fqanParts) > 1 && $fqanParts[1] ne "" )
	{
		if ( $fqanParts[1] =~ /^VO=(.*)$/ )
		{
			$fqanParts[1] = $1;
		}
		$_[3]   = $fqanParts[1];    # userVo
		$_[4]   = "fqan";           # voOrigin
		$retVal = 0;

		&printLog( 7, "Determined user VO from FQAN: $_[3]" );
	}
	elsif ( $havePoolAcc && &getVOFromPoolAccountPattern( $uid, $voName ) )
	{
		$_[3]   = $voName;          # userVo
		$_[4]   = "pool";           # voOrigin
		$retVal = 0;
	}
	elsif ( $localUserGroup2VOMap ne "" )
	{

		# try local user/group mapping!
		if ( !-r "$localUserGroup2VOMap" )
		{
			&printLog( 3,
"WARNING: cannot read from localUserGroup2VOMap '$localUserGroup2VOMap' ... cannot determine an eventual local mapping for local user $urGridInfo{user}!"
			);
		}
		else
		{

			# try specific user first!
			my $cmd =
"grep '^user\[\[:blank:\]\]$urGridInfo{user}\[\[:blank:\]\]' $localUserGroup2VOMap";
			my $userLine = `$cmd`;
			if ( $userLine =~ /^user\s$urGridInfo{user}\s([^\s]+).*$/ )
			{

				# found the VO!!!
				$_[3]   = $1;       # userVo
				$_[4]   = "map";    # voOrigin
				$retVal = 0;
				&printLog( 7,
"Determined user VO from localUserGroup2VOMap (user $urGridInfo{user}): $_[3]"
				);
			}
			else
			{

				# no specific user mapping, try groups!
				my $groupOutput = `groups $urGridInfo{user}`;
				if ( $groupOutput =~ /^$urGridInfo{user} : (.+)$/ )
				{
					my @groupvec = split( / /, $1 );
					foreach my $lgroup (@groupvec)
					{
						my $cmd =
"grep '^group\[\[:blank:\]\]$lgroup\[\[:blank:\]\]' $localUserGroup2VOMap";
						my $groupLine = `$cmd`;
						if ( $groupLine =~ /^group\s$lgroup\s([^\s]+).*$/ )
						{

							# found the VO!!!
							$_[3]   = $1;       # userVo
							$_[4]   = "map";    # voOrigin
							$retVal = 0;
							&printLog( 7,
"Determined user VO from localUserGroup2VOMap (group $lgroup): $_[3]"
							);
							last;
						}
					}
				}
			}
		}
	}

	if ( $retVal != 0 )
	{
		&printLog( 3, "Could not determine user VO for local job!" );
	}

	return $retVal;
}

sub getVOFromPoolAccountPattern()
{

	my $uid = $_[0];

	# if patterns are specified as configuration file, use them!
	if ( $poolAccountPatternFile ne "" )
	{

		my $userVo = &getVOFromConfiguredPoolAccountPatterns($uid);

		if ( $userVo ne "" )
		{
			$_[1] = $userVo;    # userVo
			&printLog( 7,
"Determined user VO from pool account (poolAccountPatternFile): $_[1]"
			);
			return 1;           # ok
		}

		return 0;               # not found
	}

	# if no patterns configured, use the built-in patterns (for backward
	# compatibility ... phase this out later?)
	elsif ($uid =~ /^(.*)sgm$/
		|| $uid =~ /^(.*)prd$/
		|| $uid =~ /^(.*)sgm\d{3}$/
		|| $uid =~ /^(.*)prd\d{3}$/
		|| $uid =~ /^sgm(.*)\d{3}$/
		|| $uid =~ /^prd(.*)\d{3}$/
		|| $uid =~ /^sgm(.*)$/
		|| $uid =~ /^prd(.*)$/
		|| $uid =~ /^(.*)\d{3}$/ )
	{

		$_[1] = $1;    # userVo

		&printLog( 7,
			"Determined user VO from pool account (built-in patterns): $_[1]" );

		return 1;      # ok
	}

	return 0;          # not found
}

##-------> sig handlers subroutines <---------##

sub sigHUP_handler
{
	&printLog( 3, "got SIGHUP" );
	$keepGoing = 1;
	goto START;
}

sub sigINT_handler
{
	&printLog( 3, "got SIGINT" );
	$keepGoing = 0;
}

## ----- print error and quit ----- ##

sub error
{
	if ( scalar(@_) > 0 )
	{
		&printLog( 2, "$_[0]" );
	}
	exit(1);
}

## ----- subs to extract GlueCEInfoTotalCPUs ----- ##

sub numCPUs
{
	my $ldifFile = $_[0];
	my $queue    = $_[1];
	my $numCPUs;
	open( LDIF, "$ldifFile" ) || &printLog( 2, "Error opening file $ldifFile" );
	my $line;
	while ( $line = <LDIF> )
	{

		#if (/^dn:\sGlueCEUniqueID=(.*)$queue,(.*)$/)
		if ( $line =~ /GlueCEUniqueID=(.*)$queue,/ )
		{
			my $innerline;
			&printLog( 9, $line );
			while ( $innerline = <LDIF> )
			{
				if ( $innerline =~ /GlueCEInfoTotalCPUs:\s?(.*)$/ )
				{
					if ( $1 != 0 )
					{
						$_[2] = $1;
						close(LDIF);
						return;
					}
				}
			}
		}
	}
	close(LDIF);
	return;
}

sub gip2numCPUs
{
	my $hostname = $_[0];
	my $port     = $_[1];
	my $queue    = $_[2];
	&printLog( 6, "Searching LDAP: $hostname,$port,$queue" );

	#$_[3] -> numCpus
	my $cmd = "/usr/bin/ldapsearch -LLL -x -h $hostname -p $port  -b o=grid";
	open( LDIF, "$cmd |" );
	while ( my $line = <LDIF> )
	{
		printLog( 9, $line );

		#if (/^dn:\sGlueCEUniqueID=(.*)$queue,(.*)$/)
		if ( $line =~ /GlueCEUniqueID=(.*)$queue,/ )
		{
			my $innerline;
			&printLog( 8, $line );
			while ( $innerline = <LDIF> )
			{
				if ( $innerline =~ /GlueCEInfoTotalCPUs:\s?(.*)$/ )
				{
					if ( $1 != 0 )
					{
						$_[3] = $1;
						close(LDIF);
						return;
					}
				}
			}
		}
	}
	close(LDIF);
	return;
}

sub searchForNumCpus
{
	my $curtime = time;
	if ( $curtime - $_[2] > 864000
	  ) ##CPU Number info for jobs older than 10 days can't be considered as valid.
	{
		$_[3] = 0;
		return;
	}
	if ( $_[0] =~ /^ldap:\/\/(.*):(\d*)$/ )
	{
		&printLog( 7, "Using LDAP: $1:$2" );

		if ( exists $numCpus{ $_[1] } )
		{
			if ( $curtime - $numCpusTS{ $_[1] } >
				7200 )    #current value xeceeded TTL
			{
				my $numCpusBuff = 0;
				&gip2numCPUs( $1, $2, $_[1], $numCpusBuff );
				$numCpus{ $_[1] }   = $numCpusBuff;
				$numCpusTS{ $_[1] } = $curtime;
				$_[3]               = $numCpus{ $_[1] };
				&printLog( 8,
					"numCpus: $numCpus{$_[1]}, queue: $_[1], TS: $curtime" );
				return;
			}
			else
			{
				$_[3] = $numCpus{ $_[1] };
				&printLog( 8,
"numCpus, cache: $numCpus{$_[1]}, queue: $_[1], TS: $numCpusTS{$_[1]}"
				);
				return;
			}
		}
		else
		{
			my $numCpusBuff = 0;
			&gip2numCPUs( $1, $2, $_[1], $numCpusBuff );
			$numCpus{ $_[1] }   = $numCpusBuff;
			$numCpusTS{ $_[1] } = $curtime;
			$_[3]               = $numCpus{ $_[1] };
			&printLog( 8,
				"numCpus: $numCpus{$_[1]}, queue: $_[1], TS: $curtime" );
			return;
		}
	}
	else
	{
		my @list  = glob( $_[0] );
		my $queue = $_[1];
		my $file;
		foreach $file (@list)
		{
			my $numCpusBuff;
			&numCPUs( $file, $queue, $numCpusBuff );
			if ( $numCpusBuff != 0 )
			{
				&printLog( 9, "Found in: $file" );
				$_[3] = $numCpusBuff;
			}
		}
	}
	return;
}

sub populateATMDef
{
	my ( $input, $ATMDefs ) = @_;
	open( FILE, $input ) || return 1;
	while (<FILE>)
	{
		if (/^\s*field:(.*)\s*$/)
		{
			&printLog( 7, $1 );
			push( @$ATMDefs, $1 );
		}
	}
}

sub composeATMtags
{
	&printLog( 7, "Entering:composeATM" );
	my ( $ATMdefs, $urHash, $atmTags ) = @_;
	my %hashBuff = %$urHash;
	foreach my $def (@$ATMdefs)
	{
		&printLog( 7, "composeATM:$def" );
		$def =~ s/\$(\w+)/$hashBuff{$1}/g;
		if ( exists $hashBuff{$1} )
		{
			push( @$atmTags, $def );
			&printLog( 7, "inserted in ATM:$def" );
		}
	}
}

sub getVOFromConfiguredPoolAccountPatterns
{
	my $uid    = $_[0];
	my $userVo = "";

	my $cmdline =
"$dgasLocation/libexec/glite-dgas-voFromPoolAccountPatterns.pl '$poolAccountPatternFile' '$uid'";

	&printLog( 9, "Executing: $cmdline" );

	my $result = `$cmdline`;

	my @resultLines = split( /\n/, $result );

	foreach my $line (@resultLines)
	{
		chomp($line);

		if ( $line =~ /ERROR/i )
		{    # whatever error
			&printLog( 2, "$line" );
		}
		elsif ( $line =~ /^userVo = '(.*)'$/ )
		{    # the user VO!
			$userVo = $1;
		}
		else
		{    # whatever comment
			&printLog( 9, "$line" );
		}

	}

	return $userVo;
}

sub parseConf
{
	my $fconf = $_[0];
	open( FILE, "$fconf" )
	  || &errorNoRemoveLock("Error: Cannot open configuration file $fconf");
	while (<FILE>)
	{
		if (/\$\{(.*)\}/)
		{
			my $value = $ENV{$1};
			s/\$\{$1\}/$value/g;
		}
		if (/^lrmsType\s*=\s*\"(.*)\"$/) { $configValues{lrmsType} = $1; }
		if (/^lrmsAcctLogDir\s*=\s*\"(.*)\"$/)
		{
			$configValues{lrmsAcctLogDir} = $1;
		}
		if (/^pbsAcctLogDir\s*=\s*\"(.*)\"$/)
		{
			$configValues{pbsAcctLogDir} = $1;
		}
		if (/^lsfAcctLogDir\s*=\s*\"(.*)\"$/)
		{
			$configValues{lsfAcctLogDir} = $1;
		}
		if (/^sgeAcctLogDir\s*=\s*\"(.*)\"$/)
		{
			$configValues{sgeAcctLogDir} = $1;
		}
		if (/^condorHistoryCommand\s*=\s*\"(.*)\"$/)
		{
			$configValues{condorHistoryCommand} = $1;
		}
		if (/^ceJobMapLog\s*=\s*\"(.*)\"$/)
		{
			$configValues{ceJobMapLog} = $1;
		}
		if (/^useCEJobMap\s*=\s*\"(.*)\"$/)
		{
			$configValues{useCEJobMap} = $1;
		}
		if (/^jobsToProcess\s*=\s*\"(.*)\"$/)
		{
			$configValues{jobsToProcess} = $1;
		}
		if (/^keyList\s*=\s*\"(.*)\"$/) { $configValues{keyList} = $1; }
		if (/^ldifDefaultFiles\s*=\s*\"(.*)\"$/)
		{
			$configValues{ldifDefaultFiles} = $1;
		}
		if (/^glueLdifFile\s*=\s*\"(.*)\"$/)
		{
			$configValues{glueLdifFile} = $1;
		}
		if (/^collectorLockFileName\s*=\s*\"(.*)\"$/)
		{
			$configValues{collectorLockFileName} = $1;
		}
		if (/^collectorLogFileName\s*=\s*\"(.*)\"$/)
		{
			$configValues{collectorLogFileName} = $1;
		}
		if (/^collectorBufferFileName\s*=\s*\"(.*)\"$/)
		{
			$configValues{collectorBufferFileName} = $1;
		}
		if (/^mainPollInterval\s*=\s*\"(.*)\"$/)
		{
			$configValues{mainPollInterval} = $1;
		}
		if (/^collectorPollInterval\s*=\s*\"(.*)\"$/)
		{
			$configValues{collectorPollInterval} = $1;
		}
		if (/^timeInterval\s*=\s*\"(.*)\"$/)
		{
			$configValues{timeInterval} = $1;
		}
		if (/^jobPerTimeInterval\s*=\s*\"(.*)\"$/)
		{
			$configValues{jobPerTimeInterval} = $1;
		}
		if (/^ignoreJobsLoggedBefore\s*=\s*\"(.*)\"$/)
		{
			$configValues{ignoreJobsLoggedBefore} = $1;
		}
		if (/^limiterWaitFor\s*=\s*\"(.*)\"$/)
		{
			$configValues{limiterWaitFor} = $1;
		}
		if (/^maxNumRecords\s*=\s*\"(.*)\"$/)
		{
			$configValues{maxNumRecords} = $1;
		}
		if (/^systemLogLevel\s*=\s*\"(.*)\"$/)
		{
			$configValues{systemLogLevel} = $1;
		}
		if (/^useUrKeyDefFile\s*=\s*\"(.*)\"$/)
		{
			$configValues{useUrKeyDefFile} = $1;
		}
		if (/^urKeyDefFile\s*=\s*\"(.*)\"$/)
		{
			$configValues{urKeyDefFile} = $1;
		}
		if (/^localUserGroup2VOMap\s*=\s*\"(.*)\"$/)
		{
			$configValues{localUserGroup2VOMap} = $1;
		}
		if (/^siteName\s*=\s*\"(.*)\"$/) { $configValues{siteName} = $1; }
		if (/^localHostNameAsCEHostName\s*=\s*\"(.*)\"$/)
		{
			$configValues{localHostNameAsCEHostName} = $1;
		}
		if (/^havePoolAccounts\s*=\s*\"(.*)\"$/)
		{
			$configValues{havePoolAccounts} = $1;
		}
		if (/^poolAccountPatternFile\s*=\s*\"(.*)\"$/)
		{
			$configValues{poolAccountPatternFile} = $1;
		}
		if (/^useCEHostName\s*=\s*\"(.*)\"$/)
		{
			$configValues{useCEHostName} = $1;
		}
		if (/^gipDynamicTmpCEFiles\s*=\s*\"(.*)\"$/)
		{
			$configValues{gipDynamicTmpCEFiles} = $1;
		}
		if (/^voToProcess\s*=\s*\"(.*)\"$/)
		{
			$configValues{voToProcess} = $1;
		}
		if (/^transportLayer\s*=\s*\"(.*)\"$/)
		{
			$configValues{transportLayer} = $1;
		}
		if (/^dgasDB\s*=\s*\"(.*)\"$/) { $configValues{dgasDB} = $1; }
		if (/^recordProducer(.*)\s*=\s*\"(.*)\"$/)
		{
			my $producerNameBuff = $1;
			my $producerBuff     = $2;
			$producerNameBuff =~ s/\s//g;
			$configValues{"recordProducer$producerNameBuff"} = $producerBuff;
		}
		if (/^recordComposer(.*)\s*=\s*\"(.*)\"$/)
		{
			my $composerNameBuff = $1;
			my $composerBuff     = $2;
			$composerNameBuff =~ s/\s//g;
			$configValues{"recordComposer$composerNameBuff"} = $composerBuff;
		}
	}
	close(FILE);
}

##-------> lock  subroutines <---------##
sub putLock
{
	my $lockName = $_[0];
	open( IN, "< $lockName" ) && return 1;
	close(IN);
	open( OUT, "> $lockName" ) || return 2;
	print OUT $$;    ## writes pid
	close(OUT);
	return 0;
}

sub delLock
{
	my $lockName = $_[0];
	open( IN, "< $lockName" ) || return 1;
	close(IN);
	my $status = system("rm -f $lockName");
	return $status;
}

sub existsLock
{
	my $lockName = $_[0];
	if ( open( IN, "< $lockName" ) != 0 )
	{
		close(IN);
		return 0;
	}
	return 1;
}

##--------> routines for job processing buffer <---------##
sub putBuffer
{

	# arguments are: 0 = buffer name
	#                1 = last LRMS job id
	#                2 = last LRMS job timestamp (log time)
	my $buffName = $_[0];
	if ( $keepGoing == 1 )
	{

# this is done only if no SIGINT was received!
#print "".localtime().": Writing info on last processed job in buffer $buffName.";
		open( OUT, "> $buffName" ) || return 2;
		my $dateBuff = localtime( $_[2] );
		print OUT "$_[1]:$_[2]:$dateBuff";
		&printLog( 9, "in Buffer $_[1]:$_[2]", 1 );
		close(OUT);

		my $tmpBuffer = "${buffName}_tmp";
		if ( -e $tmpBuffer )
		{
			print
			  "Removing temporary buffer $tmpBuffer ... not required anymore!";
			`rm -f $tmpBuffer &> /dev/null`;
		}
	}
	return 0;
}

sub readBuffer
{
	my $buffname = $_[0];
	open( IN, "< $buffname" ) || return 2;
	my $line;
	my $tstamp;
	while (<IN>)
	{
		( $line, $tstamp ) = split(':');
		chomp($tstamp);    # remove eventual newline
	}
	close(IN);
	&printLog( 8, "buffer: $buffname. First job: id=$line; timestamp=$tstamp" );
	$_[1] = $line;
	$_[2] = $tstamp;
	return 0;
}

##--------> process the LRMS log and process the jobs <-------##

# process LRMS log from last file (set lastJob and lastTimestamp) to
# startJob (get directory in temporal order and check modification date)

sub processLrmsLogs
{
	my $startJob               = $_[0];
	my $startTimestamp         = $_[1];
	my $lastJob                = $_[2];
	my $lastTimestamp          = $_[3];
	my $ignoreJobsLoggedBefore = $_[4];

	my $currLogTimestamp       = 0;
	my $continueProcessing     = 1;

	while ( $keepGoing && $continueProcessing )
	{

		# first get log files and timestamps:
		my @lrmsLogFiles;
		my %logFInodes = ();
		my %logFSizes  = ();

		my $nothingProcessed =
		  1;    # first assume process nothing in this iteration
		my $allProcessed = 0;

		opendir( DIR, $lrmsLogDir )
		  || &error("Error: can't open dir $lrmsLogDir: $!");
		while ( defined( my $file = readdir(DIR) ) )
		{
			next if ( $file =~ /^\.\.?$/ );    # skip '.' and '..'
			next
			  if ( $lrmsType eq "pbs" && !( $file =~ /^\d{8}(\.gz)?$/ ) );
			next
			  if ( $lrmsType eq "lsf"
				&& !( $file =~ /^lsb\.acct(\.\d*)?(\.gz)?$/ ) );
			next
			  if ( $lrmsType eq "sge"
				&& !( $file =~ /^accounting(\.\d*)?(\.gz)?$/ ) );

			# we accept compressed files as well (but will be able to parse
			# them only if we have the command less, see later)

			push @lrmsLogFiles, $file;

			# keep track of last modification timestamp:

			my (
				$dev,   $ino,     $mode, $nlink, $uid,
				$gid,   $rdev,    $size, $atime, $mtime,
				$ctime, $blksize, $blocks
			);    # these are dummies

			# only inode, size and modification timestamp are interesting!
			(
				$dev,   $logFInodes{$file}, $mode,  $nlink,
				$uid,   $gid,               $rdev,  $logFSizes{$file},
				$atime, $logFMod{$file},    $ctime, $blksize,
				$blocks
			  )
			  = stat("$lrmsLogDir/$file");
		}
		closedir DIR;

		# now we sort the LRMS log files according to their modification
		# timestamp
		my @sortedLrmsLogFiles =
		  ( sort { $logFMod{$a} <=> $logFMod{$b} } keys %logFMod );
		my $thisLogFile;
		# we process these LRMS log files from the last, until we find the
		# last job previously considered.
		while ( $keepGoing && $continueProcessing && @sortedLrmsLogFiles )
		{

			$thisLogFile = shift(@sortedLrmsLogFiles);
			&printLog( 8,
				"LRMS log: $thisLogFile; modified: $logFMod{$thisLogFile}" );

			# if a log file with this inode has already been processed in
			# this iteration ...
			#if (
			#	exists( $processedLogFileInodes{ $logFInodes{$thisLogFile} } ) )
			#{
			#	&printLog( 7,
#"File $thisLogFile with inode $logFInodes{$thisLogFile} already processed in this iteration, skipping!"
#			#	);
#				next;
#			}

			if ( $logFSizes{$thisLogFile} == 0 )
			{
				&printLog( 7, "File is empty, skipping!" );
				next;
			}

			if ( $logFMod{$thisLogFile} < $ignoreJobsLoggedBefore )
			{

				# last modified (appended) _before_ the job we have already
				# processed (or: should be ignored because logged to early)
				# hence we stop here!
				&printLog( 7, "skipping $thisLogFile because too early", 1 );
				next;
			}
			elsif ( $logFMod{$thisLogFile} < $startTimestamp )
			{
				&printLog( 7, "skipping $thisLogFile because already processed",
					1 );
				next;
			}
			else
			{
				&printLog( 8,
					"LRMS log: $thisLogFile; modified: $logFMod{$thisLogFile}"
				);
				if ( $logFModPreviousRun{$thisLogFile} ==
					$logFMod{$thisLogFile} )
				{
					&printLog( 7, "skipping $thisLogFile because not changed since previous run.",
                                        1 );
					next;
				}

				&processLrmsLogFile(
					$thisLogFile,
					$_[0],    # $startJob
					$_[1],    # $startTimestamp
					$_[2],    # $lastJob
					$_[3],    # $lastTimestamp
					$ignoreJobsLoggedBefore,
					$currLogTimestamp,
					$nothingProcessed,
					$allProcessed
				);
				$logFModPreviousRun{$thisLogFile} = $logFMod{$thisLogFile};

				$processedLogFileInodes{ $logFInodes{$thisLogFile} } = 1;

				last;    # quit while loop to reload directory for next file!
				         # (file names might have changed due to logrotate!)
			}
		}

		# if we checked all files and there are none left: stop iteration!
		if ( !@sortedLrmsLogFiles )
		{
			$processedLogFileInodes{ $logFInodes{$thisLogFile} } = 0;
			$continueProcessing = 0;
		}

		# stop iteration also if we either didn't process anything
		# (no new jobs!) or found the last buffer job (processed all new jobs)!
		if (
			$allProcessed
			|| (   $nothingProcessed
				&& ( $_[0] eq $_[2] )
				&& ( $_[1] eq $_[3] ) )
		  )
		{

			# $startJob = $_[0]; $startTimestamp = $_[1];
			# $lastJob = $_[2]; $lastTimestamp = $_[3];
			$continueProcessing = 0;
		}
	}
}

# --- parse a single log file --- #
sub processLrmsLogFile
{

	my $filename = $_[0];

	my $startJob       = $_[1];
	my $startTimestamp = $_[2];

	my $lastJob       = $_[3];
	my $lastTimestamp = $_[4];

	my $ignoreJobsLoggedBefore = $_[5];

	my $currLogTimestamp = $_[6];

	my $nothingProcessed = $_[7];
	my $allProcessed     = $_[8];
	my $recordsCounter   = 0;

	# for each job to process: check the CE accounting log dir (ordered by
	# modification date) for the right log file +/- 1 (according to last
	# modification date) and process it.

	# if the job is found in the CE's accounting log: route 2
	# if not: route 3 (local job)

	# FIXME decide whether to decompress using 'less':
	#if ( $filename =~ /(\.gz)$/ ) {

	#	# decompress and pipe into cat:
	#	$cmd = "$less_cmd $lrmsLogDir/$filename | " . $cmd;
	#}
	#else {
	#
	#	# just use cat:
	#	$cmd = $cmd . " $lrmsLogDir/$filename";
	#}
	#&printLog(9,"Opening with $cmd",1);
	if ( !open( LRMSLOGFILE, "$lrmsLogDir/$filename" ) )
	{
		&printLog( 6, "Warning: Couldn't open the log file ... skipping!" );
		return 1;
	}
	my $firstJobId         = 1;
	my $line               = "";
	my $lrmsEventTimestamp = "";
	my $t1                 = [gettimeofday];
	while ( $line = <LRMSLOGFILE> )
	{
		if ( !$keepGoing )
		{
			&printLog( 6, "Stop processing $filename ..." );
			close LRMSLOGFILE;
			return 1;
		}

		if ( $line !~ /\n$/ )
		{

			# no trailing newline: line still imcomplete, currently being
			# written by the LRMS
			&printLog( 6,
				"Current line not completed by LRMS, skipping: $line" );
			next;
		}

		# not more than a bunch of jobs at a time!
		if ( $jobsThisStep == $jobPerTimeInterval )
		{
			my $secsWaited = 0;
			while ( $keepGoing && $secsWaited < $timeInterval )
			{
				sleep 1;
				$secsWaited++;
			}
			$jobsThisStep = 0;
		}

		# returns an LRMS job ID only if the line contains a finished
		# job
		my $targetJobId =
		  &getLrmsTargetJobId( $lrmsType, $line )
		  ;    #jobId of the record processed within this iteration.

		next
		  if ( $targetJobId eq "" )
		  ;    #jobId not found for some reason. Next iteration.

		# get event time of LRMS log for the job (0 if not found)
		# this is for the buffer! the CE log timestamp will be matched to
		# the LRMS creation time (=submission time)!
		my $lrmsEventTimeString = "";
		$lrmsEventTimestamp =
		  &getLrmsEventTime( $lrmsType, $line, $lrmsEventTimeString );

		if ( $lrmsEventTimestamp == 0 )
		{
			&printLog( 3,
"Error: could not determine LRMS event timestamp! Wrong file format ... ignoring this log file!"
			);
			close LRMSLOGFILE;
			return 1;
		}
		if ( $lrmsEventTimestamp < ( $startTimestamp - 300 ) )
		{
			my $startBuff = localtime($startTimestamp);
			&printLog(
				7,
"Ignoring job $targetJobId with envent time: $lrmsEventTimeString earlier than current buffer timestamp: $startBuff",
				1
			);
			next;
		}

		# get creation time stamp for LRMS job (for matching CE log timestamp)
		my $job_ctime = &getLrmsJobCTime( $lrmsType, $line );

		if ( $job_ctime == 0 )
		{
			&printLog( 3,
"Error: could not determine LRMS job creation/submission timestamp! Wrong file format ... ignoring this log file!"
			);
			close LRMSLOGFILE;
			return 1;
		}

	 #set current record event time stamp and pass it back to the caller method.
		$_[6] = $currLogTimestamp = $lrmsEventTimestamp;

		if ( $lrmsEventTimestamp < ( $ignoreJobsLoggedBefore - 86400 ) )
		{

			&printLog( 4,
"Warning: Log event time $lrmsEventTimeString (=$lrmsEventTimestamp) of job $targetJobId BEFORE $ignoreJobsLoggedBefore! (May happen if ignoreJobsLoggedBefore is set and the buffer contained earlier timestamp). Stopping iteration!"
			);
			close LRMSLOGFILE;
			$_[8] = $allProcessed = 1;
			return 0;
		}
		# The record can be processed:
		
		$_[3] = $lastJob = $targetJobId;   # $lastJob, i.e. newest job processed
			$_[4] = $lastTimestamp =
			  $lrmsEventTimestamp;    # $lastTimestamp, i.e. of newest job
			$firstJobId = 0;

		
		$_[7] = $nothingProcessed = 0;    # processing something!

		&printLog( 6,
"Most recent job to process:$targetJobId; LRMS event time:$lrmsEventTimeString(=$lrmsEventTimestamp)"
		);
		&printLog( 6,
"Processing job: $targetJobId with LRMS log event time(local):$lrmsEventTimeString(=$lrmsEventTimestamp); LRMS creation time: $job_ctime"
		);

		my $gianduiottoHeader;
		if ($useCElog)
		{
			# get grid-related info from CE job map
			$gianduiottoHeader =
			  &parseCeUserMapLog( $targetJobId, $lrmsEventTimestamp,
				$job_ctime );
		}
		else
		{
			# don't use CE job map ... local job!
			$gianduiottoHeader = "JOB_TYPE=local\n";
		}

		# keep track of the number of jobs processed:
		$jobsThisStep++;

		if ( !$processLocalJobs
			&& $gianduiottoHeader =~ /JOB_TYPE=local/ )
		{
			&printLog( 7,
				"Skipping local job (jobsToProcess = \"$jobsToProcess\")!" );
			next;
		}
		elsif ( !$processGridJobs
			&& $gianduiottoHeader =~ /JOB_TYPE=grid/ )
		{
			&printLog( 7,
				"Skipping grid job (jobsToProcess = \"$jobsToProcess\")!" );
			next;
		}

		# eventually add info from LDIF file if this job
		# is not too old and we can assume the current GLUE
		# attributes to be correct:
		if ( $ldifModTimestamp != 0 )
		{
			#should add this control (record being reprocessed):
			#if ($job_ctime >= $ldifModTimestamp) {
			&printLog( 8, "Adding GLUE attributes to UR ..." );
			my $key;
			if ( $configValues{useUrKeyDefFile} ne "yes" )
			{
				foreach $key ( keys(%glueAttributes) )
				{
					$gianduiottoHeader =
					  $gianduiottoHeader . "$key=$glueAttributes{$key}\n";
				}
			}
			else
			{

# if useUrKeyDefFile is set to "yes" use urKeyDef rules to translate glue based info.
				&printLog( 8, "using populateKeyDef..." );
				my @URkeyValues;
				&populateKeyDef( $configValues{urKeyDefFile},
					\@URkeyValues, \%glueAttributes );
				foreach my $line (@URkeyValues)
				{
					&printLog( 8, "added:$line" );
					$gianduiottoHeader = $gianduiottoHeader . "$line\n";
				}
			}
		}
		if ( $keepGoing == 0 )
		{
			&printLog( 3, "Program exiting, termiantion signal received..." );
			last;
		}
		# Producing the record to be inserted in the database.
		if ( &produceRecord( $gianduiottoHeader, $line ) != 0 )
		{
			&printLog( 2,
"Error: could not create UR for job $targetJobId with LRMS event time: $lrmsEventTimeString!"
			);
			next;
			
		}
			$mainRecordsCounter++;
			$recordsCounter++;
			if ( $recordsCounter == 50 )
			{
				#commit transaction and write buffer.
				&printLog( 8, "Commit..." );
				my $commitSuccesfull = 1;
				while ( $keepGoing && $commitSuccesfull == 1 )
				{
					eval { $dbh->commit; };
					if ($@)
					{
						&printLog( 3, "DB Locked:$@" );
					}
					else
					{
						&printLog( 8, "...succesfull" );

						#commit buffer also.
						&putBuffer( $collectorBufferFileName, $targetJobId,
							$lrmsEventTimestamp );
						$commitSuccesfull = 0;
					}
				}
				#database size must not grow, so write up to $maxNumRecords, then wait for pushd to consume them.
				my $shouldWaitFor = 1;
				while ( $shouldWaitFor && $keepGoing )
				{
					if ( &numRecords() > $maxNumRecords )
					{
						&printLog(
							3,
"There are more than $maxNumRecords records in database, waiting $waitFor seconds..",
							1
						);
						my $secsWaited = 0;
						while ( $keepGoing && $secsWaited < $waitFor )
						{
							my $randomSleepTime =
							  0.25 - log( rand() )
							  ;    #0.25 + v.a. exp unilatera, media = 1
							select( undef, undef, undef, $randomSleepTime )
							  ;    #equivale a sleep per valore float
							$secsWaited++;
						}
					}
					else
					{
						$shouldWaitFor = 0;
					}
				}

				my $elapsed      = tv_interval( $t1, [gettimeofday] );
				my $jobs_min     = ( $mainRecordsCounter / $elapsed ) * 60;
				my $min_krecords = 0.0;
				if ( $jobs_min > 0 )
				{
					$min_krecords = 1000.0 / $jobs_min;
				}
				$jobs_min     = sprintf( "%.2f", $jobs_min );
				$min_krecords = sprintf( "%.1f", $min_krecords );
				&printLog( 4,
"Processed: $mainRecordsCounter,Elapsed: $elapsed,Records/min:$jobs_min,min/KRec: $min_krecords"
				);
				if ( $mainRecordsCounter >= 1000 )
				{
					$mainRecordsCounter = 0;
					$t1                 = [gettimeofday];
				}
				#reset record counter for commit step.
				$recordsCounter = 0;
			}

	}    # while (<LRMSLOGFILE>) ...
	#process trailing records still not committed when the log file hits EOF.
	if ( $recordsCounter != 0 )
	{
		my $commitSuccesfull = 1;
		&printLog( 6, "Commit trailing." );
		while ( $keepGoing && $commitSuccesfull == 1 )
		{
			eval { $dbh->commit; };
			if ($@)
			{
				&printLog( 3, "DB Locked:$@" );
				sleep 1;
			}
			else
			{
				&printLog( 6, "...succesfull" );
				#commit buffer also.
						&putBuffer( $collectorBufferFileName, $lastJob,
							$lastTimestamp );
				
				$commitSuccesfull = 0;
			}
		}

	}

	&printLog( 7, "Processed: $filename Exiting.", 1 );
	close(LRMSLOGFILE);
	$allProcessed = 1;
	if ( $keepGoing == 0 )
	{
		&printLog( 6, "Processed trailing and exit." );
		&Exit();
	}
}

##--------> process single Condor job classad <-------##

sub processCondorJobClassad
{
	my $jhString       = $_[0];
	my $targetJobId    = $_[1];
	my $startTimestamp = $_[2];
	my $endTimestamp   = $_[3];

	my $retVal = 0;    # ok

	if ( $endTimestamp == 0 )
	{
		&printLog( 2,
"Error: Cannot process job, because classad don't contain a CompletionDate! Stopping iteration!\n"
		);
		$retVal = 1;
	}
	elsif ( $startTimestamp == 0 )
	{
		&printLog( 2,
"Error: Cannot process job, because classad don't contain a JobStartDate! Stopping iteration!\n"
		);
		$retVal = 2;
	}
	else
	{
		my $gianduiottoHeader;
		if ($useCElog)
		{

			# get grid-related info from CE job map
			$gianduiottoHeader =
			  &parseCeUserMapLog( $targetJobId, $startTimestamp,
				$startTimestamp );
		}
		else
		{

			# don't use CE job map ... local job!
			$gianduiottoHeader = "JOB_TYPE=local\n";
		}

		if ( !$processLocalJobs && $gianduiottoHeader =~ /JOB_TYPE=local/ )
		{
			&printLog( 7,
				"Skipping local job (jobsToProcess = \"$jobsToProcess\")!\n" );
			return $retVal;
		}
		elsif ( !$processGridJobs && $gianduiottoHeader =~ /JOB_TYPE=grid/ )
		{
			&printLog( 7,
				"Skipping grid job (jobsToProcess = \"$jobsToProcess\")!\n" );
			return $retVal;
		}

		# eventually add info from LDIF file if this job
		# is not too old and we can assume the current GLUE
		# attributes to be correct:
		if ( $ldifModTimestamp != 0 )
		{

			# we add this control later! For now do it always!
			#if ($job_ctime >= $ldifModTimestamp) {
			&printLog( 8, "Adding GLUE attributes to UR ...\n" );
			my $key;
			foreach $key ( keys(%glueAttributes) )
			{
				$gianduiottoHeader =
				  $gianduiottoHeader . "$key=$glueAttributes{$key}\n";
			}
		}

		if ( &produceRecord( $gianduiottoHeader, $jhString . "\n" ) != 0 )
		{
			&printLog( 2,
				    ""
				  . localtime()
				  . ": Error: can't create Record for job $targetJobId with CompletionDate: $endTimestamp ("
				  . localtime($endTimestamp)
				  . ")!\n" );

			# EXIT BAD!!! and don't update the buffer!
			close(JOBHIST);
			&error("Stopping urCollector due to unrecoverable error!\n");
		}
	}

	return $retVal;    # ok?
}

##--------> process Condor job history <-------##

# process the Condor job history forward from the start job (startTimestamp)
# to the last (current) job.

sub processCondorJobHistory
{
	my $startJob       = $_[0];
	my $startTimestamp = $_[1];
	my $lastJob        = $_[2];    # we will set these for each processed job!
	my $lastTimestamp  = $_[3];
	my $ignoreJobsLoggedBefore = $_[4];

	# determine timestamp at which to stop, either because of configuration
	# or because of last processed job:
	my $postgresTimeString = "";
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst );

	if ( $startTimestamp != 0 || $ignoreJobsLoggedBefore != 0 )
	{

		if ( $ignoreJobsLoggedBefore > $startTimestamp )
		{

			( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) =
			  localtime($ignoreJobsLoggedBefore);
		}
		else
		{
			( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) =
			  localtime($startTimestamp);
		}

		$postgresTimeString = ""
		  . ( $mon + 1 ) . "/"
		  . $mday . "/"
		  . ( $year + 1900 ) . " "
		  . $hour . ":"
		  . $min . ":"
		  . $sec;
	}

#my $command = "$condorHistoryCommand -completedsince '$postgresTimeString'";
# condor_history -l -backwards -constraint "completiondate > `date -d "$(date+%-m/%-d) 04:00" +%s`" > longlist
#my $command = "$condorHistoryCommand -l -backwards -constraint \"completiondate > `date -d \"$postgresTimeString\"`\"";
	my $command = "$condorHistoryCommand -l ";

	if ( $postgresTimeString ne "" )
	{
		$command .= " -completedsince \"$postgresTimeString\"";
	}

	&printLog( 6, "Getting job history from condor: $command\n" );

	open( JOBHIST, "$command |" )
	  || &error(
		"Fatal Error: couldn't get job history from $condorHistoryCommand!\n");

	my $jhLine           = "";
	my $classadLine      = "";
	my $jhClusterId      = "";
	my $jhCompletionDate = 0;
	my $jhJobStartDate   = 0;

	my $firstJobId = 1;

	while ($keepGoing)
	{

		# not more than a bunch of jobs at a time!
		if ( $jobsThisStep == $jobPerTimeInterval )
		{
			$jobsThisStep = 0;

#print "".localtime().": $jobsThisStep jobs processed. Sleeping for $parserProcessingInterval seconds.\n";
			my $secsWaited = 0;
			while ( $keepGoing && $secsWaited < $timeInterval )
			{
				sleep 1;
				$secsWaited++;
			}
		}

		if ( !( $classadLine = <JOBHIST> ) )
		{

			# this was the last job to process:

			if (   $jhLine ne ""
				&& $jhClusterId ne $startJob
				&& $jhClusterId ne "" )
			{

				# still a job to process
				&printLog( 6,
					"Processing last job with ClusterId=$jhClusterId.\n" );

				if (
					&processCondorJobClassad(
						$jhLine,         $jhClusterId,
						$jhJobStartDate, $jhCompletionDate
					) == 0
				  )
				{

					if ($firstJobId)
					{

						# if we reach the end of the condor history output
						# we can be sure that we will need a final buffer
						# independent of whether this is the first run
						# or not.

						# for updating the buffer:
						$_[2] = $lastJob       = $jhClusterId;    # lastJob
						$_[3] = $lastTimestamp =
						  $jhCompletionDate;    # lastTimestamp
						$firstJobId = 0;
					}

				}

				&printLog( 6,
					"No other jobs in the history. Stopping iteration.\n" );
			}
			elsif ( $jhClusterId ne $startJob && $jhClusterId != 0 )
			{

				# found the last job to process, quit this iteration!
				&printLog( 6,
"Found alredy processed job with ClusterId=$jhClusterId. Stopping iteration ...\n"
				);
			}

			last;

		}
		else
		{

			# having another line from the condor_history output
			chomp($classadLine);

			if ( $classadLine =~
				/No historical jobs in the database match your query/ )
			{
				&printLog( 7, "No new jobs found in history!\n" );
				last;
			}

			# included below ...
			#elsif ($classadLine =~ /^ClusterId\s?=\s?(\d*).*$/) {
			#$jhClusterId = $1;
			#}
			elsif ($classadLine =~ /^(\S+)\s?=\s?\"(.*)\"$/
				|| $classadLine =~ /^(\S+)\s?=\s?(.*)$/ )
			{

			   # something useful like 'ClusterId = 12345' or 'Owner = "cms001"'
				chomp($classadLine);

				$jhLine .= "=====" . $classadLine;

				if ( $1 eq "ClusterId" )
				{
					$jhClusterId = $2;
				}
				elsif ( $1 eq "CompletionDate" )
				{
					$jhCompletionDate = $2;
				}
				elsif ( $1 eq "JobStartDate" )
				{
					$jhJobStartDate = $2;
				}
			}
			elsif ( $classadLine =~ /^\s*$/ )
			{

				# empty or blank line (job separator!)
				# if we had something before we need to process it before
				# continuing with the next classad.

				if ( $jhClusterId eq $startJob && $startJob ne "" )
				{

					# found the last job to process, quit this iteration!

					# write this only if we have processed at least one job!
					# Otherwise it would be written on all empty iterations.
					if (   ( $_[2] ne $startJob )
						|| ( $_[3] ne $startTimestamp ) )
					{
						&printLog( 6,
"Found already processed job with ClusterId =$jhClusterId. Stopping iteration ...\n"
						);
					}

					last;
				}
				else
				{

					if ($firstJobId)
					{

						# for updating the buffer:
						$_[2] = $lastJob       = $jhClusterId;    # lastJob
						$_[3] = $lastTimestamp =
						  $jhCompletionDate;    # lastTimestamp
						$firstJobId = 0;

						&printLog( 6,
"Most recent job to process: $jhClusterId; CompletionDate: $jhCompletionDate\n"
						);
					}

					&printLog( 6,
"Processing job: $jhClusterId with CompletionDate: $jhCompletionDate (="
						  . localtime($jhCompletionDate)
						  . " local time); JobStartDate: $jhJobStartDate\n" );

					if (
						&processCondorJobClassad(
							$jhLine,         $jhClusterId,
							$jhJobStartDate, $jhCompletionDate
						) == 0
					  )
					{

					}

					#else {
					# some slight error ...
					#   last;
					#}

					# keep track of the number of jobs processed:
					$jobsThisStep++;
				}

				#&printLog (7,"Looking for further classads ...\n");
				$jhLine           = "";
				$jhClusterId      = "";
				$jhCompletionDate = 0;
				$jhJobStartDate   = 0;
			}
			else
			{

				# cannot parse this line from the condor_history output
				&printLog( 3,
"Warning: Cannot parse this line from the condor_history output: $classadLine\n"
				);
			}

		}

	}    # while ($keepGoing)

	close(JOBHIST);
}

## --------- parse CE user map log file for accounting --------##
## to find a specific job ...

sub parseCeUserMapLog
{

	my $lrmsJobID     = $_[0];
	my $lrmsTimestamp = $_[1];
	my $job_ctime     = $_[2];

	# important note: the CE's log file might not have the precise timestamp
	# of the job's submission to the LRMS (due to implementation problems)
	# hence the CE log's entry is not necessarily in the (rotated?) log file
	# we expect from the LRMS timestamp

	my $gHeader = "";
	my $isLocal = 0;    # assume it is a grid job with an entry in the CE log

	my @ceLogFiles;

	my %logFMod = ();

	opendir( DIR, $ceJobMapLogDir )
	  || &error("Error: can't open dir $ceJobMapLogDir: $!");
	while ( defined( my $file = readdir(DIR) ) )
	{
		my $fullname = $ceJobMapLogDir . $file;

		#print "CE_LOG_FILE:$fullname ...";

		next if ( $file =~ /^\.\.?$/ );    # skip '.' and '..'
		next
		  if ( !( $fullname =~ /^$ceJobMapLog[\/\-_]?\d{8}(\.gz)?$/ )
			&& !(
				$fullname =~ /^$ceJobMapLog[\/\-_]?\d{4}-\d{2}-\d{2}(\.gz)?$/ )
			&& !( $fullname =~ /^$ceJobMapLog(\.\d*)?(\.gz)?$/ ) )
		  ;    # skip if not like "<logname>(-)20060309(.gz)" (one per day)
		       # skip if not like "<logname>(-)2006-03-09(.gz)" (one per day)
		       # and not like "<logname>.1(.gz)" (rotated)!
		push @ceLogFiles, $file;

		# keep track of last modification timestamp:
		$logFMod{$file} =
		  ( stat("$ceJobMapLogDir/$file") )[9];    #mtime in sec. after epoch
	}
	closedir DIR;

	# now we sort the CE log files according to their modification
	# timestamp
	my @sortedCeLogFiles =
	  ( sort { $logFMod{$b} <=> $logFMod{$a} } keys %logFMod );

	# find up to 3 file names: the log file that should contain the
	# LRMS timestamp; the previous file and the next file (in case the
	# CE log timestamp is not exactly synchronized the CE log entry might
	# end up in a previous or next file):

	my @ceScanLogFiles = ( "", "", "" );
	my %scanDirection;

	# $ceScanLogFiles[0] is the file expected for this timestamp
	# $ceScanLogFiles[1] is the previous file
	# $ceScanLogFiles[2] is the next file

	my $logFile = "";
	while (@sortedCeLogFiles)
	{
		$logFile = shift @sortedCeLogFiles;

		if ( $logFMod{$logFile} < $job_ctime )
		{

			# this is the first file with an earlier timestamp, thus it is the
			# previous file
			$ceScanLogFiles[1] = $logFile;
			$scanDirection{$logFile} = "backward";
			last;
		}
		else
		{

			# as long as we didn't find the previous file, this might be
			# the expected one:
			$ceScanLogFiles[2] = $ceScanLogFiles[0];    # next file
			$scanDirection{ $ceScanLogFiles[2] } = "forward";
			$ceScanLogFiles[0]                   = $logFile;     # expected file
			$scanDirection{ $ceScanLogFiles[0] } = "backward";
		}
	}

	my $scanFile            = "";
	my $keepSearchingCeLogs = 1;
	foreach $scanFile (@ceScanLogFiles)
	{

		last if ( !$keepSearchingCeLogs );

		next if ( $scanFile eq "" );
		&printLog( 7,
			"Scanning CE log $scanFile (last modified=$logFMod{$scanFile})" );

		my $cmd = $cat_cmd;

		# decide whether to decompress using 'less':
		if ( $scanFile =~ /(\.gz)?$/ )
		{

			# decompress and pipe into cat/tac:
			$cmd = "$less_cmd $ceJobMapLogDir/$scanFile | " . $cmd;
		}
		else
		{

			$cmd = $cmd . " $ceJobMapLogDir/$scanFile";
		}

		if ( !open( CELOGFILE, "$cmd |" ) )
		{
			&printLog( 7, "Warning: Couldn't open the log file ... skipping!" );
			next;
		}

		while ( my $line = <CELOGFILE> )
		{
			if ( $line =~ /\s*\"lrmsID=$lrmsJobID\"\s*/ )
			{

				# found something, check timestamp (+/- a day):
				# "timestamp=2006-03-08 12:45:01" or
				# "timestamp=2006/03/08 12:45:01"
				my $ceLogTstamp = 0;
				if ( $line =~
/\s*\"timestamp=(\d{4})[-\/](\d{2})[-\/](\d{2})\s(\d{2}):(\d{2}):(\d{2})\"\s*/
				  )
				{

					# get timestamp for this UTC time!
					my $ceEntryTimestamp = timegm(
						int($6), int($5), int($4),    # ss:mm:hh
						int($3), int($2) - 1, int($1)
					);    # dd-mm-yy
					      # month should be from 0 to 11 => -1 !
					&printLog( 6,
"Found in CE log: lrmsID=$lrmsJobID with timestamp(UTC)=$1-$2-$3 $4:$5:$6 ($ceEntryTimestamp)"
					);

					if (   ( $ceEntryTimestamp > $job_ctime - $TSTAMP_ACC )
						&& ( $ceEntryTimestamp < $job_ctime + $TSTAMP_ACC ) )
					{

						# the timestamp from the CE log is within a day
						# from the LRMS creation timestamp, accept it!

						my $logBuff =
						  "Accepting timestamp from CE log! Parsing entry: ";

# example: "timestamp=2006-03-08 12:45:01" "userDN=/C=IT/O=INFN/OU=Personal Certificate/L=Padova/CN=Alessio Gianelle/Email=alessio.gianelle@pd.infn.it" "userFQAN=/atlas/Role=NULL/Capability=NULL" "userFQAN=/atlas/production/Role=NULL/Capability=NULL" "ceID=grid012.ct.infn.it:2119/jobmanager-lcglsf-infinite" "jobID=https://scientific.civ.zcu.cz:9000/-QcMu-Pfv4qHlp2dFvaj9w" "lrmsID=3639.grid012.ct.infn.it"

						# we already got the timestamp and the lrmsID
						my $userDN    = "";
						my @userFQANs = ();
						my $ceID      = "";
						my $jobID     = "";

						my @fields = split( /\"/, $line );
						my $fld;
						foreach $fld (@fields)
						{
							next if ( $fld =~ /^\s*$/ );    # spaces in between
							if ( $fld =~ /^userDN=(.*)$/ )
							{
								$userDN = $1;
								$logBuff .= "userDN=$userDN; ";
							}
							elsif ( $fld =~ /^userFQAN=(.*)$/ )
							{
								my $fqan = $1;
								if ( !$fqan =~ /^\s*$/ )
								{
									push( @userFQANs, $fqan );
								}
								$logBuff .= "userFQAN=$fqan; ";
							}
							elsif ( $fld =~ /^ceID=(.*)$/ )
							{
								$ceID = $1;
								$logBuff .= "ceID=$ceID; ";
							}
							elsif ( $fld =~ /^jobID=(.*)$/ )
							{
								$jobID = $1;
								$logBuff .= "jobID=$jobID; ";
								if ( $jobID eq "none" || $jobID eq "NONE" )
								{
									$jobID = "";
								}
							}
						}
						&printLog( 7, "$logBuff" );

						# check that we have the minimum info:
						if ( $ceID eq "" )
						{
							&printLog( 6,
"Warning: ceID missing! Considering this as a local job!"
							);
							$isLocal = 1;
						}

						#gliedin FIX
						if ( $jobID eq "" )
						{

#this is a grid job, no jobID is available however. set one that doesn't conflict with outOfBand.
							$jobID =
							  "GRID:" . $lrmsJobID . ":" . $lrmsTimestamp;
						}

						# info on job
						$gHeader = $gHeader
						  . "GRID_JOBID=$jobID\n"
						  . "LRMS_TYPE=$lrmsType\n"
						  . "LRMS_JOBID=$lrmsJobID\n"
						  . "LRMS_EVENTTIME=$lrmsTimestamp\n"
						  . "LRMS_SUBMTIME=$job_ctime\n";

						# info on user
						$gHeader =
						  $gHeader . "USER_DN=$userDN\n" . "USER_FQAN=";
						my $fqans = "";
						my $fq    = "";
						foreach $fq (@userFQANs)
						{
							$fqans .= $fq . ";";
						}
						if ( $fqans ne "" )
						{
							chop($fqans);    # cut last ";"
						}
						$gHeader = $gHeader . $fqans;

						$gHeader = $gHeader . "\n";  # terminate field USER_FQAN

						# info on CE:
						$gHeader =
						  $gHeader . "CE_ID=$ceID\n" . "timeZone=$timeZone\n";
						$keepSearchingCeLogs = 0;
						last;
					}
					elsif ( $ceEntryTimestamp < $job_ctime - $TSTAMP_ACC )
					{

						# the timestamp from the CE log is too low, stop
						# trying to find the job!
						&printLog( 6,
"Timestamp of CE log before LRMS creation time: no job found in CE log: local job!"
						);
						$isLocal             = 1;
						$keepSearchingCeLogs = 0;
						last;
					}
					else
					{
						&printLog( 6,
"Timestamp of CE log after LRMS creation time: job with recycled LRMS ID ... ignoring!"
						);
					}

				}    # if ($line =~ /\s*\"timestamp= ...

			}    # if ($line =~ /\s*\"lrmsID= ...

		}    # while ($line = <CELOGFILE>) ...

		close CELOGFILE;

	}    # foreach ...

	if ( $gHeader eq "" )
	{
		&printLog( 6, "No job found in CE log: local job!" );
		$isLocal = 1;
	}

	if ($isLocal)
	{
		$gHeader = "JOB_TYPE=local\n" . $gHeader;
	}
	else
	{

		# grid job
		$gHeader = "JOB_TYPE=grid\n" . $gHeader;
	}
	return $gHeader;
}

sub produceRecord
{

	my $header       = $_[0];
	my $acctlog      = $_[1];
	my $recordString = $header . "\n";
	$recordString .= "ACCTLOG:$acctlog\n";
	##EX pushd hook from HERE
	if ( &parseRecord($recordString) == 0 )
	{
		my $status = &callAtmClient();
		return $status;
	}
	return 0;
}

## ------ get GLUE attributes From LDIF file --------- ##
sub getGLUEAttributesFromLdif
{
	my $modTStamp = 0;    # returns 0 if no valid attributes found!
	                      # otherwise returns the timestamp of the last
	                      # modification of the file!

	# to be stored in %glueAttributes
	%glueAttributes = ();    # first empty everything

	my @ldifFiles = split( /,/, $ldifDefaultFiles );
	unshift( @ldifFiles, $glueLdifFile );    # first to try!

	my @keys = split( /,/, $keyList );

	if ( !@keys )
	{
		&printLog( 3,
"Warning: No GLUE attributes will be added to usage records (reason: no keyList in configuration file)!"
		);
		return 0;                            # no keys -> no benchmarks
	}

	while (@ldifFiles)
	{
		my $file = shift(@ldifFiles);
		&printLog( 8, "Trying to get GLUE benchmarks from LDIF file: $file" );

		if ( !open( GLUEFILE, "< $file" ) )
		{
			&printLog( 5,
				"Warning: could not open the LDIF file ... skipping!" );
			next;
		}

		my $foundSomething = 0;
		my $line;
		while ( $line = <GLUEFILE> )
		{
			my $key;
			my $logBuff;
			foreach $key (@keys)
			{
				if (   ( $line =~ /^$key:\s?(.*)$/ )
					|| ( $line =~ /^$key=(.*)$/ )
					|| ( $line =~ /^${key}_(.*)$/ ) )
				{

		   # accept stuff like "GlueHostBenchmarkSI00: 955" and
		   # "GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU=955"
		   # "GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU_955"
					$glueAttributes{$key} = $1;
					my $logBuff .= "found: $key=$1; ";
					&printLog( 8, "$logBuff" );
					$foundSomething = 1;
				}
			}
		}

		close(GLUEFILE);

		if ($foundSomething)
		{

			# get timestamp of file:
			$modTStamp = ( stat($file) )[9];    #mtime in sec. after epoch
			last;    # don't check the other files (if any)!
		}

	}

	my $thisTStamp = time();

	if ( $modTStamp == 0 )
	{
		&printLog( 5,
"Warning: No GLUE attributes added to UR (reason: no valid entries in LDIF file(s))!"
		);
		return 0;
	}
	else
	{
		if ( $modTStamp > $thisTStamp - $DEF_LDIF_VALIDITY )
		{

			# accept it for at least a day, even if more recently modified
			$modTStamp = $thisTStamp - $DEF_LDIF_VALIDITY;
		}
	}
	return $modTStamp;
}

sub getGLUEAttributesFromLdap
{
	my $modTStamp = 0;    # returns 0 if no valid attributes found!
	                      # otherwise returns the timestamp of the last
	                      # modification of the file!

	# to be stored in %glueAttributes
	%glueAttributes = ();    # first empty everything
	my $ldapServer = $_[0];
	my $ldapPort   = $_[1];
	my @keys       = split( /,/, $keyList );

	if ( !@keys )
	{
		&printLog( 3,
"Warning: No GLUE attributes will be added to usage records (reason: no keyList in configuration file)!"
		);
		return 0;            # no keys -> no benchmarks
	}

	&printLog( 8,
		"Trying to get GLUE benchmarks from LDAP server: $ldapServer:$ldapPort"
	);

	if (
		!open( GLUEFILE,
"/usr/bin/ldapsearch -LLL -x -h $ldapServer -p $ldapPort  -b o=grid |"
		)
	  )
	{
		&printLog( 5, "Warning: could not open the LDAP server ... skipping!" );
		return 0;
	}

	my $foundSomething = 0;
	my $line;
	while ( $line = <GLUEFILE> )
	{
		my $key;
		my $logBuff;
		foreach $key (@keys)
		{
			if (   ( $line =~ /^$key:\s?(.*)$/ )
				|| ( $line =~ /^$key=(.*)$/ )
				|| ( $line =~ /^${key}_(.*)$/ ) )
			{

		   # accept stuff like "GlueHostBenchmarkSI00: 955" and
		   # "GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU=955"
		   # "GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU_955"
				$glueAttributes{$key} = $1;
				my $logBuff .= "found: $key=$1; ";
				&printLog( 8, "$logBuff" );
				$foundSomething = 1;
			}
		}
	}

	close(GLUEFILE);

	if ($foundSomething)
	{

		# get timestamp of file:
		$modTStamp = time();    #mtime in sec. after epoch
	}
	return $modTStamp;
}

## ------ these are the LRMS type-specific functions ------ ##

# returns "" if the line should be ignored!
sub getLrmsTargetJobId
{
	if ( $_[0] eq "pbs" )
	{
		return &getLrmsTargetJobId_pbs( $_[1] );
	}
	elsif ( $_[0] eq "lsf" )
	{
		return &getLrmsTargetJobId_lsf( $_[1] );
	}
	elsif ( $_[0] eq "sge" )
	{
		return &getLrmsTargetJobId_sge( $_[1] );
	}
	return "";
}

sub getLrmsTargetJobId_pbs
{
	my $jid = "";    # default: line to ignore!

	my @ARRAY = split( " ", $_[0] );
	if ( scalar(@ARRAY) > 1 )
	{
		my @ARRAY2 = split( ";", $ARRAY[1] );
		if ( scalar(@ARRAY2) > 2 && $ARRAY2[1] eq "E" )
		{
			$jid = $ARRAY2[2];    # finished job, return LRMS ID!
		}
	}
	return $jid;                  # line to ignore?
}

sub getLrmsTargetJobId_lsf
{
	my $jid = "";                 # default: line to ignore!

	my @ARRAY = split( " ", $_[0] );

	if ( ( scalar(@ARRAY) > 3 ) && ( $ARRAY[0] eq "\"JOB_FINISH\"" ) )
	{

		# make sure we consider this line only if it is a _single_ line
		my $recordOK = 1;
		if ( $_[0] =~ /^\"JOB_FINISH\".*\"JOB_FINISH\"/ )
		{

			# not a single record (can happen if last line without
			# trailing newline and reading backwards!)
			$recordOK = 0;
		}

		if ($recordOK)
		{
			$jid = $ARRAY[3];    # finished job, return LRMS ID!
		}
		else
		{
			&printLog( 3,
				"Ignoring entry from LSF log file (incomplete): $_[0]" );
		}
	}
	else
	{
		&printLog( 3,
"Warning: LSF log file seems to have a wrong format! Expected: \"JOB_FINISH\" ...; having: $_[0]"
		);
	}

	return $jid;    # line to ignore?
}

# get lrmsid (job_number for SGE) only if there are valid timestamp and end_time>start_time>submission_time; log warning for other cases
sub getLrmsTargetJobId_sge
{

	my $valid_timestamp = "946684800";    # 1 Jan 2000
	my $jid             = "";             # default: line to ignore!

	my @ARRAY = split( ":", $_[0] );

	my $submission_time = $ARRAY[8];
	my $start_time      = $ARRAY[9];
	my $end_time        = $ARRAY[10];

	if ( $submission_time > $valid_timestamp )
	{
		if ( $start_time >= $submission_time )
		{
			if ( $end_time >= $start_time )
			{
				$jid = $ARRAY[5];
			}
			else
			{
				&printLog( 3,
"Warning: SGE end_time < start_time for line: $_[0]; skipping job."
				);
			}
		}
		else
		{
			&printLog( 3,
"Warning: SGE start_time < submission_time for line: $_[0]; skipping job."
			);
		}
	}
	else
	{
		&printLog( 3,
"Warning: SGE submission_time have a wrong format in line: $_[0]; skipping job."
		);
	}

	return $jid;    # line to ignore?
}

# get event time for LRMS log entry: returns 0 if not found
sub getLrmsEventTime
{
	if ( $_[0] eq "pbs" )
	{
		return &getLrmsEventTime_pbs( $_[1], $_[2] );
	}
	elsif ( $_[0] eq "lsf" )
	{
		return &getLrmsEventTime_lsf( $_[1], $_[2] );
	}
	elsif ( $_[0] eq "sge" )
	{
		return &getLrmsEventTime_sge( $_[1], $_[2] );
	}
	return 0;
}

sub getLrmsEventTime_pbs
{

	# Format in PBS log: 03/10/2006 00:03:33;E; ...

	my $eventTimestamp = 0;

	my @ARRAY = split( ";", $_[0] );
	if ( scalar(@ARRAY) > 0 )
	{
		my $sec  = 0;
		my $min  = 0;
		my $hour = 0;
		my $mday = 0;
		my $mon  = 0;
		my $year = 0;

		if ( $ARRAY[0] =~
			/^(\d{2})\/(\d{2})\/(\d{4})\s(\d{2}):(\d{2}):(\d{2})$/ )
		{

			$mon  = int($1) - 1;    # has to be 0 to 11 -> -1 !
			$mday = int($2);
			$year = int($3);
			$hour = int($4);
			$min  = int($5);
			$sec  = int($6);

			$eventTimestamp = timelocal(
				$sec,  $min, $hour,    # ss:mm:hh
				$mday, $mon, $year
			);                         # dd-mm-yy
			$_[1] = $ARRAY[0];
		}
	}

	return $eventTimestamp;
}

sub getLrmsEventTime_lsf
{

	# Format in LSF log: "JOB_FINISH" "6.0" 1140194675 ...

	my $eventTimestamp = 0;

	my @ARRAY = split( " ", $_[0] );
	if ( ( scalar(@ARRAY) > 2 ) && ( $ARRAY[2] =~ /^(\d*)$/ ) )
	{
		$eventTimestamp = int($1);

		$_[1] = $ARRAY[2];
	}

	return $eventTimestamp;
}

sub getLrmsEventTime_sge
{

#  Event time not present on SGE, using end_time
#  Format in SGE log: cybersar:wn5-64.ca.infn.it:cybersar:cybersar002:STDIN:352:sge:0:1254769859:1254812685:1254812685:0:0:0:0:0:0.000000:0:0:0:0:14232:0:0:0.000000:0:0:0:0:135:95:NONE:defaultdepartment:NONE:1:0:0.000000:0.000000:0.000000:-q cybersar:0.000000:NONE:0.000000

	my $eventTimestamp = 0;
	my @ARRAY = split( ":", $_[0] );
	if ( ( scalar(@ARRAY) > 10 ) && ( $ARRAY[10] =~ /^(\d*)$/ ) )
	{
		$eventTimestamp = $ARRAY[10];
	}
	return $eventTimestamp;
}

# get the LRMS creation time for the job: returns 0 if not found:
sub getLrmsJobCTime
{
	if ( $_[0] eq "pbs" )
	{
		return &getLrmsJobCTime_pbs( $_[1] );
	}
	elsif ( $_[0] eq "lsf" )
	{
		return &getLrmsJobCTime_lsf( $_[1] );
	}
	elsif ( $_[0] eq "sge" )
	{
		return &getLrmsJobCTime_sge( $_[1] );
	}
	return 0;
}

sub getLrmsJobCTime_pbs
{
	my $ctime = 0;

	if ( $_[0] =~ /\sctime=(\d*)\s/ )
	{
		$ctime = int($1);
	}

	return $ctime;
}

sub getLrmsJobCTime_lsf
{
	my $ctime = 0;

	# in lsb.acct the creation time is the submitTime, the 8th field
	my @ARRAY = split( " ", $_[0] );
	if ( ( scalar(@ARRAY) > 7 ) && ( $ARRAY[7] =~ /^(\d*)$/ ) )
	{
		$ctime = int($1);
	}

	return $ctime;
}

#  ctime = submission_time in SGE log
#  Format in SGE log: cybersar:wn5-64.ca.infn.it:cybersar:cybersar002:STDIN:352:sge:0:1254769859:1254812685:1254812685:0:0:0:0:0:0.000000:0:0:0:0:14232:0:0:0.000000:0:0:0:0:135:95:NONE:defaultdepartment:NONE:1:0:0.000000:0.000000:0.000000:-q cybersar:0.000000:NONE:0.000000
sub getLrmsJobCTime_sge
{
	my $ctime = 0;
	my @ARRAY = split( ":", $_[0] );
	if ( ( scalar(@ARRAY) > 8 ) && ( $ARRAY[8] =~ /^(\d*)$/ ) )
	{
		$ctime = $ARRAY[8];
	}
	return $ctime;
}

sub errorNoRemoveLock
{
	if ( scalar(@_) > 0 )
	{
		print "$_[0]";
	}
	exit(1);
}

sub date2Timestamp
{
	my $tstamp = 0;
	if ( $_[0] =~ /^(\d{4})[-\/](\d{2})[-\/](\d{2})$/ )
	{

		# get timestamp for this UTC time!
		$tstamp = timegm(
			0, 0, 0,    # ss:mm:hh
			int($3), int($2) - 1, int($1)
		);              # dd-mm-yy
		                # month should be from 0 to 11 => -1 !
	}
	return $tstamp;
}

sub populateKeyDef
{
	my ( $input, $URkeyValues, $glueKeyValuePairs ) = @_;
	my %glueAttributes = %$glueKeyValuePairs;
	my @urKeyDef;
	open( FILE, $input ) || return 1;
	while (<FILE>)
	{
		if (/^\s*UR:(.*)\s*$/)
		{
			&printLog( 8, "fuond $_ - > $1" );
			push( @urKeyDef, $1 );
		}
	}
	close(FILE);
	foreach my $keyDef (@urKeyDef)
	{
		if ( $keyDef =~ /^(.*)\s*=\s*(.*)\s*;\s*(.*)$/ )
		{
			my $urKey     = $1;
			my $glueKey   = $2;
			my $regExp    = $3;
			my $glueValue = $glueAttributes{$glueKey};
			&printLog( "$urKey=$regExp($glueValue)  ->  ", 8 );
			if ( $glueValue =~ /$regExp/ )
			{
				&printLog( 8, "$urKey=$1" );
				push( @$URkeyValues, "$urKey=$1" );
			}
		}
	}
	return 0;
}

sub numRecords
{
	my $result = $dbh->selectall_arrayref(" SELECT count(*) FROM commands");
	$dbh->commit;
	my $recordsNumber = 0;
	if (@$result)
	{
		my $buffer = pop(@$result);
		$recordsNumber = $$buffer[0];
	}
	&printLog( 8, "Records in DB:=$recordsNumber" );
	return $recordsNumber;
}


