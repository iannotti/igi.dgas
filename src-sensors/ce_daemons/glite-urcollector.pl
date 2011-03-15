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

use strict;
use POSIX;
use Text::ParseWords;
use Time::Local;
use File::Basename;
use Sys::Syslog;

# turn off buffering of STDOUT
$| = 1;


my $TSTAMP_ACC = 86400; # timestamps between CE log and LRMS log can
                        # differ for up to a day.
my $DEF_LDIF_VALIDITY = 86400; # assume the GLUE attributes taken from the LDIF
                               # didn't change within the last day.
my $DEF_IGNORE_JOBS_LOGGED_BEFORE = "2009-01-01";
my $DEF_WAITFOR=5;
my $DEF_MAXNUMFILES=10000;
my $DEF_SYSTEMLOGLEVEL=6;


my $sigset = POSIX::SigSet ->new();
my $actionHUP = 
    POSIX::SigAction->new("sigHUP_handler",$sigset,&POSIX::SA_NODEFER);
my $actionInt = 
    POSIX::SigAction->new("sigINT_handler",$sigset,&POSIX::SA_NODEFER);
POSIX::sigaction(&POSIX::SIGHUP, $actionHUP);
POSIX::sigaction(&POSIX::SIGINT, $actionInt);
POSIX::sigaction(&POSIX::SIGTERM, $actionInt);

my $dgasLocation = $ENV{DGAS_LOCATION};
if ( $dgasLocation eq "" )
{
        $dgasLocation = "/usr/";
}

my $configFilePath = "/etc/dgas/dgas_sensors.conf";
my %configValues =
    (
     lrmsType              =>  "",
     pbsAcctLogDir         =>  "",
     lsfAcctLogDir         =>  "",
     sgeAcctLogDir         =>  "",
     condorHistoryCommand  =>  "",
     ceJobMapLog          =>  "",
     useCEJobMap          =>  "yes",
     jobsToProcess        =>  "all",

     keyList               => "GlueHostBenchmarkSF00,GlueHostBenchmarkSI00",
     ldifDefaultFiles      => "",
     glueLdifFile          => "",

     dgasURBox    =>  $dgasLocation . "/var/dgasURbox",
     
     collectorLockFileName => $dgasLocation . "/var/dgas_gianduia_urCollector.lock",
     collectorLogFileName =>  $dgasLocation . "/var/log/dgas_gianduia.log",
     collectorBufferFileName => $dgasLocation . "/var/dgasCollectorBuffer",

     mainPollInterval  => "5",
     timeInterval      =>"5",
     jobPerTimeInterval =>"10",

     ignoreJobsLoggedBefore => $DEF_IGNORE_JOBS_LOGGED_BEFORE,
     maxNumRecords => $DEF_MAXNUMFILES,
     limiterWaitFor => $DEF_WAITFOR,
     systemLogLevel => $DEF_SYSTEMLOGLEVEL,
     useUrKeyDefFile => "no",
     urKeyDefFile => "/etc/dgas/dgas_sensors.conf",
     );


my $onlyOneIteration = 0; # default is run as daemon!
my $useCElog = 1;   # default is use the CE's map: grid job <-> local job
my $processGridJobs = 1; # default, changed through jobsToProcess
my $processLocalJobs = 1; # default, changed through jobsToProcess

# get command line arguments (if any):
my $clinearg = "";
while (@ARGV) {
    $clinearg = shift @ARGV;
    if ($clinearg eq "--nodaemon") {
	$onlyOneIteration = 1; # one iteration then quit! (for cronjobs)
    }
    else {
	# take it as configuration file name
	$configFilePath = $clinearg;
    }
}

my $logType = 0;
my $LOGH;

# Parse configuration file
&parseConf($configFilePath);
&bootstrapLog($configValues{collectorLogFileName});

my $lrmsType = $configValues{lrmsType};
my $pbsAcctLogDir = $configValues{pbsAcctLogDir};
my $lsfAcctLogDir = $configValues{lsfAcctLogDir};
my $sgeAcctLogDir = $configValues{sgeAcctLogDir};
my $condorHistoryCommand = $configValues{condorHistoryCommand};
my $ceJobMapLog = $configValues{ceJobMapLog};
my $useCEJobMap = $configValues{useCEJobMap};
my $jobsToProcess = $configValues{jobsToProcess};
my $keyList = $configValues{keyList};
my $ldifDefaultFiles = $configValues{ldifDefaultFiles};
my $glueLdifFile = $configValues{glueLdifFile};
my $dgasURBox   = $configValues{dgasURBox};
my $collectorLockFileName = $configValues{collectorLockFileName};
my $collectorBufferFileName = $configValues{collectorBufferFileName};
my $mainPollInterval = $configValues{mainPollInterval};
my $timeInterval = $configValues{timeInterval};
my $jobPerTimeInterval = $configValues{jobPerTimeInterval};
my $maxNumFiles = $configValues{maxNumRecords};
my $waitFor = $configValues{limiterWaitFor};
my $systemLogLevel = $configValues{systemLogLevel};

# default: ignore jobs before 2008-01-01 00:00:00 UTC
# check that UR box exists (where the pushd expects the URs), if not create it!
( -d $dgasURBox ) || mkdir $dgasURBox;
chmod 0750, $dgasURBox;

# put lock
if ( &putLock($collectorLockFileName) != 0 ) {
    &errorNoRemoveLock("Fatal Error: Couldn't open lock file! in $collectorLockFileName\nExiting ...");
}
else {
    &printLog (4, "Daemon started. Lock file succesfully created.");
}

if ($jobsToProcess =~ /^grid$/i) {
    $processGridJobs = 1; 
    $processLocalJobs = 0; # only grid jobs
    &printLog (4,"Processing only _grid_ jobs (with entries in ceJobMapLog)!");
}
elsif ($jobsToProcess =~ /^local$/i) {
    $processGridJobs = 0; # only local jobs
    $processLocalJobs = 1;
    &printLog (4,"Processing only _local_ jobs (without entries in ceJobMapLog)!");
}
elsif ($jobsToProcess =~ /^all$/i) {
    $processGridJobs = 1; # default
    $processLocalJobs = 1; # default
    &printLog (4,"Processing _all_ jobs (grid and local)!");
}
else {
    &printLog (3,"Warning: In configuration file: cannot regonize 'jobsToProcess = \"$jobsToProcess\"', possible values are: \"grid\", \"local\" and \"all\". Using default: \"all\"!");
    $processGridJobs = 1; # default
    $processLocalJobs = 1; # default
    &printLog (4, "Processing _all_ jobs (grid and local)!");
}


# ignore old jobs? default is: ignore before $DEF_IGNORE_JOBS_LOGGED_BEFORE
if ($configValues{ignoreJobsLoggedBefore} !~ /^(\d{4})[-\/](\d{2})[-\/](\d{2})$/) {
    &printLog (3, "Attention: Cannot interpret value specified for ignoreJobsLoggedBefore in configuration file: \"$configValues{ignoreJobsLoggedBefore}\" ... using default!");
    $configValues{ignoreJobsLoggedBefore} = $DEF_IGNORE_JOBS_LOGGED_BEFORE;
}

my $ignoreJobsLoggedBefore = &date2Timestamp($configValues{ignoreJobsLoggedBefore});

&printLog (4,"Ignoring jobs whose usage was logged before $configValues{ignoreJobsLoggedBefore} 0:00 am (in UTC => $ignoreJobsLoggedBefore)!");


# check wether to use the CE log and get name of directory:
my $ceJobMapLogDir = "";

if ($useCEJobMap ne "yes" && $useCEJobMap ne "YES") {
    $useCElog = 0;  # don't use it, treat all jobs as local
    &printLog (3, "Warning: Not using the CE's job map log file for retrieving grid-related\ninformation. All jobs treated as local jobs!");

    # must be set if processing _only_ grid jobs or _only_ local jobs
    if (!$processGridJobs || !$processLocalJobs) {
	&error("Error in configuration file: 'jobsToProcess = \"$jobsToProcess\"' requires useCEJobMap and ceJobMapLog to be set!");
    }

}
else {
    if ( -d $ceJobMapLog ) {
	# specified in conf file: directory, not a file
	$ceJobMapLogDir = $ceJobMapLog;
    }
    else {
	# if $ceJobMapLog is a file, not a directory, get the directory:
	$ceJobMapLogDir = dirname($ceJobMapLog)."/";
    }

    # check whether what was specified exists:
    if ( ! -d $ceJobMapLogDir ) {
	&error("Fatal error: directory with ceJobMapLog doesn't exist: '$ceJobMapLog'");
    }
}


my $timeZone = `date +%z`;
chomp($timeZone);
my $domainName = `hostname -d`;
chomp($domainName);

# This is for parsing LDIF files:
my %glueAttributes = ();
my $ldifModTimestamp = 0;

my $keepgoing = 1;
my $jobsThisStep = 0; # used to process only bunchs of jobs, as specified in
                      # the configuration file

# determine the LRMS type we have to treat:
my $lrmsLogDir = "";
if ($lrmsType eq "pbs") {
    $lrmsLogDir = $pbsAcctLogDir;
}
elsif ($lrmsType eq "lsf") {
    $lrmsLogDir = $lsfAcctLogDir;
}
elsif ($lrmsType eq "condor") {
    # nothing to do 
}
elsif ($lrmsType eq "sge") {
    $lrmsLogDir = $sgeAcctLogDir;
}
elsif ($lrmsType eq "") {
    &error("Error: LRMS type not specified in configuration file!");
}
else {
    &error("Error: Unknown LRMS type specified in configuration file!");
}

if ($lrmsType ne "condor") {
	# check LRMS log directory:
	if (! -d $lrmsLogDir || ! -r $lrmsLogDir || ! -x $lrmsLogDir) {
    		&error("Error: Directory for LRMS log ($lrmsLogDir) cannot be accessed!");
	}
}
else { # Condor
    # try to find executable:
    if ($condorHistoryCommand eq "") {
        &printLog (4, "Condor history command not specified in configuration file, trying to find \$CONDOR_LOCATION/bin/condor_history:");
        # not specified in conf file, try default:
        $condorHistoryCommand = "$ENV{CONDOR_LOCATION}/bin/condor_history";
        if (! -e $condorHistoryCommand ) {
            &printLog (4, print "$condorHistoryCommand not found, trying 'which condor_history'.");
            $condorHistoryCommand = `which condor_history`;
        }
    }
    if (! -e $condorHistoryCommand || ! -x $condorHistoryCommand) {
        &error("Error: Cannot find executable for retrieving condor history: '$condorHistoryCommand'");
    }
}

# append LRMS type to buffer file name:
$collectorBufferFileName = $collectorBufferFileName.".".$lrmsType;


# check whether we can use the 'less' command (should be present on each
# Linux system ...) this is usefull for gzipped log files!
my $have_less = 1;
my $less_cmd = `which less`;
chomp($less_cmd);
if ( ! -x $less_cmd ) {
    $have_less = 0;
}

# this is used for parsing log files backwards ...
my $have_tac = 1;
my $tac_cmd = `which tac`;
chomp($tac_cmd);
if ( ! -x $tac_cmd ) {
    $have_tac = 0;
}

# this is used for parsing log file forwards ...
my $have_cat = 1;
my $cat_cmd = `which cat`;
chomp($cat_cmd);
if ( ! -x $cat_cmd ) {
    $have_cat = 0;
}

if (!$have_tac || !$have_cat) {
    &error("Error: commands 'cat' and 'tac' required for parsing log files! Quitting!");
}

if (!$have_less) {
    &printLog (3,"Warning: command 'less' required for parsing compressed log files! Compressed log files will be skipped!");
}


# first get info on last job processed:
my $startJob = "";
my $startTimestamp = 0;
my $lastJob = "";
my $lastTimestamp = 0;
my $tmpBuffTstamp = 0;
if (&readBuffer($collectorBufferFileName, $startJob, $startTimestamp) != 0) {
    # we have no regular buffer containing the start Job
    # look for a temporary containing the last jobt + temporary timestamp!
    if (&readTmpBuffer($collectorBufferFileName, $lastJob, $lastTimestamp,
		       $tmpBuffTstamp) != 0) {
	# this is the very first run, not even a temporary buffer is written:
	$tmpBuffTstamp = -1;  # will later be set to first log event timestamp
	                     # encountered!
    }
}

MAIN: while ($keepgoing) {

    if ( &numFiles($dgasURBox) > $maxNumFiles )
    {
        &printLog(3, "There are more than $maxNumFiles in $dgasURBox, waiting $waitFor seconds.");
	my $secsWaited = 0;
	while ($keepgoing && $secsWaited < $waitFor) {
	    sleep 1;
	    $secsWaited++;
	}
	next MAIN;
    }
    if ($onlyOneIteration) {
	print "".localtime().": Not run as a daemon. Executing a single iteration!";
    }

    # see whether the GLUE attributes are available and have changed
    $ldifModTimestamp = &getGLUEAttributesFromLdif();

    # this is the main processing part:
    if ($lrmsType ne "condor") {
	# process LRMS log from last file to startJob
	&processLrmsLogs($startJob, $startTimestamp, $lastJob, $lastTimestamp,
			$tmpBuffTstamp, $ignoreJobsLoggedBefore);
    }
    else
    {
	# process Condor history starting from startJob
	&processCondorJobHistory($startJob, $startTimestamp, $lastJob,
			$lastTimestamp, $tmpBuffTstamp,
			$ignoreJobsLoggedBefore);
    }

    # write buffer (lastJob and lastTimestamp), if necessary!
    if ( $keepgoing && ($lastJob ne "") && ($lastJob ne $startJob) ) {
	&printLog (5,"Processed backwards from $lastJob (timestamp $lastTimestamp) to $startJob (timestamp $startTimestamp). Updating buffer $collectorBufferFileName.");

	&putBuffer($collectorBufferFileName, $lastJob, $lastTimestamp);
	$tmpBuffTstamp = 0;

	$startJob = $lastJob;
	$startTimestamp = $lastTimestamp;
    }

    if ($onlyOneIteration) {
	# not run as a daemon, stop after first round!
	print "".localtime().": Not run as a daemon. Single iteration completed!";
	$keepgoing = 0;
    }

    if ($keepgoing) {
	# print "".localtime().": Waiting for new jobs to finish. Sleeping for $mainPollInterval seconds.";
	#sleep $mainPollInterval;
	my $secsWaited = 0;
	while ($keepgoing && $secsWaited < $mainPollInterval) {
	    sleep 1;
	    $secsWaited++;
	}
    }

}

&printLog (7,"Exiting...");
if ( &delLock($collectorLockFileName) != 0 ) {	
    &printLog (2,"Error removing lock file.");
}
else {
    &printLog (7,"Lock file removed.");
}
&printLog (4,"Exit.");

exit(0);

#### ------------------------ END OF MAIN PART ------------------------ ####


#### ------------------------ Functions: ------------------------ ####


#### parseConf ####
sub parseConf {
    my $fconf = $_[0];
    open(FILE, "$fconf") || &errorNoRemoveLock("Error: Cannot open configuration file $fconf");
    while(<FILE>) {
	if (/\$\{(.*)\}/)
	{
		my $value=$ENV{$1};
		s/\$\{$1\}/$value/g;
	}
	if(/^lrmsType\s*=\s*\"(.*)\"$/){$configValues{lrmsType}=$1;}
	if(/^pbsAcctLogDir\s*=\s*\"(.*)\"$/){$configValues{pbsAcctLogDir}=$1;}
	if(/^lsfAcctLogDir\s*=\s*\"(.*)\"$/){$configValues{lsfAcctLogDir}=$1;}
	if(/^sgeAcctLogDir\s*=\s*\"(.*)\"$/){$configValues{sgeAcctLogDir}=$1;}
	if(/^condorHistoryCommand\s*=\s*\"(.*)\"$/){$configValues{condorHistoryCommand}=$1;}
	if(/^ceJobMapLog\s*=\s*\"(.*)\"$/){$configValues{ceJobMapLog}=$1;}
	if(/^useCEJobMap\s*=\s*\"(.*)\"$/){$configValues{useCEJobMap}=$1;}
	if(/^jobsToProcess\s*=\s*\"(.*)\"$/){$configValues{jobsToProcess}=$1;}

	if(/^keyList\s*=\s*\"(.*)\"$/){$configValues{keyList}=$1;}
	if(/^ldifDefaultFiles\s*=\s*\"(.*)\"$/){$configValues{ldifDefaultFiles}=$1;}
	if(/^glueLdifFile\s*=\s*\"(.*)\"$/){$configValues{glueLdifFile}=$1;}

	if(/^dgasURBox\s*=\s*\"(.*)\"$/){$configValues{dgasURBox}=$1;}

	if(/^collectorLockFileName\s*=\s*\"(.*)\"$/){$configValues{collectorLockFileName}=$1;}
	if(/^collectorLogFileName\s*=\s*\"(.*)\"$/){$configValues{collectorLogFileName}=$1;}
	if(/^collectorBufferFileName\s*=\s*\"(.*)\"$/){$configValues{collectorBufferFileName}=$1;}
	if(/^mainPollInterval\s*=\s*\"(.*)\"$/){$configValues{mainPollInterval}=$1;}
	if(/^timeInterval\s*=\s*\"(.*)\"$/){$configValues{timeInterval}=$1;}
	if(/^jobPerTimeInterval\s*=\s*\"(.*)\"$/){$configValues{jobPerTimeInterval}=$1;}
	if(/^ignoreJobsLoggedBefore\s*=\s*\"(.*)\"$/){$configValues{ignoreJobsLoggedBefore}=$1;}
	if(/^limiterWaitFor\s*=\s*\"(.*)\"$/){$configValues{limiteriWaitFor}=$1;}
	if(/^maxNumRecords\s*=\s*\"(.*)\"$/){$configValues{maxNumRecords}=$1;}
	if(/^systemLogLevel\s*=\s*\"(.*)\"$/){$configValues{systemLogLevel}=$1;}
	if(/^useUrKeyDefFile\s*=\s*\"(.*)\"$/){$configValues{useUrKeyDefFile}=$1;}
	if(/^urKeyDefFile\s*=\s*\"(.*)\"$/){$configValues{urKeyDefFile}=$1;}
    }
    close(FILE);
}

##-------> lock  subroutines <---------##
sub putLock {
    my $lockName = $_[0];
    open(IN,  "< $lockName") && return 1;
    close(IN);
    open(OUT, "> $lockName") || return 2;
    print OUT  $$;    ## writes pid   
    close(OUT);
    return 0;
}

sub delLock {
    my $lockName = $_[0];
    open(IN,  "< $lockName") || return 1;
    close(IN);
    my $status = system("rm -f $lockName");
    return $status;
}

##--------> routines for job processing buffer <---------##
sub putBuffer {
    # arguments are: 0 = buffer name
    #                1 = last LRMS job id
    #                2 = last LRMS job timestamp (log time)
    my $buffName = $_[0];
    if ($keepgoing == 1) {
	# this is done only if no SIGINT was received!
	#print "".localtime().": Writing info on last processed job in buffer $buffName.";
	open(OUT, "> $buffName") || return 2;
	print OUT  "$_[1]:$_[2]";
	close(OUT);

	my $tmpBuffer = "${buffName}_tmp";
	if ( -e $tmpBuffer ) {
	    print "Removing temporary buffer $tmpBuffer ... not required anymore!";
	    `rm -f $tmpBuffer &> /dev/null`;
	}
    }
    return 0;
}


sub readBuffer {
	my $buffname = $_[0];
	open(IN, "< $buffname") || return 2;
	my $line;
	my $tstamp;
	while ( <IN> )
	{
		($line,$tstamp) = split(':');
		chomp($tstamp); # remove eventual newline
	}	
	close(IN);
	&printLog (5, "Reading buffer $buffname. First job to analyse: id=$line; log timestamp=$tstamp");
	$_[1] = $line;
	$_[2] = $tstamp;
	return 0;
}

sub putTmpBuffer {
    # arguments are: 0 = buffer name
    #                1 = last LRMS job id (to go into 
    #                2 = last LRMS job timestamp (log time)
    #                3 = temporary LRMS job timestamp (log time)
    my $buffName = $_[0]."_tmp";
    open(OUT, "> $buffName") || return 2;
    print OUT  "$_[1]:$_[2]:$_[3]";
    close(OUT);
    &printLog (6,"Writing temporary buffer $buffName with '$_[1]:$_[2]:$_[3]'");
    return 0;
}

sub readTmpBuffer {
	my $buffname = $_[0]."_tmp";
	open(IN, "< $buffname") || return 2;
	my $tstamp;
	my $startJobId = "";
	my $startJobTstamp = 0;
	my $tmpJobTstamp = 0;
	while ( <IN> )
	{
		($startJobId,$startJobTstamp,$tmpJobTstamp) = split(':');
		chomp($tmpJobTstamp); # remove eventual newline
	}	
	close(IN);
	&printLog (6,"Reading TEMPORARY buffer $buffname (service stopped before completing the first run). First job to analyse: id=$startJobId; log timestamp=$startJobTstamp. So far processed backwards from there to timestamp: $tmpJobTstamp");
	$_[1] = $startJobId;
	$_[2] = $startJobTstamp;
	$_[3] = $tmpJobTstamp;
	return 0;
}

##-------> sig handlers subroutines <---------##
sub sigHUP_handler {
        &printLog (2,"Got SIGHUP!");
	$keepgoing = 1;
}

sub sigINT_handler {
	&printLog (2,"Got SIGINT!");
        $keepgoing = 0;
}

##--------> process the LRMS log and process the jobs <-------##

# process LRMS log from last file (set lastJob and lastTimestamp) to 
# startJob (get directory in temporal order and check modification date)

sub processLrmsLogs {
    my $startJob = $_[0];
    my $startTimestamp = $_[1];
    my $lastJob = $_[2];
    my $lastTimestamp = $_[3];
    my $tmpBuffTstamp = $_[4];
    my $ignoreJobsLoggedBefore = $_[5];

    my $currLogTimestamp = 0;
    my $continueProcessing = 1;
    my $newestFile = 1; # the first file we open is the current file
    my %processedLogFileInodes = ();

    while ($keepgoing && $continueProcessing) {

	# first get log files and timestamps:
	my @lrmsLogFiles;
	my %logFMod = ();
	my %logFInodes = ();
	my %logFSizes = ();

	my $nothingProcessed = 1; # first assume process nothing in this iteration
	my $allProcessed = 0;

	opendir(DIR, $lrmsLogDir) || &error("Error: can't open dir $lrmsLogDir: $!");
	while( defined(my $file = readdir(DIR)) ) {
	    next if ( $file =~ /^\.\.?$/ ); # skip '.' and '..' 
	    next if ( $lrmsType eq "pbs" && !($file =~ /^\d{8}(\.gz)?$/) );
	    next if ( $lrmsType eq "lsf" && !($file =~ /^lsb\.acct(\.\d*)?(\.gz)?$/) );
	    next if ( $lrmsType eq "sge" && !($file =~ /^accounting(\.\d*)?(\.gz)?$/) );
	    # we accept compressed files as well (but will be able to parse
	    # them only if we have the command less, see later)

	    push @lrmsLogFiles, $file;

	    # keep track of last modification timestamp:

	    my ($dev, $ino, $mode, $nlink, $uid, $gid, $rdev,
		$size,
	        $atime, $mtime, $ctime, $blksize, $blocks); # these are dummies

	    # only inode, size and modification timestamp are interesting!
	    ($dev, $logFInodes{$file}, $mode, $nlink, $uid, $gid, $rdev,
	     $logFSizes{$file},
	     $atime, $logFMod{$file}, $ctime, $blksize, $blocks)
	        = stat("$lrmsLogDir/$file");
	}
	closedir DIR;

	# now we sort the LRMS log files according to their modification
	# timestamp
	my @sortedLrmsLogFiles
	    = (sort{ $logFMod{$b} <=> $logFMod{$a} } keys %logFMod);

	# we process these LRMS log files from the last, until we find the
	# last job previously considered.
	while ($keepgoing && $continueProcessing && @sortedLrmsLogFiles) {

	    my $thisLogFile = shift (@sortedLrmsLogFiles);
	    &printLog ( 5,"LRMS log file: $thisLogFile; last modified: $logFMod{$thisLogFile}");

	    # if this is not the first (newest) log file we read in this
	    # iteration ($currLogTimestamp != 0), make sure we will read only
	    # a log file that contains entries up to the current timestamp! 
	    next if ($currLogTimestamp != 0
		     && $logFMod{$thisLogFile} > $currLogTimestamp);

	    # if a log file with this inode has already been processed in
	    # this iteration ...
	    if (exists($processedLogFileInodes{$logFInodes{$thisLogFile}})) {
		&printLog (7, "File with inode $logFInodes{$thisLogFile} already processed in this iteration, skipping!");
		next;
	    }

	    if ($logFSizes{$thisLogFile} == 0) {
		&printLog (7,"File is empty, skipping!");
		next;
	    }

	    if ( $logFMod{$thisLogFile} < $startTimestamp
		 || $logFMod{$thisLogFile} < $ignoreJobsLoggedBefore ) {
		# last modified (appended) _before_ the job we have already
		# processed (or: should be ignored because logged to early)
		# hence we stop here!
		$continueProcessing = 0;
	    }
	    else {
		&printLog (5, "LRMS log file: $thisLogFile; last modified: $logFMod{$thisLogFile}");

		&processLrmsLogFile($thisLogFile, $newestFile,
				    $_[0], $_[1], $_[2], $_[3], $_[4],
				    $ignoreJobsLoggedBefore, $currLogTimestamp,
				    $nothingProcessed, $allProcessed);

		$processedLogFileInodes{$logFInodes{$thisLogFile}} = 1;

		last; # quit while loop to reload directory for next file!
		      # (file names might have changed due to logrotate!)
	    }
	}

	# if we checked all files and there are none left: stop iteration!
	if (!@sortedLrmsLogFiles) {
	    $continueProcessing = 0;
	}

	# stop iteration also if we either didn't process anything
	# (no new jobs!) or found the last buffer job (processed all new jobs)!
	if ( $allProcessed ||
	    ( $nothingProcessed &&  ($_[0] eq $_[2]) && ($_[1] eq $_[3]) )
	    ) {
	    # $startJob = $_[0]; $startTimestamp = $_[1];
	    # $lastJob = $_[2]; $lastTimestamp = $_[3];
	    $continueProcessing = 0;
	}
    }
}

# --- parse a single log file --- #
sub processLrmsLogFile {

    my $filename = $_[0];
    # my $newestF = $_[1];

    my $startJob = $_[2];
    my $startTimestamp = $_[3];

    my $lastJob = $_[4];
    my $lastTimestamp = $_[5];

    my $tmpBuffTstamp = $_[6];
    my $ignoreJobsLoggedBefore = $_[7];

    my $currLogTimestamp = $_[8];

    my $nothingProcessed = $_[9];
    my $allProcessed = $_[10];

    # for each job to process: check the CE accounting log dir (ordered by
    # modification date) for the right log file +/- 1 (according to last
    # modification date) and process it from backwards.

    # if the job is found in the CE's accounting log: route 2
    # if not: route 3 (local job)

    # building command to open the log file
    my $cmd = $tac_cmd;
    # decide whether to decompress using 'less':
    if ($filename =~ /(\.gz)$/) {
	# decompress and pipe into tac:
	$cmd = "$less_cmd $lrmsLogDir/$filename | ".$cmd;
    }
    else {
	# just use tac:
	$cmd = $cmd." $lrmsLogDir/$filename";
    }
    if ( !open ( LRMSLOGFILE, "$cmd |" ) ) {
	&printLog (6,"Warning: Couldn't open the log file ... skipping!");
	return 1;
    }
    my $firstJobId = 1;
    my $line = "";
    while ($line = <LRMSLOGFILE>) {
	if (!$keepgoing) {
	    &printLog (6,"Stop processing $filename ...");
	    close LRMSLOGFILE;
	    return 1;
	}

	if ($line !~ /\n$/) {
	    # no trailing newline: line still imcomplete, currently being
	    # written by the LRMS
	    &printLog(6, "Current line not completed by LRMS, skipping: $line");
	    next;
	}

	# not more than a bunch of jobs at a time!
	if ($jobsThisStep == $jobPerTimeInterval) {
	    #print "".localtime().": $jobsThisStep jobs processed. Sleeping for $timeInterval seconds.";
	    #sleep $timeInterval;
	    my $secsWaited = 0;
	    while ($keepgoing && $secsWaited < $timeInterval) {
		sleep 1;
		$secsWaited++;
	    }
	    $jobsThisStep = 0;
	}
	# not here, but only when UR file created: $jobsThisStep++;

	# returns an LRMS job ID only if the line contains a finished
	# job
	my $targetJobId = &getLrmsTargetJobId($lrmsType, $line);

	next if ($targetJobId eq "");

	# get event time of LRMS log for the job (0 if not found)
	# this is for the buffer! the CE log timestamp will be matched to 
	# the LRMS creation time (=submission time)!
	my $lrmsEventTimeString = "";
	my $lrmsEventTimestamp =
	    &getLrmsEventTime($lrmsType, $line, $lrmsEventTimeString);

	if($lrmsEventTimestamp == 0) {
	    &printLog (3,"Error: could not determine LRMS event timestamp! Wrong file format ... ignoring this log file!");
	    close LRMSLOGFILE;
	    return 1;
	}

	# get creation time stamp for LRMS job (for matching CE log timestamp)
	my $job_ctime = &getLrmsJobCTime($lrmsType, $line);

	if ($job_ctime == 0) {
	    &printLog (3,"Error: could not determine LRMS job creation/submission timestamp! Wrong file format ... ignoring this log file!");
	    close LRMSLOGFILE;
	    return 1;
	}

	$_[8] = $currLogTimestamp = $lrmsEventTimestamp;

	if ( $tmpBuffTstamp == 0 && # do this only if we have a regular buffer!
		($targetJobId eq $startJob) && ($startJob ne "") &&
		($lrmsEventTimestamp eq $startTimestamp) && ($startTimestamp ne "")
		) {

	    # write this only if we have processed at least one job!
	    # Otherwise it would be written on all empty iterations.
	    if ( ($_[4] ne $startJob) || ($_[5] ne $startTimestamp)) {
		&printLog (5,"Found already processed $targetJobId with log event time $lrmsEventTimeString (=$lrmsEventTimestamp) in log! Done with iteration!");
	    }
	    close LRMSLOGFILE;
	    $_[10] = $allProcessed = 1;
	    return 0;
	}
	elsif ($lrmsEventTimestamp < $ignoreJobsLoggedBefore) {

	    &printLog (4,"Warning: Log event time $lrmsEventTimeString (=$lrmsEventTimestamp) of job $targetJobId BEFORE $ignoreJobsLoggedBefore!\n(Mmay happen if ignoreJobsLoggedBefore is set and the buffer contained earlier timestamp)\nStopping iteration!");
	    close LRMSLOGFILE;
	    $_[6] = $tmpBuffTstamp = 0;
	    $_[10] = $allProcessed = 1;
	    return 0;
	}
	else {
	    # need to process the job:
	    if ($tmpBuffTstamp < 0) {
		# this is the very first run, we don't even have a temporary 
		# buffer; make sure we start writing temporary buffers until
		# the first iteration is completed:
		&printLog (7,"This is the very first record. Writing a temp buffer for backward processing until first iteration is completed!");
		$tmpBuffTstamp = $lrmsEventTimestamp;
	    
	    }
	    elsif ($tmpBuffTstamp > 0) {
		# we have a temporary buffer ... process that before creating a
		# new buffer!
		$firstJobId = 0;
	    }

	    # new buffer information should be taken only if we started from
	    # a regular buffer file, not a temporary one!
	    if ( $firstJobId && $_[1] ) {    # $_[1] = $newestF

		$_[4] = $lastJob = $targetJobId; # $lastJob, i.e. newest job processed
		$_[5] = $lastTimestamp = $lrmsEventTimestamp; # $lastTimestamp, i.e. of newest job
		$firstJobId = 0;
		$_[1] = 0; # $newestF; next file (if any) isn't the newest

		&printLog (6, "This is the most recent job to process: $targetJobId; LRMS log event time: $lrmsEventTimeString (=$lrmsEventTimestamp)");
	    }

	    if ($tmpBuffTstamp > 0 && $lrmsEventTimestamp > $tmpBuffTstamp) {
		&printLog (6,"Skipping $targetJobId (event log timestamp $lrmsEventTimestamp is after $tmpBuffTstamp in temporary buffer)");
		next;
	    }

	    &printLog (6,"Processing job: $targetJobId with LRMS log event time(local):$lrmsEventTimeString(=$lrmsEventTimestamp); LRMS creation time: $job_ctime");

	    $_[9] = $nothingProcessed = 0; # processing something!

	    my $gianduiottoHeader;
	    if ($useCElog) {
		# get grid-related info from CE job map
		$gianduiottoHeader =  &parseCeUserMapLog($targetJobId,
							 $lrmsEventTimestamp,
							 $job_ctime);
	    }
	    else {
		# don't use CE job map ... local job!
		$gianduiottoHeader = "JOB_TYPE=local\n";
	    }
            # keep track of the number of jobs processed:
            $jobsThisStep++;

	    if (!$processLocalJobs && $gianduiottoHeader =~ /JOB_TYPE=local/) {
		&printLog (7,"Skipping this local job (jobsToProcess = \"$jobsToProcess\")!");
		next;
	    }
	    elsif (!$processGridJobs && $gianduiottoHeader =~ /JOB_TYPE=grid/) {
		&printLog (7,"Skipping this grid job (jobsToProcess = \"$jobsToProcess\")!");
		next;
	    }

	    # eventually add info from LDIF file if this job
	    # is not too old and we can assume the current GLUE
	    # attributes to be correct:
	    if ($ldifModTimestamp != 0) {
		#should add this control: 
		#if ($job_ctime >= $ldifModTimestamp) {
	        &printLog (8,"Adding GLUE attributes to UR ...");
	        my $key;
		if ( $configValues{useUrKeyDefFile} ne "yes" )
		{
		        foreach $key (keys(%glueAttributes)) {
				$gianduiottoHeader = $gianduiottoHeader."$key=$glueAttributes{$key}\n";
			}
		}
		else
		{
	    		# if useUrKeyDefFile is set to "yes" use urKeyDef rules to translate glue based info. 
	        	&printLog (8,"using populateKeyDef...");
			my @URkeyValues;
			&populateKeyDef ( $configValues{urKeyDefFile}, \@URkeyValues, \%glueAttributes );				
			foreach my $line ( @URkeyValues )
			{
	        		&printLog (8,"added:$line");
				$gianduiottoHeader = $gianduiottoHeader."$line\n";
			}
		}
	    }
	    if (&writeGianduiottoFile($targetJobId, $lrmsEventTimestamp,
				      $gianduiottoHeader, $line)
		!= 0) {
		&printLog (2,"Error: could not create UR file in $dgasURBox for job $targetJobId with LRMS event time: $lrmsEventTimeString!");

		# EXIT BAD!!! don't write an updated buffer!
		close (LRMSLOGFILE);
		&error("Stopping urCollector due to unrecoverable error!\n");
	    }
	    else {
		if ($tmpBuffTstamp != 0) {
		    # in case this is the first run, adjust temporary buffer!
		    $_[6] = $tmpBuffTstamp = $lrmsEventTimestamp;

		    &putTmpBuffer($collectorBufferFileName, $lastJob,
				  $lastTimestamp, $tmpBuffTstamp);
		}
	    }
	}
    }  # while (<LRMSLOGFILE>) ...
    close (LRMSLOGFILE);
}

##--------> process single Condor job classad <-------##

sub processCondorJobClassad {
    my $jhString = $_[0];
    my $targetJobId = $_[1];
    my $startTimestamp = $_[2];
    my $endTimestamp = $_[3];

    my $retVal = 0; # ok

    if ($endTimestamp == 0) {
        &printLog (2,"Error: Cannot process job, because classad didn't contain a CompletionDate! Stopping iteration!\n");
        $retVal = 1;
    }
    elsif ($startTimestamp == 0) {
        &printLog (2,"Error: Cannot process job, because classad didn't contain a JobStartDate! Stopping iteration!\n");
        $retVal = 2;
    }
    else {
        my $gianduiottoHeader;
        if ($useCElog) {
            # get grid-related info from CE job map
            $gianduiottoHeader =  &parseCeUserMapLog($targetJobId,
                                                     $startTimestamp,
                                                     $startTimestamp);
        }
        else {
            # don't use CE job map ... local job!
            $gianduiottoHeader = "JOB_TYPE=local\n";
        }

        if (!$processLocalJobs && $gianduiottoHeader =~ /JOB_TYPE=local/) {
            &printLog (7,"Skipping this local job (jobsToProcess = \"$jobsToProcess\")!\n");
            return $retVal;
        }
        elsif (!$processGridJobs && $gianduiottoHeader =~ /JOB_TYPE=grid/) {
            &printLog (7,"Skipping this grid job (jobsToProcess = \"$jobsToProcess\")!\n");
            return $retVal;
        }

	# eventually add info from LDIF file if this job
        # is not too old and we can assume the current GLUE
        # attributes to be correct:
        if ($ldifModTimestamp != 0) {
            # we add this control later! For now do it always!
            #if ($job_ctime >= $ldifModTimestamp) {
                &printLog (8,"Adding GLUE attributes to UR ...\n");
                my $key;
                foreach $key (keys(%glueAttributes)) {
                    $gianduiottoHeader = $gianduiottoHeader."$key=$glueAttributes{$key}\n";
                }
        }

        if (&writeGianduiottoFile($targetJobId, $endTimestamp,
                                  $gianduiottoHeader, $jhString."\n")
            != 0) {
            &printLog (2,"".localtime().": Error: could not create UR file in $dgasURBox for job $targetJobId with CompletionDate: $endTimestamp (".localtime($endTimestamp).")!\n");
            # EXIT BAD!!! and don't update the buffer!
            close(JOBHIST);
            &error("Stopping urCollector due to unrecoverable error!\n");
        }
    }

    return $retVal; # ok?
}


##--------> process Condor job history <-------##

# process the Condor job history forward from the start job (startTimestamp)
# to the last (current) job.

sub processCondorJobHistory {
    my $startJob = $_[0];
    my $startTimestamp = $_[1];
    my $lastJob = $_[2];          # we will set these for each processed job!
    my $lastTimestamp = $_[3];
    my $tmpBuffTstamp = $_[4];
    my $ignoreJobsLoggedBefore = $_[5];

    # determine timestamp at which to stop, either because of configuration
    # or because of last processed job:
    my $postgresTimeString = "";
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);

    if ($startTimestamp != 0 || $ignoreJobsLoggedBefore != 0) {

        if ($ignoreJobsLoggedBefore > $startTimestamp) {

            ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                                          localtime($ignoreJobsLoggedBefore);
        }
        else {
            ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                                                  localtime($startTimestamp);
        }

	$postgresTimeString = "".($mon+1)."/".$mday."/".($year+1900)." ".$hour.":".$min.":".$sec;
    }


    #my $command = "$condorHistoryCommand -completedsince '$postgresTimeString'";
    # condor_history -l -backwards -constraint "completiondate > `date -d "$(date+%-m/%-d) 04:00" +%s`" > longlist
    #my $command = "$condorHistoryCommand -l -backwards -constraint \"completiondate > `date -d \"$postgresTimeString\"`\"";
    my $command = "$condorHistoryCommand -l -backwards";

    if ($postgresTimeString ne "") {
        $command .= " -completedsince \"$postgresTimeString\"";
    }

    &printLog (6,"Getting job history from condor: $command\n");

    open(JOBHIST, "$command |") || &error("Fatal Error: couldn't get job history from $condorHistoryCommand!\n");

    my $jhLine = "";
    my $classadLine = "";
    my $jhClusterId = "";
    my $jhCompletionDate = 0;
    my $jhJobStartDate = 0;

    my $firstJobId = 1;

    while ($keepgoing) {

        # not more than a bunch of jobs at a time!
        if ($jobsThisStep == $jobPerTimeInterval) {
            $jobsThisStep = 0;
            #print "".localtime().": $jobsThisStep jobs processed. Sleeping for $parserProcessingInterval seconds.\n";
            my $secsWaited = 0;
            while ($keepgoing && $secsWaited < $timeInterval) {
                sleep 1;
                $secsWaited++;
            }
        }

	if (!($classadLine = <JOBHIST>)) {
            # this was the last job to process:

            if ($jhLine ne "" && $jhClusterId ne $startJob
                && $jhClusterId ne "") {

                # still a job to process
                &printLog (6,"Processing last job with ClusterId=$jhClusterId.\n");

                if (&processCondorJobClassad($jhLine, $jhClusterId,
                                             $jhJobStartDate,
                                             $jhCompletionDate) == 0) {

                    if ($firstJobId) {
                        # if we reach the end of the condor history output
                        # we can be sure that we will need a final buffer
                        # independent of whether this is the first run
                        # or not.

                        # for updating the buffer:
                        $_[2] = $lastJob = $jhClusterId; # lastJob
                        $_[3] = $lastTimestamp = $jhCompletionDate; # lastTimestamp
                        $firstJobId = 0;
                    }

                }

                &printLog (6,"No other jobs in the history. Stopping iteration.\n");
            }
	    elsif ($jhClusterId ne $startJob && $jhClusterId != 0) {
                # found the last job to process, quit this iteration!
                &printLog (6,"Found alredy processed job with ClusterId=$jhClusterId. Stopping iteration ...\n");
            }

            last;

        }
        else {
            # having another line from the condor_history output
            chomp($classadLine);

            if ($classadLine =~ /No historical jobs in the database match your query/) {
                &printLog (7,"No new jobs found in history!\n");
                last;
            }
            # included below ...
            #elsif ($classadLine =~ /^ClusterId\s?=\s?(\d*).*$/) {
                #$jhClusterId = $1;
            #}
            elsif ($classadLine =~ /^(\S+)\s?=\s?\"(.*)\"$/
                   || $classadLine =~ /^(\S+)\s?=\s?(.*)$/) {
                # something useful like 'ClusterId = 12345' or 'Owner = "cms001"'
                chomp($classadLine);

		$jhLine .= "=====".$classadLine;

                if ($1 eq "ClusterId") {
                    $jhClusterId = $2;
                }
                elsif ($1 eq "CompletionDate") {
                    $jhCompletionDate = $2;
                }
                elsif ($1 eq "JobStartDate") {
                    $jhJobStartDate = $2;
                }
            }
            elsif ($classadLine =~ /^\s*$/) {
                # empty or blank line (job separator!)
                # if we had something before we need to process it before
                # continuing with the next classad.

                if ($tmpBuffTstamp == 0  # do if we have a regular buffer!
                       && $jhClusterId eq $startJob && $startJob ne "") {
                    # found the last job to process, quit this iteration!

                    # write this only if we have processed at least one job!
                    # Otherwise it would be written on all empty iterations.
                    if ( ($_[2] ne $startJob) || ($_[3] ne $startTimestamp)) {                        &printLog (6,"Found already processed job with ClusterId
=$jhClusterId. Stopping iteration ...\n");
                    }

                    last;
                }
		else {

                    if ($tmpBuffTstamp < 0) {
                        # this is the very first run, we don't even have a
                        # temporary buffer; make sure we start writing
                        # temporary buffers until the first iteration is
                        # completed:                        &printLog (7,"This is the very first job to process. Start writing a temporary buffer for backward processing until the first iteration is completed!\n");
                        $tmpBuffTstamp = $jhCompletionDate;
                    }
                    elsif ($tmpBuffTstamp > 0) {
                        # we have a temporary buffer ... process that 
                        # before creating a new buffer!
                        $firstJobId = 0;
                    }


                    if ($firstJobId) {
                        # for updating the buffer:
                        $_[2] = $lastJob = $jhClusterId; # lastJob
                        $_[3] = $lastTimestamp = $jhCompletionDate; # lastTimestamp
                        $firstJobId = 0;

                        &printLog (6, "This is the most recent job to process: $jhClusterId; CompletionDate: $jhCompletionDate\n");
                    }


                    if ($tmpBuffTstamp > 0 && $jhCompletionDate > $tmpBuffTstamp) {
                        &printLog (6,"Skipping job $jhClusterId (since CompletionDate $jhCompletionDate after $tmpBuffTstamp of temporary buffer).\n");
                        next;
                    }
		&printLog (6,"Processing job: $jhClusterId with CompletionDate: $jhCompletionDate (=".localtime($jhCompletionDate)." local time); JobStartDate: $jhJobStartDate\n");

                    if (&processCondorJobClassad($jhLine, $jhClusterId,
                                                 $jhJobStartDate,
                                                 $jhCompletionDate) == 0) {

                        if ($tmpBuffTstamp != 0) {
                            # in case this is the first run,
                            # adjust temporary buffer!
                            $_[4] = $tmpBuffTstamp = $jhCompletionDate;

                            &putTmpBuffer($collectorBufferFileName, $lastJob,
                                          $lastTimestamp, $tmpBuffTstamp);
                        }
                    }
                    #else {
                        # some slight error ...
                     #   last;
                    #}

                    # keep track of the number of jobs processed:
                    $jobsThisStep++;
                }

                #&printLog (7,"Looking for further classads ...\n");
                $jhLine = "";
                $jhClusterId = "";
                $jhCompletionDate = 0;
                $jhJobStartDate = 0;
            }
	    else {
                # cannot parse this line from the condor_history output
                &printLog (3,"Warning: Cannot parse this line from the condor_history output: $classadLine\n");
            }

        }

    } # while ($keepgoing)

    close(JOBHIST);
}

## --------- parse CE user map log file for accounting --------##
## to find a specific job ...

sub parseCeUserMapLog {

    my $lrmsJobID = $_[0];
    my $lrmsTimestamp = $_[1];
    my $job_ctime = $_[2];

    # important note: the CE's log file might not have the precise timestamp
    # of the job's submission to the LRMS (due to implementation problems)
    # hence the CE log's entry is not necessarily in the (rotated?) log file
    # we expect from the LRMS timestamp

    my $gHeader = "";
    my $isLocal = 0; # assume it is a grid job with an entry in the CE log

    my @ceLogFiles;

    my %logFMod = ();

    opendir(DIR, $ceJobMapLogDir) || &error("Error: can't open dir $ceJobMapLogDir: $!");
    while( defined(my $file = readdir(DIR)) ) {
	my $fullname = $ceJobMapLogDir.$file;
	#print "CE_LOG_FILE:$fullname ...";

	next if ($file =~ /^\.\.?$/); # skip '.' and '..' 
	next if ( !( $fullname =~ /^$ceJobMapLog[\/\-_]?\d{8}(\.gz)?$/ ) &&
		  !( $fullname =~ /^$ceJobMapLog[\/\-_]?\d{4}-\d{2}-\d{2}(\.gz)?$/ ) &&
		  !( $fullname =~ /^$ceJobMapLog(\.\d*)?(\.gz)?$/)
		  ); # skip if not like "<logname>(-)20060309(.gz)" (one per day)
                     # skip if not like "<logname>(-)2006-03-09(.gz)" (one per day)
                     # and not like "<logname>.1(.gz)" (rotated)!
        push @ceLogFiles, $file;

	# keep track of last modification timestamp:
	$logFMod{$file} = (stat("$ceJobMapLogDir/$file"))[9]; #mtime in sec. after epoch
    }
    closedir DIR;

    # now we sort the CE log files according to their modification
    # timestamp
    my @sortedCeLogFiles
	= (sort{ $logFMod{$b} <=> $logFMod{$a} } keys %logFMod);

    # find up to 3 file names: the log file that should contain the
    # LRMS timestamp; the previous file and the next file (in case the
    # CE log timestamp is not exactly synchronized the CE log entry might
    # end up in a previous or next file):

    my @ceScanLogFiles = ("", "", "");
    my %scanDirection;

    # $ceScanLogFiles[0] is the file expected for this timestamp
    # $ceScanLogFiles[1] is the previous file
    # $ceScanLogFiles[2] is the next file

    my $logFile = "";
    while (@sortedCeLogFiles) {
	$logFile = shift @sortedCeLogFiles;

	if ($logFMod{$logFile} < $job_ctime) {
	    # this is the first file with an earlier timestamp, thus it is the
	    # previous file
	    $ceScanLogFiles[1] = $logFile;
	    $scanDirection{$logFile} = "backward";
	    last;
	}
	else {
	    # as long as we didn't find the previous file, this might be
	    # the expected one:
	    $ceScanLogFiles[2] = $ceScanLogFiles[0]; # next file
	    $scanDirection{$ceScanLogFiles[2]} = "forward";
	    $ceScanLogFiles[0] = $logFile;           # expected file
	    $scanDirection{$ceScanLogFiles[0]} = "backward";
	}
    }

    my $scanFile = "";
    my $keepSearchingCeLogs = 1;
    foreach $scanFile (@ceScanLogFiles) {

	last if (!$keepSearchingCeLogs);

	next if ($scanFile eq "");
	&printLog(7, "Scanning CE log $scanFile (last modified=$logFMod{$scanFile}); direction: $scanDirection{$scanFile}");

	# decide whether to read forward or backward:
	my $cmd = $tac_cmd;  # default for backward!
	if ($scanDirection{$scanFile} eq "forward") {
	    $cmd = $cat_cmd;
	}
	# decide whether to decompress using 'less':
	if ($scanFile =~ /(\.gz)?$/) {
	    # decompress and pipe into cat/tac:
	    $cmd = "$less_cmd $ceJobMapLogDir/$scanFile | ".$cmd;
	}
	else {
	    # just use cat/tac:
	    $cmd = $cmd." $ceJobMapLogDir/$scanFile";
	}

	if ( !open ( CELOGFILE, "$cmd |" ) ) {
	    &printLog (7,"Warning: Couldn't open the log file ... skipping!");
	    next;
	}

	while (my $line = <CELOGFILE>) {
	    if ($line =~ /\s*\"lrmsID=$lrmsJobID\"\s*/) {
		# found something, check timestamp (+/- a day):
		# "timestamp=2006-03-08 12:45:01" or
		# "timestamp=2006/03/08 12:45:01"
		my $ceLogTstamp = 0;
		if ($line =~ /\s*\"timestamp=(\d{4})[-\/](\d{2})[-\/](\d{2})\s(\d{2}):(\d{2}):(\d{2})\"\s*/) {

		    # get timestamp for this UTC time!
		    my $ceEntryTimestamp =
			timegm(int($6),int($5),int($4),    # ss:mm:hh
			       int($3),int($2)-1,int($1)); # dd-mm-yy
		    # month should be from 0 to 11 => -1 !
		    &printLog (6,"Found in CE log: lrmsID=$lrmsJobID with timestamp(UTC)=$1-$2-$3 $4:$5:$6 ($ceEntryTimestamp)");

		    if ( ($ceEntryTimestamp > $job_ctime-$TSTAMP_ACC)
			 && ($ceEntryTimestamp < $job_ctime+$TSTAMP_ACC)
			 ) {
			# the timestamp from the CE log is within a day
			# from the LRMS creation timestamp, accept it!

			my $logBuff = "Accepting timestamp from CE log!\nParsing entry: ";

			# example: "timestamp=2006-03-08 12:45:01" "userDN=/C=IT/O=INFN/OU=Personal Certificate/L=Padova/CN=Alessio Gianelle/Email=alessio.gianelle@pd.infn.it" "userFQAN=/atlas/Role=NULL/Capability=NULL" "userFQAN=/atlas/production/Role=NULL/Capability=NULL" "ceID=grid012.ct.infn.it:2119/jobmanager-lcglsf-infinite" "jobID=https://scientific.civ.zcu.cz:9000/-QcMu-Pfv4qHlp2dFvaj9w" "lrmsID=3639.grid012.ct.infn.it"

			# we already got the timestamp and the lrmsID
			my $userDN = "";
			my @userFQANs = ();
			my $ceID = "";
			my $jobID = "";

			my @fields = split(/\"/, $line);
			my $fld;
			foreach $fld (@fields) {
			    next if ($fld =~ /^\s*$/); # spaces in between
			    if ($fld =~ /^userDN=(.*)$/) {
				$userDN = $1;
				$logBuff .= "userDN=$userDN; ";
			    }
			    elsif ($fld =~ /^userFQAN=(.*)$/) {
				my $fqan = $1;
				if (! $fqan =~ /^\s*$/) {
				    push (@userFQANs, $fqan);
				}
				$logBuff .= "userFQAN=$fqan; ";
			    }
			    elsif ($fld =~ /^ceID=(.*)$/) {
				$ceID = $1;
				$logBuff .= "ceID=$ceID; ";
			    }
			    elsif ($fld =~ /^jobID=(.*)$/) {
				$jobID = $1;
				$logBuff .= "jobID=$jobID; ";
				if ($jobID eq "none" || $jobID eq "NONE") {
				    $jobID = "";
				}
			    }
			}
			&printLog (6,"$logBuff");

			# check that we have the minimum info:
			if ($ceID eq "") {
			    &printLog (6,"Warning: ceID missing! Considering this as a local job!");
			    $isLocal = 1;
			}
			#gliedin FIX
			if ( $jobID eq "" )
			{
				#this is a grid job, no jobID is available however. set one that doesn't conflict with outOfBand.
				$jobID = "GRID:" . $lrmsJobID .":". $lrmsTimestamp;
			}
			# info on job
			$gHeader = $gHeader."GRID_JOBID=$jobID\n"
			    ."LRMS_TYPE=$lrmsType\n"
			    ."LRMS_JOBID=$lrmsJobID\n"
			    ."LRMS_EVENTTIME=$lrmsTimestamp\n"
			    ."LRMS_SUBMTIME=$job_ctime\n";
			# info on user
			$gHeader = $gHeader."USER_DN=$userDN\n"
			    ."USER_FQAN=";
			my $fqans = "";
			my $fq = "";
			foreach $fq (@userFQANs) {
			    $fqans .= $fq.";";
			}
			if ($fqans ne "") {
			    chop($fqans); # cut last ";"
			}
			$gHeader = $gHeader.$fqans;

			$gHeader = $gHeader."\n"; # terminate field USER_FQAN

			# info on CE:
			$gHeader = $gHeader."CE_ID=$ceID\n"
			    ."timeZone=$timeZone\n";
                        $keepSearchingCeLogs = 0;
			last;
		    }
		    elsif ($ceEntryTimestamp<$job_ctime-$TSTAMP_ACC) {
			# the timestamp from the CE log is too low, stop
			# trying to find the job!
			&printLog (6,"Timestamp of CE log before LRMS creation time: no job found in CE log: local job!");;
			$isLocal = 1;
			$keepSearchingCeLogs = 0;
			last;
		    }
		    else {
			&printLog (6,"Timestamp of CE log after LRMS creation time: job with recycled LRMS ID ... ignoring!");
		    }

		} # if ($line =~ /\s*\"timestamp= ...

	    } # if ($line =~ /\s*\"lrmsID= ...

	} # while ($line = <CELOGFILE>) ...

        close CELOGFILE;

    } # foreach ...

    if ($gHeader eq "") {
	&printLog (6,"No job found in CE log: local job!");
	$isLocal = 1;
    }

    if ($isLocal) {
	$gHeader = "JOB_TYPE=local\n".$gHeader;
    }
    else {
	# grid job
	$gHeader = "JOB_TYPE=grid\n".$gHeader;
    }
    return $gHeader;
}



## ------ write file in dgasURBox per pushd ---------- ##
sub writeGianduiottoFile {

    my $filename = $lrmsType."_".$_[0]."_".$_[1]; 
    # unique filename: <lrmsType>_<lrmsJobID>_<lrmsEventTimestamp>
    my $header = $_[2];
    my $acctlog = $_[3];

    open(OUT, "> $dgasURBox/$filename") || return 1;
    print OUT "$header";
    print OUT "ACCTLOG:$acctlog";
    close (OUT);
    return 0;
}


## ------ get GLUE attributes From LDIF file --------- ##
sub getGLUEAttributesFromLdif {
    my $modTStamp = 0; # returns 0 if no valid attributes found!
                       # otherwise returns the timestamp of the last
                       # modification of the file!

    # to be stored in %glueAttributes
    %glueAttributes = (); # first empty everything

    my @ldifFiles = split(/,/, $ldifDefaultFiles);
    unshift(@ldifFiles, $glueLdifFile); # first to try!

    my @keys = split(/,/, $keyList);

    if (!@keys) {
	&printLog(3, "Warning: No GLUE attributes will be added to usage records (reason: no keyList in configuration file)!");
	return 0; # no keys -> no benchmarks
    }

    while (@ldifFiles) {
	my $file = shift(@ldifFiles);
	&printLog (6,"Trying to get GLUE benchmarks from LDIF file: $file");

	if (!open(GLUEFILE, "< $file")) {
	    &printLog(5,"Warning: could not open the LDIF file ... skipping!");
	    next;
	}

	my $foundSomething = 0;
	my $line;
	while ($line = <GLUEFILE>) {
	    my $key;
	    my $logBuff;
	    foreach $key (@keys) {
		if ( ($line =~ /^$key:\s?(.*)$/ )
		     || ($line =~ /^$key=(.*)$/ ) 
		     || ($line =~ /^${key}_(.*)$/ ) ) {
		    # accept stuff like "GlueHostBenchmarkSI00: 955" and
		    # "GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU=955"
		    # "GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU_955"
		    $glueAttributes{$key} = $1;
		    my $logBuff .= "found: $key=$1; ";
	    	    &printLog (6,"$logBuff");
		    $foundSomething = 1;
		}
	    }
	}

	close(GLUEFILE);

	if ($foundSomething) {
	    # get timestamp of file:
	    $modTStamp = (stat($file))[9]; #mtime in sec. after epoch
	    last; # don't check the other files (if any)!
	}

    }

    my $thisTStamp = time();

    if ($modTStamp == 0) {
	&printLog (5,"Warning: No GLUE attributes will be added to usage records (reason: no valid entries in LDIF file(s))!");
	return 0;
    }
    else {
	if($modTStamp > $thisTStamp-$DEF_LDIF_VALIDITY) {
	    # accept it for at least a day, even if more recently modified
	    $modTStamp = $thisTStamp-$DEF_LDIF_VALIDITY;
	}
    }
    return $modTStamp;
}

## ------ these are the LRMS type-specific functions ------ ##


# returns "" if the line should be ignored!
sub getLrmsTargetJobId {
    if ($_[0] eq "pbs") {
	return &getLrmsTargetJobId_pbs($_[1]);
    }
    elsif ($_[0] eq "lsf") {
	return &getLrmsTargetJobId_lsf($_[1]);
    }
    elsif ($_[0] eq "sge") {
	return &getLrmsTargetJobId_sge($_[1]);
    }
    return "";
}

sub getLrmsTargetJobId_pbs {
    my $jid = ""; # default: line to ignore!

    my @ARRAY = split(" " , $_[0] );
    if (scalar(@ARRAY) > 1) {
	my @ARRAY2 = split(";" , $ARRAY[1] );
	if (scalar(@ARRAY2) > 2 && $ARRAY2[1] eq "E" ) {
	    $jid = $ARRAY2[2]; # finished job, return LRMS ID!
	}
    }
    return $jid; # line to ignore?
}

sub getLrmsTargetJobId_lsf {
    my $jid = ""; # default: line to ignore!

    my @ARRAY = split(" " , $_[0] );

    if ( (scalar(@ARRAY) > 3) && ($ARRAY[0] eq "\"JOB_FINISH\"") ) {

	# make sure we consider this line only if it is a _single_ line
	my $recordOK = 1;
	if ($_[0] =~ /^\"JOB_FINISH\".*\"JOB_FINISH\"/) {
	    # not a single record (can happen if last line without
	    # trailing newline and reading backwards!)
	    $recordOK = 0;
	}

	if ($recordOK) {
	    $jid = $ARRAY[3]; # finished job, return LRMS ID!
	}
	else {
	    &printLog (3,"Ignoring entry from LSF log file (incomplete): $_[0]");
	}
    }
    else {
	&printLog (3, "Warning: LSF log file seems to have a wrong format! Expected: \"JOB_FINISH\" ...; having: $_[0]");
    }

    return $jid; # line to ignore?
}

# get lrmsid (job_number for SGE) only if there are valid timestamp and end_time>start_time>submission_time; log warning for other cases
sub getLrmsTargetJobId_sge {

	my $valid_timestamp = "946684800"; # 1 Jan 2000
	my $jid = ""; # default: line to ignore!

	my @ARRAY = split(":" , $_[0] );
	
	my $submission_time = $ARRAY[8];
	my $start_time = $ARRAY[9];
	my $end_time = $ARRAY[10];

	if ($submission_time>$valid_timestamp) 
	{
		if ($start_time>=$submission_time) 
		{
			if ($end_time>=$start_time) 
			{
				$jid = $ARRAY[5];
			}
			else
			{
				&printLog (3, "Warning: SGE end_time < start_time for line: $_[0]; skipping job.");
			}
		}
		else
		{
			&printLog (3, "Warning: SGE start_time < submission_time for line: $_[0]; skipping job.");
		}
	}
	else
	{
		&printLog (3, "Warning: SGE submission_time seems to have a wrong format for line: $_[0]; skipping job.");
	}	
	
	return $jid; # line to ignore?
}


# get event time for LRMS log entry: returns 0 if not found
sub getLrmsEventTime {
    if ($_[0] eq "pbs") {
	return &getLrmsEventTime_pbs($_[1], $_[2]);
    }
    elsif ($_[0] eq "lsf") {
	return &getLrmsEventTime_lsf($_[1], $_[2]);
    }
    elsif ($_[0] eq "sge") {
	return &getLrmsEventTime_sge($_[1], $_[2]);
    }
    return 0;
}

sub getLrmsEventTime_pbs {
    # Format in PBS log: 03/10/2006 00:03:33;E; ...

    my $eventTimestamp = 0;

    my @ARRAY = split(";" , $_[0] );
    if (scalar(@ARRAY) > 0) {
	my $sec = 0; my $min = 0; my $hour = 0;
	my $mday = 0; my $mon = 0; my $year = 0;

	if ($ARRAY[0] =~
	    /^(\d{2})\/(\d{2})\/(\d{4})\s(\d{2}):(\d{2}):(\d{2})$/) {

	    $mon = int($1)-1; # has to be 0 to 11 -> -1 !
	    $mday = int($2);
	    $year = int($3);
	    $hour = int($4);
	    $min = int($5);
	    $sec = int($6);

	    $eventTimestamp = timelocal($sec,$min,$hour,    # ss:mm:hh
					$mday,$mon,$year);  # dd-mm-yy
	    $_[1] = $ARRAY[0];
	}
    }

    return $eventTimestamp;
}

sub getLrmsEventTime_lsf {
    # Format in LSF log: "JOB_FINISH" "6.0" 1140194675 ...

    my $eventTimestamp = 0;

    my @ARRAY = split(" " , $_[0] );
    if ( (scalar(@ARRAY) > 2) && ($ARRAY[2] =~ /^(\d*)$/) ) {
	$eventTimestamp = int($1);

	$_[1] = $ARRAY[2];
    }

    return $eventTimestamp;
}

sub getLrmsEventTime_sge {
	#  Event time not present on SGE, using end_time
	#  Format in SGE log: cybersar:wn5-64.ca.infn.it:cybersar:cybersar002:STDIN:352:sge:0:1254769859:1254812685:1254812685:0:0:0:0:0:0.000000:0:0:0:0:14232:0:0:0.000000:0:0:0:0:135:95:NONE:defaultdepartment:NONE:1:0:0.000000:0.000000:0.000000:-q cybersar:0.000000:NONE:0.000000

	my $eventTimestamp = 0;
	my @ARRAY = split(":" , $_[0] );
	if ( (scalar(@ARRAY) > 10) && ($ARRAY[10] =~ /^(\d*)$/) ) {
		$eventTimestamp = $ARRAY[10];
	}
	return $eventTimestamp;
}

# get the LRMS creation time for the job: returns 0 if not found:
sub getLrmsJobCTime {
    if ($_[0] eq "pbs") {
	return &getLrmsJobCTime_pbs($_[1]);
    }
    elsif ($_[0] eq "lsf") {
	return &getLrmsJobCTime_lsf($_[1]);
    }
    elsif ($_[0] eq "sge") {
	return &getLrmsJobCTime_sge($_[1]);
    }
    return 0;
}

sub getLrmsJobCTime_pbs {
    my $ctime = 0;

    if ($_[0] =~ /\sctime=(\d*)\s/) {
	$ctime = int($1);
    }

    return $ctime;
}

sub getLrmsJobCTime_lsf {
    my $ctime = 0;

    # in lsb.acct the creation time is the submitTime, the 8th field
    my @ARRAY = split(" " , $_[0] );
    if ( (scalar(@ARRAY) > 7) && ($ARRAY[7] =~ /^(\d*)$/) ) {
	$ctime = int($1);
    }

    return $ctime;
}

#  ctime = submission_time in SGE log
#  Format in SGE log: cybersar:wn5-64.ca.infn.it:cybersar:cybersar002:STDIN:352:sge:0:1254769859:1254812685:1254812685:0:0:0:0:0:0.000000:0:0:0:0:14232:0:0:0.000000:0:0:0:0:135:95:NONE:defaultdepartment:NONE:1:0:0.000000:0.000000:0.000000:-q cybersar:0.000000:NONE:0.000000
sub getLrmsJobCTime_sge {
	my $ctime = 0;
	my @ARRAY = split(":" , $_[0] );
	if ( (scalar(@ARRAY) > 8) && ($ARRAY[8] =~ /^(\d*)$/) ) {
		$ctime = $ARRAY[8];
	}
	return $ctime;
}

## ----- print error and quit ----- ##

sub error {
    if (scalar(@_) > 0) {
	&printLog (1,"$_[0]");
    }

    # but don't leave the lock file behind ...
    &printLog (1,"Exiting...");
    if ( &delLock($collectorLockFileName) != 0 ) {	
	&printLog (1,"Error removing lock file.");
    }
    exit(1);
}


sub errorNoRemoveLock {
    if (scalar(@_) > 0) {
	print "$_[0]";
    }
    exit(1);
}


sub date2Timestamp {
    my $tstamp = 0;
    if ($_[0] =~ /^(\d{4})[-\/](\d{2})[-\/](\d{2})$/) {
	# get timestamp for this UTC time!
	$tstamp = timegm(0,0,0,    # ss:mm:hh
			 int($3),int($2)-1,int($1)); # dd-mm-yy
	                 # month should be from 0 to 11 => -1 !
    }
    return $tstamp;
}

sub populateKeyDef
{
	my ( $input, $URkeyValues, $glueKeyValuePairs ) = @_;
	my %glueAttributes = %$glueKeyValuePairs;
	my @urKeyDef;
	open (FILE, $input ) || return 1;
	while ( <FILE> )
	{
		if ( /^\s*UR:(.*)\s*$/) 
		{
	        	&printLog (8,"fuond $_ - > $1");
			push ( @urKeyDef, $1 );
		}
	}
	close (FILE);
	foreach my $keyDef (@urKeyDef )
	{
		if ( $keyDef =~ /^(.*)\s*=\s*(.*)\s*;\s*(.*)$/ )
		{
			my $urKey = $1;
			my $glueKey = $2;
			my $regExp = $3;
			my $glueValue = $glueAttributes{$glueKey}; 
			&printLog ("$urKey=$regExp($glueValue)  ->  ",8);
			if ( $glueValue =~ /$regExp/ )
			{
	        		&printLog (8,"$urKey=$1");
				push ( @$URkeyValues, "$urKey=$1");
			} 
		}
	}
	return 0;
}


sub numFiles {
        my $dir = $_[0];
        my $count = 0;
        my $fd;

        if ( opendir($fd, $dir) )
        {
                while ( readdir($fd) )
                {
                        $count++;
                }
		closedir $fd;
                return $count;
        }
        else
        {
                &printLog ( "Could not open $dir!",3);
                return -1;
        }
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
                	print LOGH "$localtime: " . $log ."\n";
		}

        }
}
