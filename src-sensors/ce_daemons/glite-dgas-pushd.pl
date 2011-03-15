#!/usr/bin/perl -w

# DGAS Pushd.
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
#use vars qw($VERSION @ISA @EXPORT $PERL_SINGLE_QUOTE);
use vars qw($PERL_SINGLE_QUOTE);


# turn off buffering of STDOUT
$| = 1;


# my $MAX_LSF_RECORDLENGTH_FOR_QUOTEWORDS = 4096;
   # this is the maximum LSF record length that we'll parse with
   # quotewords, otherwise we risk a segmentation fault. Longer
   # records qill be parsed by a costum function (splitCVSstring)

START:

#FIXME needs to understand the jobmanager type blah vs lcgpbs and set it up

my $sigset = POSIX::SigSet ->new();
my $actionHUP=POSIX::SigAction->new("sigHUP_handler",$sigset,&POSIX::SA_NODEFER);
my $actionInt=POSIX::SigAction->new("sigINT_handler",$sigset,&POSIX::SA_NODEFER);
POSIX::sigaction(&POSIX::SIGHUP, $actionHUP);
POSIX::sigaction(&POSIX::SIGINT, $actionInt);
POSIX::sigaction(&POSIX::SIGTERM, $actionInt);

my $dgasLocation = $ENV{DGAS_LOCATION};
if ( $dgasLocation eq "" )
{
	$dgasLocation = $ENV{GLITE_LOCATION};
	if ( $dgasLocation eq "" )
	{
		$dgasLocation = "/usr/";
	}
}

my $configFilePath = "/etc/dgas/dgas_sensors.conf"; 
my %configValues = (
		    dgasURBox         =>  $dgasLocation . "/var/dgasURBox",
		    dgasErrDir        =>  $dgasLocation . "/var/dgasURBox/ERR/",
		    qDepth 	      => "2",
		    qMult	      => "5",
		    lockFileName      =>  $dgasLocation . "/var/glite-dgas-ce-pushd.lock",
		    logFileName       =>  $dgasLocation . "/var/log/dgas_ce_pushd.log",
		    mainPollInterval  => "5",
		    queuePollInterval =>"25",
		    siteName	      =>"",
		    localHostNameAsCEHostName => "no",
		    useCEHostName => "",
		    localUserGroup2VOMap => "",
                    havePoolAccounts => "yes",
		    poolAccountPatternFile => "",
		    systemLogLevel	      => 7,
		    maxThreadNumber           => "5",
		    gipDynamicTmpCEFiles              => "/opt/lcg/var/gip/tmp/lcg-info-dynamic-ce.ldif*",
		    useUrKeyDefFile => "no",
		    urKeyDefFile => "/etc/dgas/dgas_sensors.conf",
		    voToProcess => "",
		    printAsciiLog => "no",
		    useSQLite => "no",
		    asciiLogFilePath => $dgasLocation . "/var/log/pushdAscii.log",
		    transportLayer => "legacy",
		    recordComposer1 => $dgasLocation . "/libexec/dgas-legacyCpuComposer",
		    recordProducer1 => $dgasLocation. "/libexec/dgas-amqProducer",
		    recordComposer2 => $dgasLocation . "/libexec/ogfurComposer",
		    recordProducer2 => $dgasLocation. "/libexec/dgas-amqProducer",
		    );

my $systemLogLevel = 7;
my $logType = 0;
my $LOGH;
my $ASCIILOG;


my $hostName = `hostname -s`;
chomp($hostName);

my $domainName = `hostname -d`;
chomp($domainName);

#Parse configuration file
if(exists $ARGV[0]) 
{
    $configFilePath = $ARGV[0];
}	
&parseConf($configFilePath); 
&bootsrapLog($configValues{logFileName});
## Global variable initialization
my %urGridInfo;
my $UR;
my @queue = ();

my $maxThreadNumber = $configValues{maxThreadNumber};
my $qDept = $configValues{qDepth};
my $qMult = $configValues{qMult}; 

my $dgas_dir     = $configValues{dgasURBox};
my $dgas_err_dir = $configValues{dgasErrDir};

my $lockFileName = $configValues{lockFileName};

my $T2 = $configValues{queuePollInterval};
my $T1 = $configValues{mainPollInterval};
my $printAsciiLog = 0;

my $localUserGroup2VOMap = $configValues{localUserGroup2VOMap};

my $poolAccountPatternFile = $configValues{poolAccountPatternFile};

my $asciiLogFilePath =$configValues{asciiLogFilePath};

my $transportLayer = $configValues{transportLayer};

# make sure we don't use the local hostname for the CE if another one is
# specified!
if (exists($configValues{useCEHostName})
    && $configValues{useCEHostName} ne "") {

    $configValues{localHostNameAsCEHostName} = "no";
}

&printLog ( 5, "Got $configValues{asciiLogFilePath}");
&printLog ( 5, "Got $configValues{printAsciiLog}");
if (exists($configValues{printAsciiLog})
    && $configValues{printAsciiLog} eq "yes") {

        &printLog ( 5, "Using $asciiLogFilePath");
    $printAsciiLog = 1;
    open ( ASCIILOG, ">>$configValues{asciiLogFilePath}" ) || &printLog ( 5, "Can't open $configValues{asciiLogFilePath}");
}

#This is needed by 'amq' transport layer.
my $ceCertificateSubject = `openssl x509 -subject -in /etc/grid-security/hostcert.pem -noout`;
if ( $ceCertificateSubject =~ /subject=\s(.*)/)
{
        $ceCertificateSubject = $1;
}

my $havePoolAccounts = 1;  # default is "yes"!

if ($configValues{havePoolAccounts} =~ /^yes$/i) {
    &printLog ( 5, "Considering pool accounts for determining the VOs of out-of-band jobs.");
}
elsif ($configValues{havePoolAccounts} =~ /^no$/i) {
    $havePoolAccounts = 0;
    &printLog ( 5, "NOT considering pool accounts for determining the VOs of out-of-band jobs.");
}
else {
    &printLog ( 3, "Warning: Unknown argument for havePoolAccounts in configuration file; using default: considering pool accounts for determining the VOs of out-of-band jobs.");
}

my $voToProcess = $configValues{voToProcess};

my @dvec = split(/\//, $dgas_err_dir);

my $errDirName =  $dvec[$#dvec];
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

( -d $configValues{dgasURBox} ) || mkdir $configValues{dgasURBox};

#create dir of errors
&createErrDir();

$urGridInfo{siteName}=$configValues{siteName};
&printLog ( 4, "Publishing records for site::$urGridInfo{siteName}");
my @ATMDefinitions;
if ( $configValues{useUrKeyDefFile} eq "yes" )
{
	&printLog ( 5, "Using record definitions from:$configValues{urKeyDefFile}");
	&populateATMDef ( $configValues{urKeyDefFile}, \@ATMDefinitions);
}


while( $keepGoing )
{
    
##----------------------------------------------------------------  STEP1
    opendir(DIR, $dgas_dir) || &error("can't opendir $dgas_dir: $!");

    # first get all interesting files from dgasURBox:
    my @urFiles = ();
    while( defined(my $file = readdir(DIR)) && $keepGoing ) 
    {
	next if ($file eq "" || $file =~ /^\.\.?$/ || $file =~ /.proxy$/ || $file =~ /^$errDirName$/);	# skip . .. proxy files
	push (@urFiles, $file);
    }
    closedir DIR;

    # process files:
    my @childs;
    my $threadNumber = 0;
    while ($keepGoing && @urFiles)
    {
	my @file;
	for ( my $i = 0; $i < $maxThreadNumber; $i++)
	{
		my $fileBuffer = pop (@urFiles);
		next if (! -r "$dgas_dir/$fileBuffer" && ! -f "$dgas_dir/$fileBuffer");
		$file[$i] = $fileBuffer;
	}
	for ( $threadNumber = 0; $threadNumber < $maxThreadNumber; $threadNumber++)
	{
		my $pid = fork();	
		if ( $pid )
		{
			#parent
			push(@childs, $pid);
		}
		elsif ( $pid == 0 )
		{
			##child
			if ( $file[$threadNumber] ne "" )
			{
				&printLog ( 4, "Spawning new process:$threadNumber on file: $file[$threadNumber]" );
				if( &checkQueue($file[$threadNumber]) == 0 )           ## if file is not in queue
				{
					&parseFile($file[$threadNumber]);         ## if file is a true input file   
					if(&callAtmClient($file[$threadNumber]) == 0)
					{
						&printLog ( 4, "UR successfully forwarded to HLR." );
						&delFile($file[$threadNumber]);
					}
					else{&pushToQueue($file[$threadNumber]);}
				}
			}
			exit(0);
		}
		else
		{
			##no resource
			&printLog ( 2, "Not enough system resources to spawn new process." );
		}
	}
	foreach (@childs)
	{
		waitpid($_,0);
	}
    }

    if ( $keepGoing )
    {
	my $secsWaited = 0;
	while ($keepGoing && $secsWaited < $T1) {
	    sleep 1;
	    $secsWaited++;
	}
    }

##----------------------------------------------------------------  END STEP1 
    
    if((&timeOut() == 0) && ($#queue != -1) && $keepGoing )    ##timeout reached
    {
	my @localQueue = ();
	
	@queue = sort { ( ($a =~ /:(\d*):\d*$/)[0] <=> ($b =~ /:(\d*):\d*$/)[0] )
			    || 
			    ( ($a =~ /:(\d*)$/)[0] <=> ($b =~ /:(\d*)$/)[0] ) } @queue;
	
	foreach(@queue)
	{
	    my @vec = split(/:/, $_);
	    
	    
	    if(-e $dgas_dir.$vec[0])
	    {
		&printLog ( 7, "Retrying to process $vec[0]; multiplicity: $vec[1]; queue depth: $vec[2]" );

		&parseFile($vec[0]);
		if(&callAtmClient($vec[0]) == 0)
		{
		    &printLog ( 4, "UR sent to HLR." );
		    &delFile($vec[0]);
		}
		elsif ($vec[1]>= $qMult)
		{
		    &printLog ( 3, "Warning: UR could not be forwarded to HLR; maximum number of retries reached; moving $vec[0] to $dgas_err_dir" );
		    &moveInErrDir($vec[0]);
		}
		else
		{
		    &printLog ( 3, "Warning: UR could not be forwarded to HLR; retrying later ..." );
		    my $updatedItem = &setFile(@vec);
		    push @localQueue , $updatedItem;
		}
	    }
	}
	
	@queue =  @localQueue;  #  update queue 
	splice @localQueue, 0;
    }
    
}   

&Exit();

sub parse_line {
    my($delimiter, $keep, $line) = @_;
    my($word, @pieces);

    no warnings 'uninitialized';        # we will be testing undef strings

    while (length($line)) {
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
                    )//xs or return;            # extended layout                  
        my ($quote, $quoted, $unquoted, $delim) = (($1 ? ($1,$2) : ($3,$4)), $5, $6);


        return() unless( defined($quote) || length($unquoted) || length($delim));

        if ($keep) {
            $quoted = "$quote$quoted$quote";
        }
        else {
            $unquoted =~ s/\\(.)/$1/sg;
            if (defined $quote) {
                $quoted =~ s/\\(.)/$1/sg if ($quote eq '"');
                $quoted =~ s/\\([\\'])/$1/g if ( $PERL_SINGLE_QUOTE && $quote eq "'");
            }
        }
        $word .= substr($line, 0, 0);   # leave results tainted
        $word .= defined $quote ? $quoted : $unquoted;

        if (length($delim)) {
            push(@pieces, $word);
            push(@pieces, $delim) if ($keep eq 'delimiters');
            undef $word;
        }
        if (!length($line)) {
            push(@pieces, $word);
        }
    }
    return(@pieces);
}

sub quotewords {
    my($delim, $keep, @lines) = @_;
    my($line, @words, @allwords);


    foreach $line (@lines) {
        @words = parse_line($delim, $keep, $line);
        return() unless (@words || !length($line));
        push(@allwords, @words);
    }
    return(@allwords);
}



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
}

###-----------------------END-----------------------------------------###
sub bootsrapLog 
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
                	print LOGH "$localtime: " . $log . "\n";
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
	if(/^qDept\s*=\s*\"(.*)\"$/){$configValues{qDept}=$1;}
	if(/^qMult\s*=\s*\"(.*)\"$/){$configValues{qMult}=$1;} 
	if(/^dgasURBox\s*=\s*\"(.*)\"$/){$configValues{dgasURBox}=$1;}
	if(/^dgasErrDir\s*=\s*\"(.*)\"$/){$configValues{dgasErrDir}=$1;}
	if(/^pushdLockFileName\s*=\s*\"(.*)\"$/){$configValues{lockFileName}=$1;}
	if(/^pushdLogFileName\s*=\s*\"(.*)\"$/){$configValues{logFileName}=$1;}
	if(/^mainPollInterval\s*=\s*\"(.*)\"$/){$configValues{mainPollInterval}=$1;}
	if(/^queuePollInterval\s*=\s*\"(.*)\"$/){$configValues{queuePollInterval}=$1;}
	if(/^lrmsType\s*=\s*\"(.*)\"$/){$configValues{lrmsType}=$1;}
	if(/^localUserGroup2VOMap\s*=\s*\"(.*)\"$/){$configValues{localUserGroup2VOMap}=$1;}
	if(/^siteName\s*=\s*\"(.*)\"$/){$configValues{siteName}=$1;}
	if(/^localHostNameAsCEHostName\s*=\s*\"(.*)\"$/){$configValues{localHostNameAsCEHostName}=$1;}
        if(/^havePoolAccounts\s*=\s*\"(.*)\"$/){$configValues{havePoolAccounts}=$1;}
        if(/^poolAccountPatternFile\s*=\s*\"(.*)\"$/){$configValues{poolAccountPatternFile}=$1;}
	if(/^useCEHostName\s*=\s*\"(.*)\"$/){$configValues{useCEHostName}=$1;}
	if(/^systemLogLevel\s*=\s*\"(.*)\"$/){$configValues{systemLogLevel}=$1;}
	if(/^gipDynamicTmpCEFiles\s*=\s*\"(.*)\"$/){$configValues{gipDynamicTmpCEFiles}=$1;}
	if(/^maxThreadNumber\s*=\s*\"(.*)\"$/){$configValues{maxThreadNumber}=$1;}
	if(/^useUrKeyDefFile\s*=\s*\"(.*)\"$/){$configValues{useUrKeyDefFile}=$1;}
	if(/^urKeyDefFile\s*=\s*\"(.*)\"$/){$configValues{urKeyDefFile}=$1;}
	if(/^voToProcess\s*=\s*\"(.*)\"$/){$configValues{voToProcess}=$1;}
	if(/^printAsciiLog\s*=\s*\"(.*)\"$/){$configValues{printAsciiLog}=$1;}
	if(/^useSQLite\s*=\s*\"(.*)\"$/){$configValues{useSQLite}=$1;}
	if(/^asciiLogFilePath\s*=\s*\"(.*)\"$/){$configValues{asciiLogFilePath}=$1;}
	if(/^transportLayer\s*=\s*\"(.*)\"$/){$configValues{transportLayer}=$1;}
	if(/^recordComposer1\s*=\s*\"(.*)\"$/){$configValues{recordComposer1}=$1;}
	if(/^recordProducer1\s*=\s*\"(.*)\"$/){$configValues{recordProducer1}=$1;}
	if(/^recordComposer2\s*=\s*\"(.*)\"$/){$configValues{recordComposer2}=$1;}
	if(/^recordProducer2\s*=\s*\"(.*)\"$/){$configValues{recordProducer2}=$1;}
    }
    close(FILE);

    if (exists($configValues{systemLogLevel})
	       && $configValues{systemLogLevel} ne "") {
	$systemLogLevel = int($configValues{systemLogLevel});
    }
}


sub checkQueue
{
    foreach (@queue)
    {
	my $file = $_;
	if($file =~ /($_[0]:\d*:\d*)/){ return 1;}   ## file in queue
    }
    
    return 0;
}      


sub pushToQueue 
{
    my $file= "$_[0]:0:0";
    unshift @queue, $file; 
    
    return 0;
}

sub parseFile
{
    my $file = $_[0];
    # empty these from the previous processed record:
    $UR = "";
    %urGridInfo = ();
    open(FILE, "< $dgas_dir/$_[0]") || return 1;
    &printLog ( 6 , "\nTrying to parse UR file $dgas_dir/$_[0]");
    my $line;
    while ($line = <FILE>) {
	if ($line =~ /^ACCTLOG:(.*)$/ ) {
	    $UR = $1;
	    last; # stop here
	}
	if ($line =~ /^([^=]*)=(.*)$/) {
	    $urGridInfo{$1}=$2;
	}
    }
    close(FILE);

    if ($UR eq "") {
	&printLog ( 2 , "Error: UR file has wrong format: $dgas_dir/$_[0] ... skipping!" );
	return 2;
    }

    # try to determine the LRMS type.
    my $lrmsType = "";
    if (exists($urGridInfo{"LRMS_TYPE"}) && ($urGridInfo{"LRMS_TYPE"} ne "")) {
	# give priority to what is stated by Gianduia
	$lrmsType = $urGridInfo{"LRMS_TYPE"};
    }
    elsif ($file =~ /^([^_]*)_/) {
        # try to extract from file name
        $lrmsType = $1;
    }

    # verify that we can treat this LRMS type!
    if ($lrmsType ne "pbs" 
		&& $lrmsType ne "lsf" 
		&& $lrmsType ne "sge" 
		&& $lrmsType ne "condor") {
	&printLog ( 2, "Error: cannot determine LRMS type for UR file $dgas_dir/$_[0] ... skipping!");
	return 3;
    }
    else {
	$urGridInfo{lrmsType} = $lrmsType;
	&printLog ( 7 , "LRMS type: $lrmsType");
    }

    &parseUR($lrmsType, $UR);

    return 0;
}

sub parseUR {
    if ($_[0] eq "pbs") {
	return &parseUR_pbs($_[1]);
    }
    elsif ($_[0] eq "lsf") {
	return &parseUR_lsf($_[1]);
    }
    elsif ($_[0] eq "condor") {
        return &parseUR_condor($_[1]);
    }
    elsif ($_[0] eq "sge") {
        return &parseUR_sge($_[1]);
    }
}

sub parseUR_pbs
{
	my $URString = $_[0];
	&printLog ( 8, "Got UR string:\n$URString");

	my @URArray = split ( ' ' , $URString );
	my @tmpArray = split ( ';', $URArray[1] );
	$_ = $tmpArray[3];
	if (/^user=(.*)$/){$urGridInfo{user}=$1;};
	$urGridInfo{lrmsId}=$tmpArray[2];
	$_ = $tmpArray[2];
	if (/^(\d*)\.(.*)$/){$urGridInfo{server}=$2;};
	foreach ( @URArray )
	{
		if ( /^queue=(.*)$/){$urGridInfo{queue}=$1;};
		if ( /^resources_used.cput=(.*)$/)
		{
			$_=$1;
			$_=~ /(\d*):(\d\d):(\d\d)$/;
			$urGridInfo{cput}= $3 + $2*60 + $1*3600;
		}
		if ( /^resources_used.walltime=(.*)$/)
		{
			$_=$1;
			$_=~ /(\d*):(\d\d):(\d\d)$/;
			$urGridInfo{walltime}= $3 + $2*60 + $1*3600;
		}
		if ( /^resources_used.vmem=(.*)$/)
		{
			$_=$1;
                        $_=~ /(\d*[M.k]b)$/;
			$urGridInfo{vmem}     = $1;
		}
		if ( /^resources_used.mem=(.*)$/)
		{
			$_=$1;
			$_=~ /(\d*[M.k]b)$/;
			$urGridInfo{mem}     = $1;
		}
		if ( /^Resource_List.neednodes=(\d*)$/)
		{	
			$urGridInfo{processors} = $1;
			# attention! might also be list of hostnames,
			# in this case the number of hosts should be
			# counted!? What about SMP machines; is their
			# hostname listed N times or only once??
		}
		if ( /^group=(.*)$/)
		{	
			$urGridInfo{group} = $1;
		}
		if ( /^jobname=(.*)$/)
		{	
			$urGridInfo{jobName} = $1;
		}
		if ( /^ctime=(\d*)$/)
		{	
			$urGridInfo{ctime} = $1;
		}
		if ( /^qtime=(\d*)$/)
		{	
			$urGridInfo{qtime} = $1;
		}
		if ( /^etime=(\d*)$/)
		{	
			$urGridInfo{etime} = $1;
		}
		if ( /^start=(\d*)$/)
		{	
			$urGridInfo{start} = $1;
		}
		if ( /^end=(\d*)$/)
		{	
			$urGridInfo{end} = $1;
		}
		if ( /^exec_host=(.*)$/)
		{	
			$urGridInfo{execHost} = $1;
		}
		if ( /^Exit_status=(\d*)$/)
		{	
			$urGridInfo{exitStatus} = $1;
		}
	}
	
}

sub parseUR_lsf
{
        my $URString = $_[0];
	&printLog ( 8, "Got UR string:\n$URString");

	my @new = ();
#	if (length($URString) <= $MAX_LSF_RECORDLENGTH_FOR_QUOTEWORDS) {
	    @new = quotewords(" ", 0, $URString);
#	}
#	else {
#	    # we use this instead for extreme records.
#	    @new = &splitCVSstring(" ", "\"", $URString);
#	}
	#$shift1 = new[22]: numAskedHosts
	#$shift2 = $new[23+$shift1]: numExHosts
        my $shift1 = $new[22];
        my $shift2 = $new[23+$shift1];
	$urGridInfo{server}=$new[16];
	if ($urGridInfo{server} =~ /^([^\.]*)\.(.*)$/)
	{
	    #server hostname has domain.
	    #PLACEHOLDER
	}
	elsif ($domainName ne "")
	{
	    # if no domain name in LSF log: add the CE's domain name
	    $urGridInfo{server} .= ".$domainName";
	}

        $urGridInfo{queue}=$new[12];
        $urGridInfo{user}=$new[11];
	# we now know the user's UNIX login, try to find out the group, that
	# isn't available in the LSF log file:
	my $groupOutput = `groups $urGridInfo{user}`;
	if ($groupOutput =~ /^$urGridInfo{user} : (.+)$/) {
	    $urGridInfo{group} = $1;
	}

        $urGridInfo{lrmsId}=$new[3];
        $urGridInfo{processors}=$new[6];
	if ($new[10] == 0)
	{   # happens if job was cancelled before executing?
	    $urGridInfo{walltime}=0;
	}
	else
	{
	    $urGridInfo{walltime}=$new[2]-$new[10];
	}
	if ($new[28+$shift2] == -1)
	{   # indicates that the value is not available!
	    $new[28+$shift2]=0;
	}
	if ($new[29+$shift2] == -1)
	{   # indicates that the value is not available!
	    $new[29+$shift2]=0;
	}
        $urGridInfo{cput}=int($new[28+$shift2])+int($new[29+$shift2]);
        $urGridInfo{mem}=$new[54+$shift2]."k";
        $urGridInfo{vmem}=$new[55+$shift2]."k";
        $urGridInfo{start}=$new[10];
        $urGridInfo{end}=$new[2];
        $urGridInfo{ctime}=$new[7];
        $urGridInfo{jobName}=$new[26+$shift2];
        $urGridInfo{exitStatus}=$new[49+$shift2];
	if ( $shift2 != 0 )
	{
		$urGridInfo{execHost} = "";
		for ( my $i = 1; $i<=$shift2; $i++ )
		{
			$urGridInfo{execHost} = $urGridInfo{execHost} . $new[23+$i];
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
	my @URArray = split ( ':',$URString);
	$urGridInfo{queue} = $URArray[0];
	$urGridInfo{execHost} = $URArray[1];
	$urGridInfo{group} = $URArray[2];
	$urGridInfo{user} = $URArray[3];
	$urGridInfo{jobName} = $URArray[4];
	$urGridInfo{lrmsId} = $URArray[5];
	$urGridInfo{ctime} = $URArray[8];
	$urGridInfo{qtime} = $URArray[9];
	$urGridInfo{etime} = $URArray[9];
	$urGridInfo{start} = $URArray[9];
	$urGridInfo{end} = $URArray[10];
	$urGridInfo{exitStatus} = $URArray[12];
	$urGridInfo{walltime} = $URArray[13];
	$urGridInfo{proessors} = $URArray[34];
	$urGridInfo{cput} = $URArray[36];
	$urGridInfo{mem} = $URArray[17]+$URArray[18]+$URArray[19];
	$urGridInfo{vmem} = $URArray[42];
}

sub parseUR_condor
{
        my $URString = $_[0];
        &printLog ("".localtime().": Got UR string:$URString",7);

        my %classadItems = ();

        my @URArray = split ( '=====' , $URString );

        foreach my $urPart (@URArray) {
            if($urPart =~ /^(\S+)\s?=\s?\"(.*)\"$/
               || $urPart =~ /^(\S+)\s?=\s?(.*)$/ ) {
                # something like 'ClusterId = 12345' or 'Owner = "cms001"'
                my $item = $1;
                my $value = $2;
                $classadItems{$item} = $value;
            }
        }

        if (exists($classadItems{ClusterId})) {
            # example: ClusterId = 6501
            $urGridInfo{lrmsId} = $classadItems{ClusterId};
        }

        if (exists($classadItems{Owner})) {
            # example: Owner = "cdf"
            $urGridInfo{user} = $classadItems{Owner};
        }
        # we now know the user's UNIX login, try to find out the group, that
        # isn't available in the Condor log file:
        my $groupOutput = `groups $urGridInfo{user}`;
        if ($groupOutput =~ /^$urGridInfo{user} : (.+)$/) {
            $urGridInfo{group} = $1;
        }

	if (exists($classadItems{x509userproxysubject})
            && !exists($urGridInfo{USER_DN})) {
            # example: x509userproxysubject = "/C=IT ..."
            $urGridInfo{USER_DN} = $classadItems{x509userproxysubject};
        }

        if (exists($classadItems{RemoteWallClockTime})
            && $classadItems{RemoteWallClockTime} =~ /^(\d+)(\.\d*)?$/) {
            # example: RemoteWallClockTime = 4264.000000
            $urGridInfo{walltime} = int($1);
        }

        $urGridInfo{cput} = 0;
        if (exists($classadItems{RemoteUserCpu})
            && $classadItems{RemoteUserCpu} =~ /^(\d+)(\.\d*)?$/) {
            $urGridInfo{cput} += int($1);
        }
        if (exists($classadItems{LocalUserCpu})
            && $classadItems{LocalUserCpu} =~ /^(\d+)(\.\d*)?$/) {
            $urGridInfo{cput} += int($1);
        }
        if (exists($classadItems{RemoteSysCpu})
            && $classadItems{RemoteSysCpu} =~ /^(\d+)(\.\d*)?$/) {
            $urGridInfo{cput} += int($1);
        }
        if (exists($classadItems{LocalSysCpu})
            && $classadItems{LocalSysCpu} =~ /^(\d+)(\.\d*)?$/) {
            $urGridInfo{cput} += int($1);
        }

	# no memory information in Condor history, but
        # mandatory for atmClient! Ugly hack: set to zero ...
        $urGridInfo{mem} = 0;
        $urGridInfo{vmem} = 0;


        if (exists($classadItems{CompletionDate})) {
            # example: CompletionDate = 1126899118
            $urGridInfo{end} = $classadItems{CompletionDate};
        }

        if (exists($classadItems{JobStartDate})) {
            # example: JobStartDate = 1126878488
            $urGridInfo{start} = $classadItems{JobStartDate};
        }

        if (exists($classadItems{LastRemoteHost}) &&
            $classadItems{LastRemoteHost} =~ /^(.*)$/) {
            # example: LastRemoteHost = "vm1@fnpc212.fnal.gov"
            $urGridInfo{execHost} = $1;
        }

        if (exists($classadItems{GlobalJobId}) &&
            $classadItems{GlobalJobId} =~ /^([^\#]*)\#.*$/) {
            # example: GlobalJobId = "fngp-osg.fnal.gov#1126868442#6501.0"
            $urGridInfo{server} = $1;
            # WARNING: take this also ad dgJobId or is it better to construct
            #          a unique ID???
        }

        if (exists($classadItems{Cmd})) {
            # use as jobName!
            $urGridInfo{jobName} = $classadItems{Cmd};
        }

	if (exists($classadItems{JobUniverse})) {
            # example: JobUniverse = 5
            $urGridInfo{queue} = $classadItems{JobUniverse};
        }

        if (!exists($classadItems{ExitBySignal})) {
            # old classad format, use ExitStatus:
            if (exists($classadItems{ExitStatus})) {
                # example: ExitStatus = 0
                $urGridInfo{exitStatus} = $classadItems{ExitStatus};
            }
        }
        elsif ($classadItems{ExitBySignal} =~ /false/i) {
            # new classad format: "clean" exit of job, take returned exit code:
            if (exists($classadItems{ExitCode})) {
                # example: ExitCode = 0
                $urGridInfo{exitStatus} = $classadItems{ExitCode};
            }
        }
        else {
            # new classad format: job crashed due to an unhandled signal, 
            # take signal code as exit code:
            if (exists($classadItems{ExitSignal})) {
                # example: ExitCode = 0
                $urGridInfo{exitStatus} = $classadItems{ExitSignal};
            }
        }

}

sub callAtmClient 
{
    my $now_string = localtime();
    if ( $keepGoing == 0 )
    {
	&printLog (4, "Got termination signal..");
	return -2;
    }
    my $cmd = "";
    my $status = -1;

    my $forceThisJobLocal = "no";
    if (exists($urGridInfo{JOB_TYPE}) && $urGridInfo{JOB_TYPE} eq "local")
    {
	&printLog (5, "JOB_TYPE=local found in UR file. Storing UR on Resource HLR _only_ and as LOCAL job!");
	$forceThisJobLocal = "yes";
    }
    my $localHostname = $hostName.".".$domainName;

    # Get submission host (for LSF):
    if ($urGridInfo{lrmsType} eq "lsf") {
	$urGridInfo{submitHost} = $urGridInfo{server};
    }
    elsif ($urGridInfo{lrmsType} eq "pbs") {
	$urGridInfo{lrmsServer} = $urGridInfo{server};
    }
    if ( $urGridInfo{lrmsType} eq "sge" )#SGE doesnt provid LRMS server info.
    {
	$urGridInfo{server} = $localHostname;#default. Not working on multi CE
	if (exists($configValues{useCEHostName}) 
		&& $configValues{useCEHostName} ne "" )
	{
		#sggested configuration on multiCe and single LRMS deployments.
		$urGridInfo{server} = $configValues{useCEHostName};
	}
    }

    # CE ID:
    if (exists($urGridInfo{CE_ID}) && $urGridInfo{CE_ID} ne "") {
	$urGridInfo{resGridId} = $urGridInfo{"CE_ID"};
	&printLog ( 6, "Got CE_ID from UR file: $urGridInfo{resGridId}");

	# get also ceHostName!
	if ($urGridInfo{CE_ID} =~ /^([^:]*):.*/) {
	    $urGridInfo{ceHostName} = $1;
	}
	$urGridInfo{execCE} = $urGridInfo{"CE_ID"};
    }
    elsif ($forceThisJobLocal
	   && exists($urGridInfo{server}) && $urGridInfo{server} ne ""
	   && exists($urGridInfo{queue}) && $urGridInfo{queue} ne "") {
	$urGridInfo{resGridId} = "$urGridInfo{server}:$urGridInfo{queue}";
	&printLog ( 5,"Constructed resource ID for local job: $urGridInfo{resGridId}");
    }
    else {
	&printLog ( 2, "Error: cannot determine CE/resource ID! Skipping job!");
	return -3;
    }

    if (exists($configValues{useCEHostName}) && $configValues{useCEHostName} ne ""
	&& exists($urGridInfo{queue}) && $urGridInfo{queue} ne "") {

	$urGridInfo{resGridId} = "$configValues{useCEHostName}:$urGridInfo{queue}";
	&printLog ( 4, "useCEHostName specified in configuration file! Using as resource ID: $urGridInfo{resGridId}");
    }
    elsif ($configValues{localHostNameAsCEHostName} eq "yes") {
	$urGridInfo{resGridId} = "$localHostname:$urGridInfo{queue}";
	&printLog ( 4,"localHostNameAsCEHostName =\"yes\" in configuration file! Using as resource ID: $urGridInfo{resGridId}");
    }

    # does the CE ID finish with the queue name???
    if ($urGridInfo{resGridId} !~ /[-:]$urGridInfo{queue}\W*$/) {
	$urGridInfo{resGridId} = $urGridInfo{resGridId}."-".$urGridInfo{queue};
	$urGridInfo{execCE} = $urGridInfo{execCE}."-".$urGridInfo{queue};
	&printLog ( 5, "Added queue name: CE_ID = $urGridInfo{resGridId}");
    }
    if ($urGridInfo{execCE} ne "" && $urGridInfo{execCE} !~ /[-:]$urGridInfo{queue}\W*$/) {
	$urGridInfo{execCE} = $urGridInfo{execCE}."-".$urGridInfo{queue};
    }

    # User DN:
    my $userDN = "";
    if (exists($urGridInfo{USER_DN}) && $urGridInfo{USER_DN} ne "") {
	$userDN = $urGridInfo{USER_DN};
	&printLog ( 5,  "Got USER_DN from UR file: $userDN");
    }
    else {
	&printLog ( 3, "Couldn't determine USER_DN.");
    }

    # FQAN:
    if (exists($urGridInfo{USER_FQAN}) && $urGridInfo{USER_FQAN} ne "") {
	$urGridInfo{fqan} = $urGridInfo{USER_FQAN};
	&printLog ( 5, "Got USER_FQAN from UR file: $urGridInfo{fqan}");
    }
    else {
	&printLog (3,"Couldn't determine USER_FQAN.");
    }

    # VO:
    &determineUserVO($urGridInfo{fqan}, $urGridInfo{user}, $havePoolAccounts, $urGridInfo{userVo}, $urGridInfo{voOrigin});
    
    # grid Job ID:
    my $gridJobId = "";
    if (exists($urGridInfo{GRID_JOBID}) && $urGridInfo{GRID_JOBID} ne "") {
	$gridJobId = $urGridInfo{GRID_JOBID};
	&printLog (5, "Got GRID_JOBID from UR file: $gridJobId");
    }
    elsif (exists($urGridInfo{server}) && $urGridInfo{server} ne ""
	   && exists($urGridInfo{lrmsId}) && $urGridInfo{lrmsId} ne ""
	   && exists($urGridInfo{start}) && $urGridInfo{start} ne "") {

	$gridJobId = $urGridInfo{server}.":".$urGridInfo{lrmsId}."_".$urGridInfo{start};

	&printLog (4, "No grid job ID. Constructed a unique ID: $gridJobId");
    }
    else {
	&printLog (2, "Error: cannot retrieve or construct a unique job ID! Skipping job");
	return -4;
    }

    # retrieving GlueCEInfoTotalCPUs

    if (exists($urGridInfo{queue})) {
        &searchForNumCpus($configValues{gipDynamicTmpCEFiles},$urGridInfo{queue},$urGridInfo{numCPUs});
    }

    # building command:
    my $legacyCmd = "$ENV{DGAS_LOCATION}/libexec/dgas-atmClient";

    $cmd = "";

    if ($gridJobId ne "") {
        $cmd .= " --jobid \"$gridJobId\"";
    }
    if ($urGridInfo{resGridId} ne "") {
	$cmd .= " --resgridid \"$urGridInfo{resGridId}\"";
    }
    if ($userDN ne "") {
        $cmd .= " --usrcert \"$userDN\"";
    }
     
    if (exists($urGridInfo{"GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU"})
	&& $urGridInfo{"GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU"} ne "") {
	my $si2k = $urGridInfo{"GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU"};
	$cmd .= " \"si2k=$si2k\"";
	&printLog ( 6, "Got 'GlueHostApplicationSoftwareRunTimeEnvironment: SI00MeanPerCPU' from UR file: $si2k");
	&printLog ( 8, "(note: Taking mean SI00, ignoring GlueHostBenchmarkSI00)");
    }
    elsif (exists($urGridInfo{GlueHostBenchmarkSI00})
	&& $urGridInfo{GlueHostBenchmarkSI00} ne "") {
	$cmd .= " \"si2k=$urGridInfo{GlueHostBenchmarkSI00}\"";
	&printLog ( 6, "Got 'GlueHostBenchmarkSI00' from UR file: $urGridInfo{GlueHostBenchmarkSI00}");
    }

    if (exists($urGridInfo{"GlueHostApplicationSoftwareRunTimeEnvironment: SF00MeanPerCPU"})
	&& $urGridInfo{"GlueHostApplicationSoftwareRunTimeEnvironment: SF00MeanPerCPU"} ne "") {
	my $sf2k = $urGridInfo{"GlueHostApplicationSoftwareRunTimeEnvironment: SF00MeanPerCPU"};
	$cmd .= " \"sf2k=$sf2k\"";
	&printLog ( 6, "Got 'GlueHostApplicationSoftwareRunTimeEnvironment: SF00MeanPerCPU' from UR file: $sf2k");
	&printLog ( 8, "(note: Taking mean SF00, ignoring GlueHostBenchmarkSF00)");
    }
    elsif (exists($urGridInfo{GlueHostBenchmarkSF00})
	&& $urGridInfo{GlueHostBenchmarkSF00} ne "") {
	$cmd .= " \"sf2k=$urGridInfo{GlueHostBenchmarkSF00}\"";
	&printLog ( 6, "Got 'GlueHostBenchmarkSF00' from UR file: $urGridInfo{GlueHostBenchmarkSF00}");
    }
    $urGridInfo{siteName}=$configValues{siteName};
    # Is this a strictly local job? Let the HLR server know!
    if (exists($urGridInfo{JOB_TYPE}) && $urGridInfo{JOB_TYPE} eq "local")
    {
	$cmd .= " \"accountingProcedure=outOfBand\"";
    }
    elsif (exists($urGridInfo{JOB_TYPE}) && $urGridInfo{JOB_TYPE} eq "grid")
    {
	$cmd .= " \"accountingProcedure=grid\"";
    } 
    $cmd .= " \"URCREATION=$now_string\"";
    if ( $configValues{useUrKeyDefFile} eq "yes" )
    {
    	    $cmd .= " -3";
	    my @atmTags;
	    &composeATMtags ( \@ATMDefinitions, \%urGridInfo, \@atmTags );
	    foreach my $tag (@atmTags )
	    {
		$cmd .= " \"$tag\""; 
	    }
    }
    else
    {
	#this is to ensure record consistency for backward compatibility
	if (exists($urGridInfo{cput})) {
			$cmd .= " \"CPU_TIME=$urGridInfo{cput}\"";
	}
    	if (exists($urGridInfo{walltime})) {
		$cmd .= " \"WALL_TIME=$urGridInfo{walltime}\"";
    	}
    	if (exists($urGridInfo{mem})) {
		$cmd .= " \"PMEM=$urGridInfo{mem}\"";
    	}
    	if (exists($urGridInfo{vmem})) {
		$cmd .= " \"VMEM=$urGridInfo{vmem}\"";
	    }
	    if (exists($urGridInfo{queue})) {
		$cmd .= " \"QUEUE=$urGridInfo{queue}\"";
	    }
	    if (exists($urGridInfo{user})) {
		$cmd .= " \"USER=$urGridInfo{user}\"";
	    }
	    if (exists($urGridInfo{lrmsId})) {
		$cmd .= " \"LRMSID=$urGridInfo{lrmsId}\"";
	    }
	    if (exists($urGridInfo{processors})) {
		$cmd .= " \"PROCESSORS=$urGridInfo{processors}\"";
	    }
	    if (exists($urGridInfo{group})) {
		$cmd .= " \"group=$urGridInfo{group}\"";
	    }
	    if (exists($urGridInfo{jobName})) {
		$cmd .= " \"jobName=$urGridInfo{jobName}\"";
	    }
	    if (exists($urGridInfo{start})) {
		$cmd .= " \"start=$urGridInfo{start}\"";
	    }
	    if (exists($urGridInfo{end})) {
		$cmd .= " \"end=$urGridInfo{end}\"";
	    }
	    if (exists($urGridInfo{ctime})) {
		$cmd .= " \"ctime=$urGridInfo{ctime}\"";
	    }
	    if (exists($urGridInfo{qtime})) {
		$cmd .= " \"qtime=$urGridInfo{qtime}\"";
	    }
	    if (exists($urGridInfo{etime})) {
		$cmd .= " \"etime=$urGridInfo{etime}\"";
	    }
	    if (exists($urGridInfo{exitStatus})) {
		$cmd .= " \"exitStatus=$urGridInfo{exitStatus}\"";
	    }
	    if (exists($urGridInfo{execHost})) {
		$cmd .= " \"execHost=$urGridInfo{execHost}\"";
	    }
	    if (exists($urGridInfo{ceHostName})) {
		$cmd .= " \"ceHostName=$urGridInfo{ceHostName}\"";
	    }
	    if (exists($urGridInfo{execCE})) {
		$cmd .= " \"execCe=$urGridInfo{execCE}\"";
	    }
	    if (exists($urGridInfo{submitHost})) {
		$cmd .= " \"submitHost=$urGridInfo{submitHost}\"";
	    }
	    if (exists($urGridInfo{lrmsServer})) {
		$cmd .= " \"lrmsServer=$urGridInfo{lrmsServer}\"";
	    }
		if (exists($urGridInfo{numCPUs})) {
	    	    $cmd .= " \"GlueCEInfoTotalCPUs=$urGridInfo{numCPUs}\"";
    		}
	    if (exists($urGridInfo{timeZone})) {
		$cmd .= " \"tz=$urGridInfo{timeZone}\"";
	    }
	    if ($urGridInfo{fqan} ne "") {
	        $cmd .= " \"fqan=$urGridInfo{fqan}\"";
	    }
	    if ($urGridInfo{userVo} ne "") {
	        $cmd .= " \"userVo=$urGridInfo{userVo}\"";
	    }
	    if ($urGridInfo{voOrigin} ne "") {
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
    my $asciiLogLine;

    if ( $printAsciiLog == 1)
    {
        my $localtime = localtime();
        $asciiLogLine = ";#$localtime;#$urGridInfo{userVo};#'$transportLayer'";
    }

    # running the command: 
    if ( $voToProcess ne "" )
    {
        my $processRecord = 0;
        my @voList = split(/;/, $voToProcess);
        foreach my $vo(@voList)
        {
                if ( $urGridInfo{userVo} eq $vo )
                {
                        &printLog ( 4, "Record for VO:$urGridInfo{userVo} found. forwarding it.");
                        $processRecord = 1;
                }
        }
        if ( $processRecord == 0)
        {

                &printLog ( 8, "Record for VO:$urGridInfo{userVo} found. SKIPPING.");
                return 0;
        }
    }

    my $exe = "";
    if ( ( $transportLayer =~ "amq" ) || ( $transportLayer =~ "transport1" ) )
    {
	my $recordComposer1 = $configValues{recordComposer1};
	my $recordProducer1 = $configValues{recordProducer1};
	$exe = $recordComposer1 . " ". $cmd . " | ". $recordProducer1;
	&printLog ( 8, "Running: $exe");
	$status = system("$exe");
	 if ($? == -1) {
		&printLog ( 2, "failed to execute: $!");
	    }
	    elsif ($? & 127) {
		$status = ($? & 127);
	    }
	    else {
		$status = $? >> 8;
	    }
	    &printLog ( 4, "TRANSPORT1 EXITSTATUS=$status" );

	if ( $printAsciiLog == 1)
        {
                print ASCIILOG "$exe $asciiLogLine;#T1STATUS=$status\n";
        }
    }

    $exe = "";
    if ( $transportLayer =~ "transport2"  )
    {
	my $recordComposer2 = $configValues{recordComposer2};
	my $recordProducer2 = $configValues{recordProducer2};
	$exe = $recordComposer2 . " ". $cmd . " | ". $recordProducer2;
	&printLog ( 8, "Running: $exe");
	$status = system("$exe");
	 if ($? == -1) {
		&printLog ( 2, "failed to execute: $!");
	    }
	    elsif ($? & 127) {
		$status = ($? & 127);
	    }
	    else {
		$status = $? >> 8;
	    }
	    &printLog ( 4, "TRANSPORT2 EXITSTATUS=$status" );

	if ( $printAsciiLog == 1)
        {
                print ASCIILOG "$exe $asciiLogLine;#T2STATUS=$status\n";
        }
    }


    $exe = "";
    if ( $transportLayer =~ "legacy" )
    {
	    $exe = $legacyCmd . $cmd;
	    &printLog ( 8, "Running: $exe");
	    $status = system("$exe");
	    if ($? == -1) {
		&printLog ( 2, "failed to execute: $!");
	    }
	    elsif ($? & 127) {
		$status = ($? & 127);
	    }
	    else {
		$status = $? >> 8;
	    }
	    &printLog ( 4, "EXITSTATUS=$status" );

	    # job duplicated on HLR?
	    #define ATM_E_DUPLICATED 	"65"
	    #define ATM_E_DUPLICATED_A 	"70"
	    #define ATM_E_DUPLICATED_B 	"71"
	    #define ATM_E_DUPLICATED_C 	"72" [might be error on HLR, do not delete UR!]
	    #define ATM_E_DUPLICATED_D 	"73"
	    if ( $status == 65 || $status == 70 || $status == 71 || $status == 73) {
		&printLog ( 4, "The record is already present on the HLR. Removing ...");
		return 0;
	    }

	    # job already waiting for processing on HLR?
	    #define ATM_E_TRANS       "64"
	    #define ATM_E_TRANS_A     "67" [might be error on HLR, do not delete UR!]
	    #define ATM_E_TRANS_B     "68" [might be error on HLR, do not delete UR!]
	    #define ATM_E_TRANS_C     "69"
	    if ( $status == 64 || $status == 69 ) {
		&printLog (4, "The record is already waiting on the HLR (in the transaction queue) to be processed. Removing ...");
		return 0;
	    }
		
		if ( $printAsciiLog == 1)
        	{
                	print ASCIILOG "$exe $asciiLogLine;#LEGACYSTATUS=$status\n";
        	}

	    return $status;
    }
}

sub determineUserVO()
{

    my $retVal = 1; # not found

    my $uFqan = $_[0];
    my $uid = $_[1];
    my $havePoolAcc = $_[2];

    my $voName = "";

    my @fqans = split(/;/, $uFqan);
    my @fqanParts;
    if (scalar(@fqans) > 0 && $fqans[0] ne "") {
	@fqanParts = split(/\//, $fqans[0]);
    }

    if (scalar(@fqanParts) > 1 && $fqanParts[1] ne "")
    {
	if ($fqanParts[1] =~ /^VO=(.*)$/)
	{
	    $fqanParts[1] = $1;
	}
	$_[3] = $fqanParts[1]; # userVo
	$_[4] = "fqan";        # voOrigin
	$retVal = 0;

	&printLog (7, "Determined user VO from FQAN: $_[3]");
    }
    elsif ($havePoolAcc && &getVOFromPoolAccountPattern($uid, $voName)) {
	$_[3] = $voName;         # userVo
	$_[4] = "pool";          # voOrigin
	$retVal = 0;
    }
    elsif ($localUserGroup2VOMap ne "") {
	# try local user/group mapping!
	if (! -r "$localUserGroup2VOMap") {
	    &printLog (3, "WARNING: cannot read from localUserGroup2VOMap '$localUserGroup2VOMap' ... cannot determine an eventual local mapping for local user $urGridInfo{user}!");
	}
	else {
	    # try specific user first!
	    my $cmd = "grep '^user\[\[:blank:\]\]$urGridInfo{user}\[\[:blank:\]\]' $localUserGroup2VOMap";
	    my $userLine = `$cmd`;
	    if ($userLine =~ /^user\s$urGridInfo{user}\s([^\s]+).*$/) {
		# found the VO!!!
		$_[3] = $1;        # userVo
		$_[4] = "map";     # voOrigin
		$retVal = 0;
		&printLog (7,"Determined user VO from localUserGroup2VOMap (user $urGridInfo{user}): $_[3]");
	    }
	    else {
		# no specific user mapping, try groups!
		my $groupOutput = `groups $urGridInfo{user}`;
		if ($groupOutput =~ /^$urGridInfo{user} : (.+)$/) {
		    my @groupvec = split (/ /, $1);
		    foreach my $lgroup (@groupvec) {
			my $cmd = "grep '^group\[\[:blank:\]\]$lgroup\[\[:blank:\]\]' $localUserGroup2VOMap";
			my $groupLine = `$cmd`;
			if ($groupLine =~ /^group\s$lgroup\s([^\s]+).*$/) {
			    # found the VO!!!
			    $_[3] = $1;        # userVo
			    $_[4] = "map";     # voOrigin
			    $retVal = 0;
			    &printLog ( 7, "Determined user VO from localUserGroup2VOMap (group $lgroup): $_[3]");
			    last;
			}
		    }
		}
	    }
	}
    }

    if ($retVal != 0) {
	&printLog ( 3, "Could not determine user VO for local job!");
    }

    return $retVal;
}



sub getVOFromPoolAccountPattern() {

    my $uid = $_[0];

    # if patterns are specified as configuration file, use them!
    if ($poolAccountPatternFile ne "") {

	my $userVo = &getVOFromConfiguredPoolAccountPatterns($uid);

	if ($userVo ne "") {
	    $_[1] = $userVo;         # userVo
	    &printLog (7,"Determined user VO from pool account (poolAccountPatternFile): $_[1]");
	    return 1;  # ok
	}

        return 0;   # not found
    }
    # if no patterns configured, use the built-in patterns (for backward
    # compatibility ... phase this out later?)
    elsif ($uid =~ /^(.*)sgm$/ || $uid =~ /^(.*)prd$/
		|| $uid =~ /^(.*)sgm\d{3}$/ || $uid =~ /^(.*)prd\d{3}$/
		|| $uid =~ /^sgm(.*)\d{3}$/ || $uid =~ /^prd(.*)\d{3}$/
		|| $uid =~ /^sgm(.*)$/ || $uid =~ /^prd(.*)$/
		|| $uid =~ /^(.*)\d{3}$/
	   ) {

	$_[1] = $1;         # userVo

	&printLog (7, "Determined user VO from pool account (built-in patterns): $_[1]");

	return 1; # ok
    }


    return 0;  # not found
}

sub setFile()
{
    my @set = @_;   ## set[0] = fileName, set[1] = multiplicity, set[2] = depth
    
    if ($set[2] >= $qDept) 
    {
	$set[1]++;
	$set[2] = 0;
    }
    else {$set[2]++;}
    
    my $point = ":";
    my $updated = $set[0].$point.$set[1].$point.$set[2];

    return $updated;
}

sub delFile {   #receive as argument the filename to delete

    my $cmd = "rm -f $dgas_dir/$_[0]";
    &printLog ( 9, "$cmd ");
    my $status = system($cmd);
    if (-f "$dgas_dir/$_[0].proxy") {
	$cmd .=".proxy";
	&printLog (9, "$cmd ");
	$status =system($cmd);
    }
}

sub timeOut
{
    my $stop = time();
    my $delta = $stop -$start;
    #print "delta = $delta " ;   
    
    if($delta > $T2)
    {
	$start = time();
	return 0;
    }
    
    return 1;
}

sub createErrDir
{
    my $cmd = "mkdir $dgas_err_dir";
    &printLog (9, "$cmd ");
    my $status = system($cmd);

    return $status;
}

sub moveInErrDir
{
    my $cmd = "mv $dgas_dir/$_[0] $dgas_err_dir";
    my $status = system($cmd);
    &printLog (9, "$cmd ");
    if (-f "$dgas_dir/$_[0].proxy") {
	$cmd = "mv $dgas_dir/$_[0].proxy $dgas_err_dir";
	$status = system($cmd);
    }
    &printLog (9, "$cmd ");

    return $status;
}

##-------> lock  subroutines <---------##

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

sub delLock
{
    my $lockName = $_[0];
    open(IN,  "< $lockName") || return 1;
    close(IN);
    my $status = system("rm -f $lockName");
    return $status;
}

##-------> sig handlers subroutines <---------##

sub sigHUP_handler {
        &printLog (3, "got SIGHUP");
	$keepGoing = 1;
	goto START;
}

sub sigINT_handler {
        &printLog (3, "got SIGINT");
        $keepGoing = 0;
}

## ----- print error and quit ----- ##

sub error {
    if (scalar(@_) > 0) {
	&printLog ("$_[0]",3);
    }
    exit(1);
}

## ----- subs to extract GlueCEInfoTotalCPUs ----- ##

sub numCPUs
{
	my $ldifFile = $_[0];
	my $queue = $_[1];
	my $numCPUs;
	open(LDIF, "$ldifFile") || &printLog ( 2, "Error opening file $ldifFile");
	my $line;
	while ($line = <LDIF>)
	{
		#if (/^dn:\sGlueCEUniqueID=(.*)$queue,(.*)$/)
		if ($line =~ /GlueCEUniqueID=(.*)$queue,/)
		{
			my $innerline;
                        &printLog ($line,9);
                        while ($innerline = <LDIF>)
                        {
                                if ( $innerline =~ /GlueCEInfoTotalCPUs:\s?(.*)$/ )
                                {
                                        if ($1 != 0 )
                                        {
                                                $_[2] =$1;
                                                close (LDIF);
                                                return;
                                        }
                                }
                        }
                }
        }
        close (LDIF);
        return;
}

sub searchForNumCpus
{
	my @list = glob($_[0]);
	my $queue = $_[1];
	my $file;
	foreach $file (@list)
	{
		my $numCPUs;
		&numCPUs($file,$queue,$numCPUs);
		if ($numCPUs != 0)
		{
			&printLog ( "Found in: $file",9);
			$_[2] =$numCPUs;
		}
	}
	return;
}

sub populateATMDef
{
	my ( $input, $ATMDefs ) = @_;
	open (FILE, $input ) || return 1;
	while ( <FILE> )
	{
		if ( /^\s*ATM:(.*)\s*$/)
		{
			&printLog (7, $1);
			push ( @$ATMDefs, $1);
		}	
	}
}

sub composeATMtags
{
	&printLog (7, "Entering:composeATM");
	my ( $ATMdefs, $urHash, $atmTags ) = @_;
	my %hashBuff = %$urHash;
	foreach my $def ( @$ATMdefs )
	{
		&printLog (7, "composeATM:$def");
		$def =~ s/\$(\w+)/$hashBuff{$1}/g;
		if ( exists $hashBuff{$1} )
		{
			push ( @$atmTags, $def );
			&printLog (7, "inserted in ATM:$def");
		}
	}
}

sub getVOFromConfiguredPoolAccountPatterns {
        my $uid = $_[0];
        my $userVo = "";

	my $cmdline = "$ENV{DGAS_LOCATION}/libexec/glite-dgas-voFromPoolAccountPatterns.pl '$poolAccountPatternFile' '$uid'";

	&printLog (9, "Executing: $cmdline");

	my $result = `$cmdline`;

	my @resultLines = split(/\n/, $result);

	foreach my $line (@resultLines) {
	    chomp($line);

	    if ($line =~ /ERROR/i) {                # whatever error
		&printLog ( 2, "$line" );
	    }
	    elsif ($line =~ /^userVo = '(.*)'$/) {  # the user VO!
		$userVo = $1;
	    }
	    else {                                  # whatever comment
		&printLog ( 9, "$line" );
	    }

	}

        return $userVo;
}
