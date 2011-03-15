#!/usr/bin/perl -w -I /usr/lib/perl5/vendor_perl/5.8.5 -I /usr/lib/perl5/vendor_perl/5.8.0

use strict;

use POSIX;
use Getopt::Std;
use DBI();
use Time::Local;
use strict;

# no buffering of output:
select STDERR; $| = 1;
select STDOUT; $| = 1;

my $NUM_LATEST_SUMRECORDS_TO_CHECK = 3;

my $systemLogLevel = 7;


my $prgname = "glite-dgas-sumrecords2goc.pl";

if ( !exists($ENV{DGAS_LOCATION}) || $ENV{DGAS_LOCATION} eq "" ) {
        $ENV{DGAS_LOCATION}="/usr/";
	&printLog (3, "Environment variable DGAS_LOCATION not defined! Trying /usr/ ...\n");
}

my $cfname = "glite-dgas-sumrecords2goc.conf";
my $conffile = "/etc/dgas/$cfname";


my $useVOlist = 0;
my %vo_list = ();  # checked after SELECT on HLR DB, before INSERT in GOC DB
my $vo_list_location = "";

my $useCElist = 0;
my @ce_list = ();  # integrated in SELECT on HLR DB
my $ce_list_location = "";

my $useSitelist = 0;
my %site_list = (); # checked after SELECT on HLR DB, before INSERT in GOC DB
my $site_list_location = "";

my $processLocalJobs = 0; # default: only jobs with voOrigin=[fqan,pool].

my $checkAllGOCdata = 0;

# -------------------
# getting options ...
# -------------------

my %opts;
$opts{h} = "";
$opts{c} = "";

getopts('hc:', \%opts);


if ( $opts{h} eq "1" ) {
    &printUsageInfo();
}

if ( $opts{c} ne "" ) {
    $conffile = $opts{c};
}


# ------------------------------
# parsing of configuration file:
# ------------------------------

# defaults for GOC db (summary records):
# This is the database where the SumCPU records are written
my $gocDbServer = "localhost";
my $gocDbPort = 3306;
my $gocDbUser = "root";
my $gocDbPassword = "";
my $gocDbName = "dgas2goc";
my $gocDbSumCPUTable = "SumCPU";

# defaults for DGAS HLR db:
# This is the database where the original HLR accounting information is taken
# from.
my $hlrDbServer = "localhost";
my $hlrDbPort = 3306;
my $hlrDbUser = "root";
my $hlrDbPassword = "";
my $hlrDbName = "hlr";

my $minStopTimeEpoch = 0;    # for conf. parameter PROCESS_FROM_JOB_ENDTIME
my $beforeStopTimeEpoch = 0; # for conf. parameter PROCESS_BEFORE_JOB_ENDTIME


# read configuration from file:
&readConfiguration();


# -----------------------
# connecting to MySQL dbs
# -----------------------

my $gocDbh;
my $hlrDbh;

my $gocDbQuery;
my $hlrDbQuery;

my $gocSth;
my $hlrSth;

&connectToDatabases();



# -------------------------------
# starting aggregation procedure:
# -------------------------------

my $exitStatus = 0; # 0 if ok

&printLog (1, "Starting forwarding of summary records to GOC DB ...\n");

# first get the aggregated records from jobTransSummary:

my $sumRows_ref;

$exitStatus = &getSummaryRecordsFromJobTransSummary($sumRows_ref);

$exitStatus = &processAndInsertSummaryRecords($sumRows_ref);

&printLog (1, "Forwarding of summary records completed. Exiting ...\n");

exit ($exitStatus);




# ===========================================================================
#                               FUNCTIONS
# ===========================================================================


sub printLog 
{   
        my $logLevel = $_[0];
        my $log = $_[1]; 
		if ( $logLevel <= int($systemLogLevel) )        {
		my $localtime=localtime();       
                print "$localtime: ".$log;
        }
}


# ---------------------------------------------------------------------------
#
# argument: MySQL date ("YYYY-MM-DD")
# returns a timestamp in seconds since Jan 1st, 1970
#
# ---------------------------------------------------------------------------
sub sqlDate2Timestamp {
    my $sqlDate = $_[0];

    my $dateTimestamp = 0;

    if ($sqlDate =~ /^(\d{4})-(\d{2})-(\d{2})$/) {
	my $year = $1 - 1900;
	my $mon = $2 - 1; # range from 0 to 11
	my $mday = $3;
	eval {
	    $dateTimestamp = timegm(0,0,0,$mday,$mon,$year);
	};
	if ($@) {
	    $dateTimestamp = 0; # error, leave 0!
	}
    }
    else {
	$dateTimestamp = 0; # error, leave 0!
    }

    return $dateTimestamp;
}


# ---------------------------------------------------------------------------
#
# read configuration file and parse parameters:
# arguments: none
#
# ---------------------------------------------------------------------------
sub readConfiguration() {

    # first read the file:
    my $line = "";

    unless ( -e $conffile ) {
	&printLog(1, "Error: configuration file $conffile not found!\n");
	exit(1);
    }

    unless ( -r $conffile ) {
	&printLog(1, "Error: configuration file $conffile not readable!\n");
	exit(1);
    }
         
    unless ( open(IN, "< $conffile") ) {
	&printLog(1, "Error: cannot open configuration file $conffile!\n");
        exit(1);
    }

    my %confparameters;

    # reading the single lines
    while ($line = <IN>) {
	chomp($line);

	$line || next;

	next if ($line =~ /^\s*\#+/);

	if ($line =~ /([^\s]*)\s*=\s*\"(.*)\"/) {

	    $confparameters{$1} = $2;

	}

    }

    # close the configuration file
    close(IN);


    # now parse the parameters:

    if (exists($confparameters{"systemLogLevel"}) &&
	$confparameters{"systemLogLevel"} ne "") {
	$systemLogLevel = int($confparameters{"systemLogLevel"});
    }

    # get GOC db info:

    if (exists($confparameters{"GOC_DB_SERVER"})) {
	$gocDbServer = $confparameters{"GOC_DB_SERVER"};
    }
    else {
	&printLog (7, "Configuration parameter GOC_DB_SERVER missing, trying '$gocDbServer'!\n");
    }

    if (exists($confparameters{"GOC_DB_PORT"})) {
	$gocDbPort = $confparameters{"GOC_DB_PORT"};
    }
    else {
	&printLog (7, "Configuration parameter GOC_DB_PORT missing, trying '$gocDbPort'!\n");
    }

    if (exists($confparameters{"GOC_DB_USER"})) {
	$gocDbUser = $confparameters{"GOC_DB_USER"};
    }
    else {
	&printLog (7, "Configuration parameter GOC_DB_USER missing, trying '$gocDbUser'!\n");
    }

    if (exists($confparameters{"GOC_DB_PASSWORD"})) {
	$gocDbPassword = $confparameters{"GOC_DB_PASSWORD"};
    }
    else {
	&printLog (7, "Configuration parameter GOC_DB_PASSWORD missing, trying '$gocDbPassword'!\n");
    }

    if (exists($confparameters{"GOC_DB_NAME"})) {
	$gocDbName = $confparameters{"GOC_DB_NAME"};
    }
    else {
	&printLog (7, "Configuration parameter GOC_DB_NAME missing, trying '$gocDbName'!\n");
    }

    if (exists($confparameters{"GOC_DB_SUMCPU_TABLE"})) {
	$gocDbSumCPUTable = $confparameters{"GOC_DB_SUMCPU_TABLE"};
    }
    else {
	&printLog (7, "Configuration parameter GOC_DB_SUMCPU_TABLE missing, trying '$gocDbSumCPUTable'!\n");
    }



    # get DGAS HLR db info:

    if (exists($confparameters{"HLR_DB_SERVER"})) {
	$hlrDbServer = $confparameters{"HLR_DB_SERVER"};
    }
    else {
	&printLog (7, "Configuration parameter HLR_DB_SERVER missing, trying '$hlrDbServer'!\n");
    }

    if (exists($confparameters{"HLR_DB_PORT"})) {
	$hlrDbPort = $confparameters{"HLR_DB_PORT"};
    }
    else {
	&printLog (7, "Configuration parameter HLR_DB_PORT missing, trying '$hlrDbPort'!\n");
    }

    if (exists($confparameters{"HLR_DB_USER"})) {
	$hlrDbUser = $confparameters{"HLR_DB_USER"};
    }
    else {
	&printLog (7, "Configuration parameter HLR_DB_USER missing, trying '$hlrDbUser'!\n");
    }

    if (exists($confparameters{"HLR_DB_PASSWORD"})) {
	$hlrDbPassword = $confparameters{"HLR_DB_PASSWORD"};
    }
    else {
	&printLog (7, "Configuration parameter HLR_DB_PASSWORD missing, trying '$hlrDbPassword'!\n");
    }

    if (exists($confparameters{"HLR_DB_NAME"})) {
	$hlrDbName = $confparameters{"HLR_DB_NAME"};
    }
    else {
	&printLog (7, "Configuration parameter HLR_DB_NAME missing, trying '$hlrDbName'!\n");
    }


    # get list of accepted VOs if specified:
    if (exists($confparameters{"VO_LIST_LOCATION"})) {

	$useVOlist = 1; # use the VO list to limit usage records.

	$vo_list_location = $confparameters{"VO_LIST_LOCATION"};

	my $vListStr = "";

	# try to read the VO list from the location:
	if ( $vo_list_location =~ /^http:\/\// ) {
	    $vListStr = `wget -q --output-document=- $vo_list_location`;
	}
	else {
	    $vListStr = `cat $vo_list_location`;
	}

	if ( $vListStr ne "" ) {
	    my @voTokens = split(/\n/, $vListStr);
	    my $counter;
	    my $voListLog = "Aggregating usage records for VOs listed in '$vo_list_location': ";

	    for ($counter = 0; $counter < scalar(@voTokens); $counter++) {
		if ( $voTokens[$counter] =~ /^([\._\-a-zA-Z0-9]+)\s*.*$/ ) {
		    $vo_list{$1} = 1;

		    $voListLog .= "".$1.", ";
		}
	    }
	    $voListLog .= "... Number of VOs loaded: ".scalar(keys(%vo_list))."\n";

	    &printLog (3, $voListLog);
	}
	else {
	    &printLog (1, "Error: Configuration parameter VO_LIST_LOCATION pointing to an empty location, quitting!\n");
	    exit(1);
	}
    }


    # get list of accepted CEs if specified:
    if (exists($confparameters{"RESOURCE_LIST_LOCATION"})) {

	$useCElist = 1; # use the CE list to limit usage records.

	$ce_list_location = $confparameters{"RESOURCE_LIST_LOCATION"};

	my $cListStr = "";

	# try to read the CE list from the location:
	if ( $ce_list_location =~ /^http:\/\// ) {
	    $cListStr = `wget -q --output-document=- $ce_list_location`;
	}
	else {
	    $cListStr = `cat $ce_list_location`;
	}

	if ( $cListStr ne "" ) {
	    my @ceTokens = split(/\n/, $cListStr);
	    my $counter;
	    my $ceListLog = "Aggregating usage records for resources (CEs) listed in '$ce_list_location': ";

	    for ($counter = 0; $counter < scalar(@ceTokens); $counter++) {
		if ( $ceTokens[$counter] !~ /^\#/     # not a commented line!
		     && $ceTokens[$counter] =~ /^([^\s]*)\s*.*$/ ) {
		    my $thisCEString = $1;

		    $ceListLog .= "".$thisCEString.", ";

                    # replace configuration wildcards for MySQL regexp
		    $thisCEString =~ s/\*/%/g;
		    push (@ce_list, $thisCEString);
		}
	    }
	    $ceListLog .= "... Number of CEs/resources loaded: ".scalar(@ce_list)."\n";

	    &printLog (3, $ceListLog);
	}
	else {
	    &printLog (1, "Error: Configuration parameter RESOURCE_LIST_LOCATION pointing to an empty location, quitting!\n");
	    exit(1);
	}
    }


    # get list of accepted sites if specified:
    if (exists($confparameters{"SITE_LIST_LOCATION"})) {

	$useSitelist = 1; # use the site list to limit usage records.

	$site_list_location = $confparameters{"SITE_LIST_LOCATION"};

	my $sListStr = "";

	# try to read the site list from the location:
	if ( $site_list_location =~ /^http:\/\// ) {
	    $sListStr = `wget -q --output-document=- $site_list_location`;
	}
	else {
	    $sListStr = `cat $site_list_location`;
	}

	if ( $sListStr ne "" ) {
	    my @siteTokens = split(/\n/, $sListStr);
	    my $counter;
	    my $siteListLog = "Aggregating usage records for sites listed in '$site_list_location': ";

	    for ($counter = 0; $counter < scalar(@siteTokens); $counter++) {
		if ( $siteTokens[$counter] !~ /^\#/     # not a commented line!
		     && $siteTokens[$counter] =~ /^([^\s]*)\s*.*$/ ) {
		    my $thisSiteString = $1;

		    $siteListLog .= "".$thisSiteString.", ";

                    # not passed to MySQL anymore
                    # # replace configuration wildcards for MySQL regexp
		    #$thisSiteString =~ s/\*/%/g;
		    $site_list{$thisSiteString} = 1;
		}
	    }
	    $siteListLog .= "... Number of sites loaded: ".scalar(keys(%site_list))."\n";

	    &printLog (3, $siteListLog);
	}
	else {
	    &printLog (1, "Error: Configuration parameter SITE_LIST_LOCATION pointing to an empty location, quitting!\n");
	    exit(1);
	}
    }


    # process also local jobs (voOrigin=map)?
    if (exists($confparameters{"PROCESS_LOCAL_JOBS"})) {
	if ($confparameters{"PROCESS_LOCAL_JOBS"} =~ /^yes$/i) {
	    $processLocalJobs = 1;
	}
	elsif (! ($confparameters{"PROCESS_LOCAL_JOBS"} =~ /^no$/i) ) {
	    &printLog (1, "Error: Configuration parameter PROCESS_LOCAL_JOBS has to be either \"yes\" or \"no\", quitting!\n");
	    exit(1);
	}
    }


    # check ALL GOC data instead of only last two existing records?
    if (exists($confparameters{"CHECK_ALL_GOC_DATA"})) {
	if ($confparameters{"CHECK_ALL_GOC_DATA"} =~ /^yes$/i) {
	    $checkAllGOCdata = 1;
	}
	elsif (! ($confparameters{"CHECK_ALL_GOC_DATA"} =~ /^no$/i) ) {
	    &printLog (1, "Error: Configuration parameter CHECK_ALL_GOC_DATA has to be either \"yes\" or \"no\", quitting!\n");
	    exit(1);
	}
    }


    # process only jobs from a certain date (EndTime) or later?
    if (exists($confparameters{"PROCESS_FROM_JOB_ENDTIME"})
	&& $confparameters{"PROCESS_FROM_JOB_ENDTIME"} ne "") {

	&printLog (3, "Reading configuration parameter PROCESS_FROM_JOB_ENDTIME: \"".$confparameters{"PROCESS_FROM_JOB_ENDTIME"}."\"\n");
	$minStopTimeEpoch = &sqlDate2Timestamp($confparameters{"PROCESS_FROM_JOB_ENDTIME"});
	if ($minStopTimeEpoch == 0) {
	    &printLog (3, "Configuration parameter PROCESS_FROM_JOB_ENDTIME (".$confparameters{"PROCESS_FROM_JOB_ENDTIME"}.") has wrong format: should be \"YYYY-MM-DD\", ignoring!\n");
	}
	else {
	    &printLog (3, "=> processing only jobs with execution _end_ timestamp >= $minStopTimeEpoch!\n");
	}
    }


    # process only jobs before a certain date (EndTime)?
    if (exists($confparameters{"PROCESS_BEFORE_JOB_ENDTIME"})
	&& $confparameters{"PROCESS_BEFORE_JOB_ENDTIME"} ne "") {

	&printLog (3, "Reading configuration parameter PROCESS_BEFORE_JOB_ENDTIME: \"".$confparameters{"PROCESS_BEFORE_JOB_ENDTIME"}."\"\n");
	$beforeStopTimeEpoch = &sqlDate2Timestamp($confparameters{"PROCESS_BEFORE_JOB_ENDTIME"});
	if ($beforeStopTimeEpoch == 0) {
	    &printLog (3, "Configuration parameter PROCESS_BEFORE_JOB_ENDTIME (".$confparameters{"PROCESS_BEFORE_JOB_ENDTIME"}.") has wrong format: should be \"YYYY-MM-DD\", ignoring!\n");
	}
	else {
	    &printLog (3, "=> processing only jobs with execution _end_ timestamp < $beforeStopTimeEpoch!\n");
	}
    }


} # readConfiguration() ...


# ---------------------------------------------------------------------------
#
# connecting to the databases (GOC, DGAS HLR)
# argument: none
#
# ---------------------------------------------------------------------------
sub connectToDatabases() {

    # GOC conversion db:

    eval { $gocDbh = DBI->connect("DBI:mysql:database=$gocDbName;host=$gocDbServer",
				   "$gocDbUser", "$gocDbPassword",
				   {'RaiseError' => 1}
				   )
	   };
    if ($@) {
	&printLog (1, "Error: Cannot connect to GOC database! Error: $@\n");
	exit(1);
    }

    # DGAS HLR db:

    eval { $hlrDbh = DBI->connect("DBI:mysql:database=$hlrDbName;host=$hlrDbServer",
				  "$hlrDbUser", "$hlrDbPassword",
				  {'RaiseError' => 1}
				  )
	   };
    if ($@) {
	&printLog (1, "Error: Cannot connect to DGAS HLR database! Error: $@\n");
	exit(1);
    }

} # connectToDatabases() ...



# ---------------------------------------------------------------------------
#
# Print usage information (help).
# arguments: none
#
# ---------------------------------------------------------------------------
sub printUsageInfo() {
    print "Authors: Rosario Piro (piro\@to.infn.it), Andrea Guarise (guarise\@to.infn.it)\n\n";
    print "Usage:\n";
    print "$prgname [OPTIONS]\n";
    print "\nWhere OPTIONS are:\n";
    print "-c <file>    Specifies the configuration file.\n";
    print "             Default: /etc/dgas/$cfname\n";
    print "-v           Verbose log file: additional information of processing of the\n";
    print "             single SumCPU records will be printed to the standard output.\n";
    exit(1);

} # printUsageInfo() ...




# ---------------------------------------------------------------------------
#
# Print usage information (help).
#
# ---------------------------------------------------------------------------
sub getSummaryRecordsFromJobTransSummary() {
    my @sumRows = ();
    my $row;
    my $retVal = 0; # ok

    &printLog(5, "Getting data from HLR jobTransSummary ...\n");

    $hlrDbQuery = "SELECT siteName AS ExecutingSite, userVO AS LCGUserVO, count(*) AS Njobs, ROUND(sum(cpuTime)/3600) as SumCPU, ROUND(sum(cpuTime*iBench)/(1000*3600)) as NormSumCPU, ROUND(sum(wallTime)/3600) as SumWCT, ROUND(sum(wallTime*iBench)/(1000*3600)) as NormSumWCT, MONTH(endDate) AS Month, YEAR(endDate) AS Year, Min(endDate) AS RecordStart, Max(endDate) AS RecordEnd FROM jobTransSummary WHERE siteName != \"\" AND (voOrigin = \"fqan\" OR voOrigin = \"pool\"";

    if ($processLocalJobs) {
	$hlrDbQuery .= " OR voOrigin = \"map\"";
    }

    $hlrDbQuery .= ")";

    if ($minStopTimeEpoch > 0) {
	$hlrDbQuery .= " AND end >= $minStopTimeEpoch";
    }
    if ($beforeStopTimeEpoch > 0) {
	$hlrDbQuery .= " AND end < $beforeStopTimeEpoch";
    }

    if ($useCElist) {
	$hlrDbQuery .= " AND (";
	my $ii;
	for ($ii = 0; $ii < scalar(@ce_list); $ii++) {

	    $hlrDbQuery .= "thisGridId ";
	    if ($ce_list[$ii] =~ /%/) {
		$hlrDbQuery .= "LIKE";
	    }
	    else {
		$hlrDbQuery .= "=";
	    }
	    $hlrDbQuery .= " \"$ce_list[$ii]\"";

	    if ($ii != scalar(@ce_list) - 1) { # not the last CE
		$hlrDbQuery .= " OR ";
	    }
	}
	$hlrDbQuery .= ")";
    }

    $hlrDbQuery .= " GROUP BY ExecutingSite, LCGUserVO, Year, Month ORDER BY ExecutingSite, LCGUserVO, RecordEnd DESC;";

    &printLog(9, "Executing on HLR DB: $hlrDbQuery\n");


    eval { $hlrSth = $hlrDbh->prepare($hlrDbQuery) };
    if ($@) {
	&printLog(1, "Query preparation failed! Error: $@\n");
	return 1; # error
    }

    eval { $hlrSth->execute() };
    if ($@) {
	&printLog(1, "Query execution failed! Error: $@\n");
	return 1; # error
    }

    my $numRow = 0;

    while ($row = $hlrSth->fetchrow_hashref()) {

	$numRow++;

	&printLog(9, "found summary record \#$numRow: ExecutingSite=$row->{'ExecutingSite'}, LCGUserVO=$row->{'LCGUserVO'}, Njobs=$row->{'Njobs'}, SumCPU=$row->{'SumCPU'}, NormSumCPU=$row->{'NormSumCPU'}, SumWCT=$row->{'SumWCT'}, NormSumWCT=$row->{'NormSumWCT'}, Month=$row->{'Month'}, Year=$row->{'Year'}, RecordStart=$row->{'RecordStart'}, RecordEnd=$row->{'RecordEnd'}.\n");

	push(@sumRows, $row);
    }

    $_[0] = \@sumRows;
    return $retVal; # ok?
}



# ---------------------------------------------------------------------------
#
# Check which records to insert into the GOC database and do so
#
# ---------------------------------------------------------------------------
sub processAndInsertSummaryRecords() {

    my @sumRows = @{$_[0]};

    my $row;
    my $retVal = 0; # ok


    my $thisVO = "";
    my $thisSite = "";

    my $checkedRecords_4_VOandSite = 0;

    foreach $row (@sumRows) { # already in the correct order

	# first check if this is one of the VOs to process:
	if ($useVOlist && !exists($vo_list{$row->{'LCGUserVO'}})) {
	    # we have a restricted VO list and this record's VO is not in it!
	    &printLog (7, "Ignoring record for ExecutingSite=\"$row->{'ExecutingSite'}\", LCGUserVO=\"$row->{'LCGUserVO'}\", Month=$row->{'Month'}, Year=$row->{'Year'}; VO not specified in $vo_list_location!\n");

	    next;
	}

	# first check if this is one of the sites to process:
	if ($useSitelist && !exists($site_list{$row->{'ExecutingSite'}})) {
	    # we have a restricted site list; this record's site isn't in it!
	    &printLog (7, "Ignoring record for ExecutingSite=\"$row->{'ExecutingSite'}\", LCGUserVO=\"$row->{'LCGUserVO'}\", Month=$row->{'Month'}, Year=$row->{'Year'}; site name not specified in $site_list_location!\n");

	    next;
	}


	if ($row->{'LCGUserVO'} ne $thisVO
	    || $row->{'ExecutingSite'} ne $thisSite) {
	    # this is another VO and/or another site; reset counters, etc.

	    $thisVO = $row->{'LCGUserVO'};
	    $thisSite = $row->{'ExecutingSite'};
	    $checkedRecords_4_VOandSite = 0;

	    &printLog (3, "Summary records for VO $thisVO at site $thisSite:\n");

	}
	elsif($checkedRecords_4_VOandSite >= $NUM_LATEST_SUMRECORDS_TO_CHECK) {
	    # same VO and site and we have already two updates for it, skip!

	    &printLog (7, "Ignoring record for ExecutingSite=\"$row->{'ExecutingSite'}\", LCGUserVO=\"$row->{'LCGUserVO'}\", Month=$row->{'Month'}, Year=$row->{'Year'}; already checked $NUM_LATEST_SUMRECORDS_TO_CHECK existing records for this VO and site!\n");

	    next;
	}

	# check if summary is different:
	my $oldrow;
	my $oldrow_exists = 0;
	$retVal += &getOldSummaryRecord($row, $oldrow, $oldrow_exists);

	if (!$oldrow_exists) {
	    # not yet present, needs to be inserted!
	    &printLog (5, "Inserting new record for ExecutingSite=\"$row->{'ExecutingSite'}\", LCGUserVO=\"$row->{'LCGUserVO'}\", Month=$row->{'Month'}, Year=$row->{'Year'}.\n");

	    $retVal += &insertSummaryRecord($row);
	}
	else {
	    if (&summaryRecordsDiffer($row, $oldrow)) {
		# present but needs to be updated:
		&printLog (5, "Updating record for ExecutingSite=\"$row->{'ExecutingSite'}\", LCGUserVO=\"$row->{'LCGUserVO'}\", Month=$row->{'Month'}, Year=$row->{'Year'}; previously: Njobs=$oldrow->{'Njobs'}, SumCPU=$oldrow->{'SumCPU'}, NormSumCPU=$oldrow->{'NormSumCPU'}, SumWCT=$oldrow->{'SumWCT'}, NormSumWCT=$oldrow->{'NormSumWCT'}; => now: Njobs=$row->{'Njobs'}, SumCPU=$row->{'SumCPU'}, NormSumCPU=$row->{'NormSumCPU'}, SumWCT=$row->{'SumWCT'}, NormSumWCT=$row->{'NormSumWCT'}.\n");

		$retVal += &updateSummaryRecord($row);
	    }
	    else {
		&printLog (5, "Summary record for ExecutingSite=\"$row->{'ExecutingSite'}\", LCGUserVO=\"$row->{'LCGUserVO'}\", Month=$row->{'Month'}, Year=$row->{'Year'} is unchanged (Njobs=$row->{'Njobs'}, SumCPU=$row->{'SumCPU'}, NormSumCPU=$row->{'NormSumCPU'}, SumWCT=$row->{'SumWCT'}, NormSumWCT=$row->{'NormSumWCT'}) ... skipping.\n");
	    }

	    # not a new record (either updated or the same as before)
	    if (!$checkAllGOCdata) {
		$checkedRecords_4_VOandSite++;
	    }
	}

    }


    return $retVal; # ok?

}


# ---------------------------------------------------------------------------
#
# Update summary record in the GOC DB ...
#
# ---------------------------------------------------------------------------
sub updateSummaryRecord() {

    my $row = $_[0];

    my $retVal = 0; # ok

    # primary key is the combination of ExecutingSite,LCGUserVO,Month,Year
    $gocDbQuery = "UPDATE $gocDbSumCPUTable SET Njobs = $row->{'Njobs'}, SumCPU = $row->{'SumCPU'}, NormSumCPU = $row->{'NormSumCPU'}, SumWCT = $row->{'SumWCT'}, NormSumWCT = $row->{'NormSumWCT'}, RecordStart = \"$row->{'RecordStart'}\", RecordEnd = \"$row->{'RecordEnd'}\" WHERE ExecutingSite = \"$row->{'ExecutingSite'}\" AND LCGUserVO = \"$row->{'LCGUserVO'}\" AND Month = $row->{'Month'} AND Year = $row->{'Year'};";

    &printLog(9, "Executing on GOC DB: $gocDbQuery\n");

    eval { $gocSth = $gocDbh->prepare($gocDbQuery) };
    if ($@) {
	&printLog(1, "Query preparation failed! Error: $@\n");
	return 1; # error
    }

    eval { $gocSth->execute() };
    if ($@) {
	&printLog(1, "Query execution failed! Error: $@\n");
	return 1; # error
    }

    return $retVal; # ok?
}




# ---------------------------------------------------------------------------
#
# Insert new summary record in the GOC DB ...
#
# ---------------------------------------------------------------------------
sub insertSummaryRecord() {

    my $row = $_[0];

    my $retVal = 0; # ok

    # primary key is the combination of ExecutingSite,LCGUserVO,Month,Year
    $gocDbQuery = "INSERT INTO $gocDbSumCPUTable VALUES (\"$row->{'ExecutingSite'}\", \"$row->{'LCGUserVO'}\", $row->{'Njobs'}, $row->{'SumCPU'}, $row->{'NormSumCPU'}, $row->{'SumWCT'}, $row->{'NormSumWCT'}, $row->{'Month'}, $row->{'Year'}, \"$row->{'RecordStart'}\", \"$row->{'RecordEnd'}\");";

    &printLog(9, "Executing on GOC DB: $gocDbQuery\n");

    eval { $gocSth = $gocDbh->prepare($gocDbQuery) };
    if ($@) {
	&printLog(1, "Query preparation failed! Error: $@\n");
	return 1; # error
    }

    eval { $gocSth->execute() };
    if ($@) {
	&printLog(1, "Query execution failed! Error: $@\n");
	return 1; # error
    }

    return $retVal; # ok?
}




# ---------------------------------------------------------------------------
#
# Get a specific summary record from the GOC DB ...
#
# ---------------------------------------------------------------------------
sub getOldSummaryRecord() {

    my $row_toget = $_[0]; # defines for what to search
                           # $_[1] is where to put the record found in the DB
                           # $_[2] is where to specify if we found something

    my $retVal = 0; # ok

    # primary key is the combination of ExecutingSite,LCGUserVO,Month,Year
    $gocDbQuery = "SELECT * FROM $gocDbSumCPUTable WHERE ExecutingSite = \"$row_toget->{'ExecutingSite'}\" AND LCGUserVO = \"$row_toget->{'LCGUserVO'}\" AND Month = $row_toget->{'Month'} AND Year = $row_toget->{'Year'};";

    &printLog(9, "Executing on GOC DB: $gocDbQuery\n");

    eval { $gocSth = $gocDbh->prepare($gocDbQuery) };
    if ($@) {
	&printLog(1, "Query preparation failed! Error: $@\n");
	return 1; # error
    }

    eval { $gocSth->execute() };
    if ($@) {
	&printLog(1, "Query execution failed! Error: $@\n");
	return 1; # error
    }

    if ($_[1] = $gocSth->fetchrow_hashref()) {
	# we found the record:
	$_[2] = 1;

	&printLog(9, "found old record for ExecutingSite=\"$row_toget->{'ExecutingSite'}\", LCGUserVO=\"$row_toget->{'LCGUserVO'}\", Month=$row_toget->{'Month'}, Year=$row_toget->{'Year'}.\n");
    }
    else {
	$_[2] = 0;

		&printLog(9, "no old record for ExecutingSite=\"$row_toget->{'ExecutingSite'}\", LCGUserVO=\"$row_toget->{'LCGUserVO'}\", Month=$row_toget->{'Month'}, Year=$row_toget->{'Year'}.\n");
    }

    return $retVal; # ok?
}



# ---------------------------------------------------------------------------
#
# Check if two summary records differ for the most important metrics ...
#
# ---------------------------------------------------------------------------
sub summaryRecordsDiffer() {

    my $row1 = $_[0];
    my $row2 = $_[1];


    # we check only the metrics NOT the other nomianl record info such as
    # ExecutingSite, LCGUserVO ... make sure that you compare two records
    # of which you can at least expect to be equal! 

    if ( $row1->{'Njobs'} != $row2->{'Njobs'}
	 || $row1->{'SumCPU'} != $row2->{'SumCPU'}
	 || $row1->{'SumWCT'} != $row2->{'SumWCT'}
	 || $row1->{'NormSumCPU'} != $row2->{'NormSumCPU'}
	 || $row1->{'NormSumWCT'} != $row2->{'NormSumWCT'} ) {

	&printLog(9, "summary metrics for record from HLR ($row1->{'ExecutingSite'}, $row1->{'LCGUserVO'}, $row1->{'Month'}/$row1->{'Year'}) and record at GOC ($row2->{'ExecutingSite'}, $row2->{'LCGUserVO'}, $row2->{'Month'}/$row2->{'Year'}) differ!\n");

	return 1; # rows differ!

    }

    &printLog(9, "no difference in summary metrics between record from HLR ($row1->{'ExecutingSite'}, $row1->{'LCGUserVO'}, $row1->{'Month'}/$row1->{'Year'}) and record at GOC ($row2->{'ExecutingSite'}, $row2->{'LCGUserVO'}, $row2->{'Month'}/$row2->{'Year'}).\n");

    return 0; # rows don't differ!
}

