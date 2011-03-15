#!/usr/bin/perl
#This script queries a BDII and creates HLR accounts for
#the CEs registered with this BDII.
#Authors: Andrea Guarise, andrea.guarise@to.infn.it,
#         Rosario Piro, piro@to.infn.it
#	  Simone Dalla Fina, simone.dallafina@pd.infn.it
# program flow: 1) get all CE hostnames(+port) from the BDII (1st LDAP session)
#               2) get all host certificate subjects via LDAP
#               3) get all other info from the BDII (one LDAP session per CE)
# Note: two different LDAP sessions where necessary since the openssl command
#       has a very long timeout that may cause still open LDAP sessions to
#       fail due to a broken pipe. 

use strict;

use Net::LDAP;
use Getopt::Std;

# turn off buffering of STDOUT
$| = 1;

my @ceHostDomainNames = ();
my @createdAccounts = ();

my $bdiiHost = "";
my $bdiiPort = 2170; # default
my $ldapBase = "o=Grid";
my $ceHostFilter = "";
my $useCEHostName = "";
my $createLocalQueueAccounts = "yes";
my $forceAccountCreation = "no";
my $forceFlag = "";
my $dryRun = "no";
my $printCommands = "no";

my @ceIDs = ();
my %ce_hostCert_map = (); # to store all ceIDs and host certificate subjects

my $allHostCertString = ""; # used only for option -U!

my %opts = ();

getopts('fhp:s:b:v:H:U:Nc:DC', \%opts);

if ( $ENV{DGAS_LOCATION} eq "" ) {
    $ENV{DGAS_LOCATION}="/usr/";
}


# first check if help is needed
if ( exists($opts{h}) && $opts{h} eq "1" ) {
    &printUsageInfo();
    exit(0);
}

# first read the conf file, if it is specified:
if ( exists($opts{c}) && $opts{c} ne "" ) {
    &parseConf($opts{c});
}


# now get parameters from the command line (priority over conf file):

if ( exists($opts{s}) && $opts{s} ne "" ) { $bdiiHost = $opts{s}; }
if ( exists($opts{p}) && $opts{p} ne "" ) { $bdiiPort = $opts{p}; }
if ( exists($opts{b}) && $opts{b} ne "" ) { $ldapBase = $opts{b}; }
if ( exists($opts{v}) && $opts{v} ne "" ) { } #for backward compatibility
if ( exists($opts{H}) && $opts{H} ne "" ) { $ceHostFilter = $opts{H}; }
if ( exists($opts{U}) && $opts{U} ne "" ) { $useCEHostName = $opts{U}; }
if ( exists($opts{N}) && $opts{N} eq "1" ) { $createLocalQueueAccounts = "no";}
if ( exists($opts{f}) && $opts{f} eq "1" ) { $forceAccountCreation = "yes"; }
if ( exists($opts{D}) && $opts{D} eq "1" ) { $dryRun = "yes"; }
if ( exists($opts{C}) && $opts{C} eq "1" ) { $printCommands = "yes"; }


# check if the parameters are ok, before proceeding:
if ( $bdiiHost eq "" ) {
    print "Error: Hostname of BDII server not specified.\n";
    &printUsageInfo();
    exit(11);
}
print "Using BDII server: $bdiiHost\n";

if ( $bdiiPort !~ /^\d+$/ ) {
    print "Error: Port of BDII server has to be numeric.\n";
    &printUsageInfo();
    exit(11);
}
print "BDII server port: $bdiiPort\n";

print "LDAP query base string: $ldapBase\n";

if ( $ceHostFilter eq "" ) { # no filter, take all
    print "No filter for CE host names specified, creating accounts for all CEs found on the BDII.\n";
}
else {
    print "Filter for CE host names specified, creating accounts only for CEs with the following hostnames/domainnames: ";
    @ceHostDomainNames = split (/,/, $ceHostFilter);
    foreach my $hname (@ceHostDomainNames) {
	print "$hname; ";
    }
    print "\n";
}

if ( $createLocalQueueAccounts eq "no" ) {
    print "Warning: no accounts will be created for locally submitted jobs (createLocalQueueAccounts = \"no\")!\n";
}
else {
    $createLocalQueueAccounts = "yes"; # default
    print "Accounts will also be created for locally submitted jobs\n";
}

if ( $useCEHostName ne "" ) {
    print "Important: Replacing all CE host names for account creation by: $useCEHostName\n";
}

if ( $forceAccountCreation eq "yes" ) { $forceFlag = "-F"; };

# first LDAP session: get only CE hostnames + ports ("<hostname>:<port>")
print "--- Trying to retrive CE host names and ports from BDII (LDAP):\n";

my $ldap = Net::LDAP->new($bdiiHost, port=>$bdiiPort);
if (!defined($ldap)) {
    print "Error! Cannot connect to BDII server $bdiiHost on port $bdiiPort!\n";
    exit(2);
}
$ldap->bind;
my $result = &LDAPsearch($ldap, "(objectclass=*)", $ldapBase);
my @entries = $result->entries;


foreach my $entry (@entries)  
{
    	my @members = $entry->get_value("GlueCEUniqueID");
    	foreach my $member (@members) 	
    	{
    		if ($member =~ /^(.+):(\d+)\/.*-([^\-]*)$/i) 
		{
		   	print "found: $1:$2\n";
			if ( $ceHostFilter ne "" ) 
			{
				foreach my $okHostDomain (@ceHostDomainNames) 
				{
					if ( $1 =~ $okHostDomain )
					{
						push(@ceIDs, $member); # keep the CE IDs for later
					}
				}
			}
			else
			{
				push(@ceIDs, $member); # keep the CE IDs for later
			}
		}
	}
}
$ldap->unbind; # end first ldap session

foreach my $ce (@ceIDs)
{
	print "Creating accounts for:$ce\n";
}

# second LDAP session: get all info and create accounts!
print "--- Trying to retrive detailed info from BDII (LDAP) and creating HLR accounts for ".scalar(@ceIDs)." CEs:\n";

my %accountsCreated = ();

my %accountInfo = (
		   ceId => "",
		   mail => "",
		   description => "",
		   group => "",
		   rid => "",
		   queue => "",
		   hostCertSubject => ""
		   );
my $i=0;
foreach my $member (@ceIDs) {
    %accountInfo = (
		    ceId => "",
		    mail => "",
		    description => "",
		    group => "",
		    rid => "",
		    ceHostname => "",
		    queue => "",
		    hostCertSubject => ""
		    );

    my $cePort = 0;

    # first get CE hostname and queue name:
    if ($member =~ /^(.+):(\d+)\/.*-([^\-]*)$/i) {
	$accountInfo{ceHostname} = $1;
	$cePort = $2;
	$accountInfo{queue} = $3;
    }
    if ($accountInfo{ceHostname} eq ""
	|| $accountInfo{queue} eq "") {
	print "Warning: cannot parse GlueCEUniqueID '$member' ... skipping!\n";
	next;
    }

    my $dn;
    my $ceFQDN;
    my $glueGlueCEName;
    my $ceDomainName;	
    my $suggestedRid;
    $accountInfo{description}=$member;
    $accountInfo{ceId}=$member;

    # Retrieve DNs

    print "==== $accountInfo{description} - BEGIN ====\n";
    $ldap = Net::LDAP->new($bdiiHost, port=>$bdiiPort);
    if (!defined($ldap)) {
	print "Error! Cannot connect to BDII server $bdiiHost on port $bdiiPort!\n";
	exit(2);
    }
    $ldap->bind;
    # CE data
    my $result2 = &LDAPsearch($ldap, "(GlueCEUniqueID=$accountInfo{description})", $ldapBase);
    my @entries2 = $result2->entries;
    # CE certificate
    my $result3 = &LDAPsearch($ldap, "(GlueServiceDataKey=DN)", $ldapBase);
    my @entries3 = $result3->entries;
    $ldap->unbind;
		
    # CE data
    print "LDAP (on BDII) search for: GlueCEUniqueID=$accountInfo{description} with base: $ldapBase\n";
    print "-- CE data entries found: ".scalar(@entries2)."\n";
    if (scalar(@entries2) == 0) {
	print "Error: no detailed info found on BDII ... skipping!\n";
	next;
    }
    foreach my $entry2 (@entries2) {
	$dn = $entry2->dn();
	print "$dn\n";
	$ceFQDN= $entry2->get_value("GlueCEInfoHostName");
	$glueGlueCEName= $entry2->get_value("GlueCEName");
	$ceDomainName = "";
	if ( $ceFQDN =~ /^(.+?)\.(.*)/) {
	    $ceDomainName = $2;
	}
    }

    # CE certificate
    print "LDAP (on BDII) search for: GlueServiceDataKey=DN with base: $ldapBase\n";
    print "-- CE certificate entries found: ".scalar(@entries3)."\n";
    if (scalar(@entries3) == 0) {
	print "Error: no detailed info found on BDII ... skipping!\n";
	next;
    }
    if ( $useCEHostName ne "" )
    {
		foreach my $entry3 (@entries3) 
		{
			my $glueGlueChunkKey= $entry3->get_value("GlueChunkKey");
			my $glueGlueServiceDataValue= $entry3->get_value("GlueServiceDataValue");
			foreach my $hostName (@ceHostDomainNames)
			{
				if ( $glueGlueServiceDataValue =~ /^.*$hostName.*/ )
				{
					print "$glueGlueServiceDataValue\n";
					if ( $accountInfo{hostCertSubject} ne "" )
					{
						# not the first, add seperator!
						$accountInfo{hostCertSubject} .= ";";
					}
					$accountInfo{hostCertSubject} .= $glueGlueServiceDataValue;
				}
			}
		}
    }
    else
    {
	    foreach my $entry3 (@entries3) {
	        my $glueGlueChunkKey= $entry3->get_value("GlueChunkKey");
		my $glueGlueServiceDataValue= $entry3->get_value("GlueServiceDataValue");
	        if ( $glueGlueChunkKey =~ /GlueServiceUniqueID\s*=\s*$accountInfo{ceHostname}.*/ ) 
		{
	       	     if ( $glueGlueServiceDataValue =~ /^.*$accountInfo{ceHostname}.*/ ) {   # improvable regex
	                if ( $accountInfo{hostCertSubject} == "" ) {
	                    $accountInfo{hostCertSubject} = $glueGlueServiceDataValue;
	                }
	            }
	        }
	    }
    }

    # host certificate subject, if retrieved previously:
    if (!exists($accountInfo{hostCertSubject})) {
	print "Warning: not creating acccount(s) for: $member (filtered)\n";
	next;
    }
    elsif ($accountInfo{hostCertSubject} eq "") {
	print "Warning: not creating acccount(s) for: $member (subject not retrieved)\n";
	next;
    }
    print "-- Host certificate found for $accountInfo{ceHostname}: $accountInfo{hostCertSubject}\n";

    # In case we -U has been specified:
    # a) substitution of CE host name:
    # b) use _all_ host cert subjects!
    if ($useCEHostName ne "") {
	$accountInfo{ceHostname} = $useCEHostName;
	if ($accountInfo{ceId} =~ /^(.+):([^:]*)$/) {
	    $accountInfo{ceId} = "$useCEHostName:$2";
	    $accountInfo{description} = $accountInfo{ceId};
	}
	$ceFQDN = $useCEHostName;
	if ( $ceFQDN =~ /^(.+?)\.(.*)/) {
	    $ceDomainName = $2;
	}

	print "Substitution of CE host name: $useCEHostName; certificate subject(s): $accountInfo{hostCertSubject}\n";
    }


    $suggestedRid= $ceFQDN;
    $suggestedRid =~ s/\./_/g;
    $suggestedRid .= "_$glueGlueCEName";

    # make sure each resourceid (rid) exists only once:
    my $count = 2;
    my $newrid = $suggestedRid;
    while ( ( &resourceIDExists($newrid) == 0 && ( $dryRun  eq "no" )) ) {
	$newrid = $suggestedRid . $count;
	$count++;
    }
    $suggestedRid = $newrid;
    $accountInfo{rid}=$suggestedRid;


    $accountInfo{group}="${ceDomainName}";

    $i +=1;
    #print "\n$i";

    my $aref = \%accountInfo;

    if (!exists($accountsCreated{$accountInfo{ceId}})) {
	&createGridAccount($aref);
	$accountsCreated{$accountInfo{ceId}} = 1;
    }
    else {
	print "Warning: account already created for: $accountInfo{ceId} ... skipping!\n";
    }

    
    if ($createLocalQueueAccounts eq "yes") {
	if (!exists($accountsCreated{"$accountInfo{ceHostname}:$accountInfo{queue}"})) {
	    &createLocalAccount($aref);
	    $accountsCreated{"$accountInfo{ceHostname}:$accountInfo{queue}"} = 1;
	}
	else {
	    print "Warning: account already created for: $accountInfo{ceHostname}:$accountInfo{queue} ... skipping!\n";
	}
    }
    print "==== $accountInfo{description} - END ====\n";
}

print "The follwing accounts have been created:\n";
foreach my $account ( @createdAccounts )
{
	print "$account\n";
}


sub LDAPsearch() {
    my ($ldap,$searchString,$base,$attrs) = @_;

    # if they don't pass a base... set it for them
    if (!$base ) { $base = "o=grid"; }

    my $result = $ldap->search ( base    => "$base",
                                 scope   => "sub",
                                 filter  => "$searchString",
                                 attrs   =>  $attrs
                               );

    return $result;
}

sub mySystem() {
	my $cmd = $_[0];
	if ( $printCommands eq "yes" ) { print "$cmd\n"; }
	if ( $dryRun eq "no" )
	{
		return system($cmd);
	}
	else
	{
		return 0;
	}	
}

sub createGridAccount() {
    my %Info = %{$_[0]};
    my $prefix = "$ENV{DGAS_LOCATION}/sbin/";

    if ( $forceAccountCreation eq "no" && &resourceExists($Info{ceId}) == 0 ) {
	print "Warning: resource account exists for $Info{ceId} ... skipping!\n";
	return 0;
    }
    my $cmd = "$prefix/glite-dgas-hlr-addresource $forceFlag -r '$Info{rid}' -g '$Info{group}' -d '$Info{description}' -c '$Info{ceId}' -S '$Info{hostCertSubject}'";
    push ( @createdAccounts, "$Info{ceId}:$Info{hostCertSubject}");
    return &mySystem($cmd);
}

sub createLocalAccount() {
    my %Info = %{$_[0]};
    my $prefix = "$ENV{DGAS_LOCATION}/sbin/";

    if ( $forceAccountCreation eq "no" && &resourceExists("$Info{ceHostname}:$Info{queue}") == 0 ) {
	print "Warning: resource account (local) exists for $Info{ceHostname}:$Info{queue} ... skipping!\n";
	return 0;
    }

    my $cmd = "$prefix/glite-dgas-hlr-addresource $forceFlag -r '$Info{rid}_local' -g '$Info{group}' -d '$Info{description} (local)' -c '$Info{ceHostname}:$Info{queue}' -S '$Info{hostCertSubject}'";
    return &mySystem($cmd);
}

sub resourceExists {
    my $ceID = $_[0];
    my $prefix ="$ENV{DGAS_LOCATION}/sbin/";
    my $cmd = "$prefix/glite-dgas-hlr-queryresource -R -c $ceID 2> /dev/null";
    my $ret = &mySystem($cmd);
    print "Checking: ceId '$ceID' exists in HLR database...";
    if ( $ret == 0 )
    {
	print "yes\n";
    }
    else
    {
	print "no\n";
    }
    return $ret;
}


sub resourceIDExists {
    my $rid = $_[0];
    my $prefix ="$ENV{DGAS_LOCATION}/sbin/";
    my $cmd = "$prefix/glite-dgas-hlr-queryresource -R -r $rid 2> /dev/null";
    my $ret = &mySystem($cmd);
    print "Checking: rid '$rid' exists in HLR database...";
    if ( $ret == 0 )
    {
	print "yes\n";
    }
    else
    {
	print "no\n";
    }
    return $ret;
}

sub parseConf {
    my $fconf = $_[0];
    open(FILE, "$fconf") || &error("Error: Cannot open configuration file: $fconf\n");
    while(<FILE>) {
	if (/\$\{(.*)\}/) {
	    my $value=$ENV{$1};
	    s/\$\{$1\}/$value/g;
        }
	if(/^bdiiHost\s*=\s*\"(.*)\"$/){$bdiiHost=$1;}
	if(/^bdiiPort\s*=\s*\"(.*)\"$/){$bdiiPort=$1;}
	if(/^ldapBase\s*=\s*\"(.*)\"$/){$ldapBase=$1;}
	if(/^ceHostFilter\s*=\s*\"(.*)\"$/){$ceHostFilter=$1;}
	if(/^useCEHostName\s*=\s*\"(.*)\"$/){$useCEHostName=$1;}
	if(/^createLocalQueueAccounts\s*=\s*\"(.*)\"$/){$createLocalQueueAccounts=$1;}
	if(/^forceAccountCreation\s*=\s*\"(.*)\"$/){$forceAccountCreation=$1;}
    }
    close(FILE);
}


sub printUsageInfo {
    print "\nUsage:\n";
    print "glite-dgas-hlr-bdiiresimport.pl <OPTIONS>\n";
    print "\nWhere OPTIONS are:\n\n";
    print "-s <hostnmame>      Host name of the BDII server to contact.\n";
    print "-p <port>           Port number for the BDII server to query (default: 2170).\n";
    print "-b <base>           Base string for the query (Default=\"o=Grid\").\n";
    print "-v <voFlag>         Virtual Organisation flag (\"bdII\" or \"Mds-Vo-Name\").\n";
    print "                    \"bdII\": Selects the bdII FQDN as the resource's VO ID.\n";
    print "                    \"Mds-Vo-Name\": Selects the Mds-Vo-Name record\n";
    print "                                   as the VO ID to which the resource is\n";
    print "                                   associated in the HLR database.\n";
    print "                    Default: \"Mds-Vo-Name\".\n";
    print "-H <CEhost-filter>  Comma-seperated list of CE host names (or domain names)\n";
    print "                    for which to create resource accounts; if specified, all\n";
    print "                    other CEs found on the BDII will be ignored.\n";
    print "-U <useCEhostname>  Specifies one specific hostname for which to create\n";
    print "                    accounts. All CE IDs (and local queues if -N not used)\n";
    print "                    found on the BDII - for _all_ CEs that pass the filter \n";
    print "                    <CEhost-filter> - will be considered as if they had \n";
    print "                    <useCEhostname> as hostname (other hostnames will be \n";
    print "                    replaced).\n";
    print "-N                  Do not create accounts for local queues. Default is: for\n";
    print "                    each grid resource account created, create also an\n";
    print "                    account for jobs submitted locally to the queue\n";
    print "                    (\"<hostname>:<queue>\"). In case of doubt, leave the\n";
    print "                    default behaviour!\n";
    print "-f                  Force flag, if specified existing accounts are overwritten.\n";
    print "-c <conf-file>      Configuration file, that may contain the above mentioned\n";
    print "                    parameters as:\n";
    print "                       bdiiHost=\"<hostnmame>\"\n";
    print "                       bdiiPort=\"<port>\"\n";
    print "                       ldapBase=\"<base>\"\n";
    print "                       voFlag=\"<voFlag>\"\n";
    print "                       ceHostFilter=\"<CEhost-filter>\"\n";
    print "                       useCEHostName=\"<useCEhostname>\"\n";
    print "                       createLocalQueueAccounts=\"yes/no\"\n";
    print "                       forceAccountCreation=\"yes/no\"\n";
    print "                    (Parameters specified on the command line have priority).\n";
}
