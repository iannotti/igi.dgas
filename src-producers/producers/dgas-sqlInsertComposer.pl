#!/usr/bin/perl -w


use strict;
use Getopt::Long;
use HTTP::Date;
use Digest::MD5 qw(md5 md5_hex md5_base64);

my $jobid ="";
my $resgridid ="";
my $usrcert ="";

GetOptions (
	'jobid=s' => \$jobid,
	'j=s' => \$jobid,
	'resgridid=s' => \$resgridid,
	'usrcert=s' => \$usrcert
);

my %urList;

foreach my $item (@ARGV)
{
	 my ( $key, $val );
	 if (($key, $val ) = ($item =~ /^(.+?)=(.*)$/))
	 {
	 	$urList{$key} = $val;	
	 }
}

my %record = (
	dgJobId => $jobid,	
	date => $urList{URCREATION},
	localJobId => $urList{LRMSID} ,
	localUserId => $urList{USER} ,
	globalUserName => $usrcert,
	status => "completed",
	exitStatus => $urList{exitStatus},
	queue => $urList{QUEUE},
	localGroup => $urList{group},
	jobName => $urList{jobName},
	ceCertificateSubject => $urList{"ceCertificateSubject"},
	wallTime => $urList{"WALL_TIME"},
	cpuTime => $urList{"CPU_TIME"},
	endTime => $urList{"end"},
	startTime => $urList{"start"},
	submitHost => $urList{execCe},
	site => $urList{SiteName},
	userVo => $urList{userVo},
	ceHost => $urList{ceHostName},
	hosts => $urList{execHost},
	physicalMemory => $urList{PMEM},
	virtualMemory => $urList{VMEM},
	si2k => $urList{si2k},
	sf2k => $urList{sf2k},
	serviceLevelGlueCEInfoTotalCPUs => $urList{GlueCEInfoTotalCPUs},
	timeInstantCTime => HTTP::Date::time2isoz($urList{"ctime"}),
	timeInstantQTime => HTTP::Date::time2isoz($urList{"qtime"}),
	timeInstantETime => HTTP::Date::time2isoz($urList{"etime"}),
	dgasAccountingProcedure => $urList{accountingProcedure},
	infrastructure => "unknown",
	vomsFqan => $urList{fqan},
	lrmsServer => $urList{lrmsServer},
	voOrigin => $urList{voOrigin},
	projectName => $urList{projectName},
	amount => $urList{amount},
);


my @executingNodes  = split('\+',$record{hosts});
my $numNodes = $#executingNodes+1;

if ($urList{exitStatus} != 0 )
{
	$record{status} = "failed";
}
else
{
	$record{status} = "completed";
}

if ($urList{"accountingProcedure"} eq "grid" )
{
	$record{"infrastructure"} = "grid";
}
else
{
	$record{"infrastructure"} = "local";
}


( $record{"machineName"}, $record{"port"}, $record{"ceSuffix"}) = ( $urList{"execCe"} =~ /^(.*):(\d*)\/(.*)$/ );
$record{"createTime"} =~ s/\s/T/;
$record{"endTime"} =~ s/\s/T/;
$record{"startTime"} =~ s/\s/T/;
$record{"timeInstantCTime"} =~ s/\s/T/;
$record{"timeInstantQTime"} =~ s/\s/T/;
$record{"timeInstantETime"} =~ s/\s/T/;
($record{"vomsGroup"},$record{"vomsRole"},$record{"vomsCapability"}) = ( $record{"vomsFqan"} =~ /^\/(.*)\/Role=(.*)\/Capability=(.*)$/ );


my $uniqueChecksum = $record{"machineName"} . $record{"localJobId"} . $urList{"start"} . $urList{"WALL_TIME"} . $urList{"CPU_TIME"}; 
$record{"recordID"} = md5_hex($uniqueChecksum);


my $queryString;
$queryString = "INSERT INTO jobTransSummary SET";
$queryString += "dgJobId=\"$record{dgJobId}\",";
$queryString += "date=\"$record{date}\",";
$queryString += "gridResource=\"$record{execCe}\",";
$queryString += "gridUser=\"$record{globalUserName}\",";
$queryString += "userFqan=\"$record{vomsFqan}\",";
$queryString += "userVo=\"$record{userVo}\",";
$queryString += "cpuTime=\"$record{cpuTime}\",";
$queryString += "wallTime=\"$record{walltime}\",";
$queryString += "pmem=\"$record{physicalMemory}\",";
$queryString += "vmem=\"$record{virtualMemory}\",";
$queryString += "amount=\"$record{amount}\",";
$queryString += "start=\"$record{startTime}\",";
$queryString += "end=\"$record{endTime}\",";
$queryString += "iBench=\"$record{si2k}\",";
$queryString += "iBenchType=\"si2k\",";
$queryString += "fBench=\"$record{sf2k}\",";
$queryString += "fBenchType=\"sf2k\",";
$queryString += "lrmsId=\"$record{localJobId}\",";
$queryString += "localUserId=\"$record{localUserId}\",";
$queryString += "localGroup=\"$record{localGroup}\",";
$queryString += "endDate=\"$record{endTime}\",";
$queryString += "siteName=\"$record{site}\",";
$queryString += "accountingProcedure=\"$record{dgasAccountingProcedure}\",";
$queryString += "voOrigin=\"$record{voOrigin}\",";
$queryString += "GlueCEInfoTotalCPUs=\"$record{serviceLevelGlueCEInfoTotalCPUs}\",";
$queryString += "executingNodes=\"$record{hosts}\",";
$queryString += "numNodes=\"$numNodes\",";
$queryString += "uniqueChecksum=\"$uniqueChecksum\"";
print $queryString;



#my $doc = XML::LibXML::Document->new('1.0', 'utf-8');
#
#my $root = $doc->createElementNS( 'http://eu-emi.eu/namespaces/2011/11/computerecord', 'UsageRecord' );
#$doc->setDocumentElement( $root );
#$root->setNamespace( 'http://eu-emi.eu/namespaces/2011/11/computerecord' , 'urf', 0 );
#$root->setNamespace( 'http://www.w3.org/2001/XMLSchema-instance' , 'xsi', 0 );
#$root->setAttribute( 'xsi:schemaLocation' , 'http://eu-emi.eu/namespaces/2011/11/computerecord car_v1.0.xsd ') ;
#
#my $RecordIdentityTag = $doc->createElement("RecordIdentity");
#	$RecordIdentityTag->setAttribute('urf:recordId',$record{"recordID"});
#	$RecordIdentityTag->setAttribute('urf:createTime',$record{"createTime"});
#	
#my $JobIdentityTag = $doc->createElement("JobIdentity");
#	
#	if ( ($record{"infrastructure"}) ne "local" )
#	{
#		my $GlobalJobIdTag = $JobIdentityTag->addNewChild( '', "GlobalJobId" );
#		$GlobalJobIdTag->appendTextNode($record{"globalJobId"});
#	}	
#	my $LocalJobIdTag = $JobIdentityTag->addNewChild( '', "LocalJobId" );
#	$LocalJobIdTag->appendTextNode($record{"localJobId"});
#	
#my $UserIdentityTag = $doc->createElement("UserIdentity");
#	
#	if ( ($record{"infrastructure"}) ne "local" )
#	{
#		my $GlobalUserNameTag = $UserIdentityTag->addNewChild( '', "GlobalUserName" );
#		$GlobalUserNameTag->appendTextNode($record{"globalUserName"});	
#		my $GroupTag = $UserIdentityTag->addNewChild('', "Group");
#		$GroupTag->appendTextNode($record{"group"});
#	
#		my $GroupAttributeVomsFqanTag = $UserIdentityTag->addNewChild('',"GroupAttribute");
#		$GroupAttributeVomsFqanTag->setAttribute('urf:type',"FQAN");
#		$GroupAttributeVomsFqanTag->appendTextNode($record{"vomsFqan"});
#		
#		my $GroupAttributeVomsGroupTag = $UserIdentityTag->addNewChild('',"GroupAttribute");
#		$GroupAttributeVomsGroupTag->setAttribute('urf:type',"group");
#		$GroupAttributeVomsGroupTag->appendTextNode($record{"vomsGroup"});
#	
#		my $GroupAttributeVomsRoleTag = $UserIdentityTag->addNewChild('',"GroupAttribute");
#		$GroupAttributeVomsRoleTag->setAttribute('urf:type',"role");
#		$GroupAttributeVomsRoleTag->appendTextNode($record{"vomsRole"});
#	}
#
#	my $GroupAttributeProjectTag = $UserIdentityTag->addNewChild('',"GroupAttribute");
#	$GroupAttributeProjectTag->setAttribute('urf:type',"ProjectName");
#	$GroupAttributeProjectTag->appendTextNode($record{"projectName"});
#	
#my $LocalUserIdTag = $UserIdentityTag->addNewChild( '', "LocalUserId" );
#	$LocalUserIdTag->appendTextNode($record{"localUserId"});
#
#my $LocalGroupTag = $UserIdentityTag->addNewChild( '', "LocalGroup" );
#	$LocalGroupTag->appendTextNode($record{"localGroup"});
#
#my $ChargeTag = $doc->createElement("Charge");
#	$ChargeTag->appendTextNode($record{"charge"});
#
#my $StatusTag = $doc->createElement("Status");
#	$StatusTag->appendTextNode($record{"status"});
#
#my $ExitStatusTag = $doc->createElement("ExitStatus");
#	$ExitStatusTag->appendTextNode($record{"exitStatus"});
#
#my $WallDurationTag = $doc->createElement("WallDuration");
#	$WallDurationTag->appendTextNode($record{"wallDuration"});
#
#my $CpuDurationTag = $doc->createElement("CpuDuration");
#	$CpuDurationTag->setAttribute('urf:usageType',"all");
#	$CpuDurationTag->appendTextNode($record{"cpuDuration"});
#
#my $EndTimeTag = $doc->createElement("EndTime");
#	$EndTimeTag->appendTextNode($record{"endTime"});
#
#my $StartTimeTag = $doc->createElement("StartTime");
#	$StartTimeTag->appendTextNode($record{"startTime"});
#
#my $MachineNameTag = $doc->createElement("MachineName");
#	$MachineNameTag->appendTextNode($record{"machineName"});
#
#my $SubmitHostTag = $doc->createElement("SubmitHost");
#	$SubmitHostTag->appendTextNode($record{"submitHost"});
#	$SubmitHostTag->setAttribute('urf:type','CE-ID');
#
#my @nodeTags;
#my @nodeNames;
#
#foreach my $node (@executingNodes)
#{
#
#	my $Host1Tag = $doc->createElement("Host");
#		my $nodeName,my $processor;
#		($nodeName,$processor) = ( $node =~ /^(.*)\/(\d+)$/);
#		$Host1Tag->appendTextNode("$nodeName");
#		$Host1Tag->setAttribute("urf:description","WorkerNodeProcessor$processor");
#		push(@nodeTags,$Host1Tag);
#	        if ( grep(/^$nodeName$/, @nodeNames) == 0)
#		{ 
#			push(@nodeNames,$nodeName);
#		}
#}
#
#my $QueueTag = $doc->createElement("Queue");
#	$QueueTag->appendTextNode($record{"queue"});
#
#my $SiteTag = $doc->createElement("Site");
#	$SiteTag->setAttribute('urf:type', "GOCDB");
#	$SiteTag->appendTextNode($record{"site"});
#
#my $SiteX509Tag = $doc->createElement("Site");
#	$SiteX509Tag->setAttribute('urf:type', "X509DN");
#	$SiteX509Tag->appendTextNode($record{"ceCertificateSubject"});
#
#my $JobNameTag = $doc->createElement("JobName");
#	$JobNameTag->appendTextNode($record{"jobName"});
#
#my $Memory1 = $doc->createElement("Memory");
#	$Memory1->appendTextNode($record{"physicalMemory"});
#	$Memory1->setAttribute('urf:metric','max');
#	$Memory1->setAttribute('urf:storageUnit','KB');
#	$Memory1->setAttribute('urf:type','Physical');
#
#my $Memory2 = $doc->createElement("Memory");
#	$Memory2->appendTextNode($record{"virtualMemory"});
#	$Memory2->setAttribute('urf:metric','max');
#	$Memory2->setAttribute('urf:storageUnit','KB');
#	$Memory2->setAttribute('urf:type','Shared');
#
#my $NodeCountTag = $doc->createElement("NodeCount");
#	$NodeCountTag->appendTextNode($#nodeNames+1);
#
#my $ProcessorsTag = $doc->createElement("Processors");
#	$ProcessorsTag->appendTextNode($#executingNodes+1);
#
#my $TimeInstantCTimeTag = $doc->createElement("TimeInstant");
#	$TimeInstantCTimeTag->setAttribute('urf:type',"ctime");
#	$TimeInstantCTimeTag->appendTextNode($record{"timeInstantCTime"});
#
#my $TimeInstantQTimeTag = $doc->createElement("TimeInstant");
#	$TimeInstantQTimeTag->setAttribute('urf:type',"qtime");
#	$TimeInstantQTimeTag->appendTextNode($record{"timeInstantQTime"});
#
#
#my $TimeInstantETimeTag = $doc->createElement("TimeInstant");
#	$TimeInstantETimeTag->setAttribute('urf:type',"etime");
#	$TimeInstantETimeTag->appendTextNode($record{"timeInstantETime"});
#
#my $ServiceLevelSi2KTag = $doc->createElement("ServiceLevel");
#	$ServiceLevelSi2KTag->setAttribute('urf:type',"si2k");
#	$ServiceLevelSi2KTag->appendTextNode($record{"sericeLevelSi2K"});
#
#my $ServiceLevelSf2KTag = $doc->createElement("ServiceLevel");
#	$ServiceLevelSf2KTag->setAttribute('urf:type',"sf2k");
#	$ServiceLevelSf2KTag->appendTextNode($record{"sericeLevelSf2K"});
#
#my $ServiceLevelGlueCEInfoTotalCPUsTag = $doc->createElement("ServiceLevel");
#	$ServiceLevelGlueCEInfoTotalCPUsTag->setAttribute('urf:type',"GlueCEInfoTotalCPUs");
#	$ServiceLevelGlueCEInfoTotalCPUsTag->appendTextNode($record{"serviceLevelGlueCEInfoTotalCPUs"});
#
#my $InfrastructureTag = $doc->createElement("Infrastructure");
#	$InfrastructureTag->setAttribute('urf:type',$record{"infrastructure"});
#
#my $ResourceDgasAccountingProcedureTag = $doc->createElement("Resource");
#	$ResourceDgasAccountingProcedureTag->setAttribute('urf:description',"DgasAccountingProcedure");
#	$ResourceDgasAccountingProcedureTag->appendTextNode($record{"dgasAccountingProcedure"});
#
#my $ResourceLrmsServerTag = $doc->createElement("Resource");
#	$ResourceLrmsServerTag->setAttribute('urf:description',"LrmsServer");
#	$ResourceLrmsServerTag->appendTextNode($record{"lrmsServer"});
#
#my $ResourceVoOriginTag = $doc->createElement("Resource");
#	$ResourceVoOriginTag->setAttribute('urf:description',"DgasVoOrigin");
#	$ResourceVoOriginTag->appendTextNode($record{"voOrigin"});
#
#$root->appendChild($RecordIdentityTag);
#$root->appendChild($JobIdentityTag);
#$root->appendChild($UserIdentityTag);
#$root->appendChild($JobNameTag);
#$root->appendChild($ChargeTag);
#$root->appendChild($StatusTag);
#$root->appendChild($ExitStatusTag);
#$root->appendChild($InfrastructureTag);
#$root->appendChild($WallDurationTag);
#$root->appendChild($CpuDurationTag);
#$root->appendChild($ServiceLevelSi2KTag);
#$root->appendChild($ServiceLevelSf2KTag);
#$root->appendChild($ServiceLevelGlueCEInfoTotalCPUsTag);
#$root->appendChild($Memory1);
#$root->appendChild($Memory2);
#$root->appendChild($TimeInstantCTimeTag);
#$root->appendChild($TimeInstantQTimeTag);
#$root->appendChild($TimeInstantETimeTag);
#$root->appendChild($NodeCountTag);
#$root->appendChild($ProcessorsTag);
#$root->appendChild($EndTimeTag);
#$root->appendChild($StartTimeTag);
#$root->appendChild($MachineNameTag);
#$root->appendChild($SubmitHostTag);
#$root->appendChild($QueueTag);
#$root->appendChild($SiteTag);
#$root->appendChild($SiteX509Tag);
#
#foreach my $nodeTag ( @nodeTags )
#{
#	$root->appendChild($nodeTag);
#}
#
#$root->appendChild($ResourceDgasAccountingProcedureTag);
#$root->appendChild($ResourceLrmsServerTag);
#$root->appendChild($ResourceVoOriginTag);
#
#
##my %tags = (
##    RecordIdentity => '',
##    JobIdentity => '',
##    UserIdentity => '',
##);
#
#
##for my $name (keys %tags) {
##    my $tag = $doc->createElement($name);
##    my $value = $tags{$name};
##    $tag->appendTextNode($value);
##    $root->appendChild($tag);
##}
#
#$doc->setDocumentElement($root);
#print $doc->toString(1);
