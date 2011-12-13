#!/usr/bin/perl -w


use strict;
use XML::LibXML;
use Getopt::Long;
use HTTP::Date;

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
	recordID => $jobid,	
	createTime => HTTP::Date::time2isoz(HTTP::Date::str2time($urList{URCREATION})),
	globalJobId => $jobid,
	localJobId => $urList{LRMSID} ,
	localUserId => $urList{USER} ,
	globalUserName => $usrcert,
	charge => "0",
	status => "completed",
	queue => $urList{QUEUE},
	group => $urList{group},
	jobName => $urList{jobName},
	ceCertificateSubject => $urList{"ceCertificateSubject"},
	wallDuration => "PT".$urList{"WALL_TIME"}."S",
	cpuDuration => "PT".$urList{"CPU_TIME"}."S",
	endTime => HTTP::Date::time2isoz($urList{"end"}),
	startTime => HTTP::Date::time2isoz($urList{"start"}),
	machineName => $urList{SiteName},
	projectName => $urList{userVo},
	ceHost => $urList{ceHostName},
	hosts => $urList{execHost},
	physicalMemory => $urList{PMEM},
	virtualMemory => $urList{VMEM},
	sericeLevelSi2K => $urList{si2k},
	sericeLevelSf2K => $urList{sf2k},
	serviceLevelGlueCEInfoTotalCPUs => $urList{GlueCEInfoTotalCPUs},
	timeInstantCTime => HTTP::Date::time2isoz($urList{"ctime"}),
	timeInstantQTime => HTTP::Date::time2isoz($urList{"qtime"}),
	timeInstantETime => HTTP::Date::time2isoz($urList{"etime"}),
	dgasAccountingProcedure => $urList{accountingProcedure},
	vomsFqan => $urList{fqan},
	execCe => $urList{execCe},
	lrmsServer => $urList{lrmsServer},
	voOrigin => $urList{voOrigin},
);

my @executingNodes  = split('\+',$record{hosts});

if ($urList{exitStatus} != 0 )
{
	$record{status} = "failed";
}
else
{
	$record{status} = "completed";
}

$record{"createTime"} =~ s/\s/T/;
$record{"endTime"} =~ s/\s/T/;
$record{"startTime"} =~ s/\s/T/;
$record{"timeInstantCTime"} =~ s/\s/T/;
$record{"timeInstantQTime"} =~ s/\s/T/;
$record{"timeInstantETime"} =~ s/\s/T/;
($record{"physicalMemory"},$record{"physicalMemoryUnit"}) = ( $record{"physicalMemory"} =~ /^(\d*)(.*)$/ );
($record{"virtualMemory"},$record{"virtualMemoryUnit"}) = ( $record{"virtualMemory"} =~ /^(\d*)(.*)$/ );

my $file = 'record.xml';
my $schemaFile = 'ur_v1.xsd.xml';

my $doc = XML::LibXML::Document->new('1.0', 'utf-8');

my $root = $doc->createElementNS( 'http://schema.ogf.org/urf/2003/09/urf', 'JobUsageRecord' );
$doc->setDocumentElement( $root );
$root->setNamespace( 'http://schema.ogf.org/urf/2003/09/urf' , 'urf', 0 );
$root->setNamespace( 'http://www.w3.org/2001/XMLSchema-instance' , 'xsi', 0 );
$root->setNamespace( 'http://www.w3.org/2000/09/xmldsig#' , 'ds', 0 );

my $RecordIdentityTag = $doc->createElement("RecordIdentity");
	$RecordIdentityTag->setAttribute('urf:recordId',$record{"recordID"});
	$RecordIdentityTag->setAttribute('urf:createTime',$record{"createTime"});
	
	my $dsKeyInfoTag1 = $RecordIdentityTag->addNewChild( '', "ds:KeyInfo" );
	$dsKeyInfoTag1->setAttribute('xmlns:ds','http://www.w3.org/2000/09/xmldsig#');
	$dsKeyInfoTag1->setAttribute('Id','RecordIdentityX509');
		my $dsKeyNameTag1 = $dsKeyInfoTag1->addNewChild( '', "ds:KeyName" );
		$dsKeyNameTag1->appendTextNode($record{"ceCertificateSubject"});
		my $dsRetrievalMethodTag1 = $dsKeyInfoTag1->addNewChild( '', "ds:RetrievalMethod" );
		$dsRetrievalMethodTag1->setAttribute('URI','#RecordIdentityX509');
		$dsRetrievalMethodTag1->setAttribute('Type','http://www.w3.org/2000/09/xmldsig#X509Data');
		my $dsX509DataTag1 = $dsKeyInfoTag1->addNewChild( '', "ds:X509Data" );
			my $dsX509SubjectNameTag1 = $dsX509DataTag1->addNewChild( '', "ds:X509SubjectName" );
			$dsX509SubjectNameTag1->appendTextNode($record{"ceCertificateSubject"});

my $JobIdentityTag = $doc->createElement("JobIdentity");
	my $GlobalJobIdTag = $JobIdentityTag->addNewChild( '', "GlobalJobId" );
	$GlobalJobIdTag->appendTextNode($record{"globalJobId"});
	
	my $LocalJobIdTag = $JobIdentityTag->addNewChild( '', "LocalJobId" );
	$LocalJobIdTag->appendTextNode($record{"localJobId"});
	
	

my $UserIdentityTag = $doc->createElement("UserIdentity");
	my $LocalUserIdTag = $UserIdentityTag->addNewChild( '', "LocalUserId" );
	$LocalUserIdTag->appendTextNode($record{"localUserId"});
	my $GlobalUserNameTag = $UserIdentityTag->addNewChild( '', "GlobalUserName" );
	$GlobalUserNameTag->appendTextNode($record{"globalUserName"});
	my $dsKeyInfoTag2 = $UserIdentityTag->addNewChild( '', "ds:KeyInfo" );
	$dsKeyInfoTag2->setAttribute('Id','UserIdentityX509');
		my $dsKeyNameTag2 = $dsKeyInfoTag2->addNewChild( '', "ds:KeyName" );
		$dsKeyNameTag2->appendTextNode($record{"globalUserName"});
		my $dsRetrievalMethodTag2 = $dsKeyInfoTag2->addNewChild( '', "ds:RetrievalMethod" );
		$dsRetrievalMethodTag2->setAttribute('URI','#UserIdentityX509');
		$dsRetrievalMethodTag2->setAttribute('Type','http://www.w3.org/2000/09/xmldsig#X509Data');
		my $dsX509DataTag2 = $dsKeyInfoTag2->addNewChild( '', "ds:X509Data" );
			my $dsX509SubjectNameTag2 = $dsX509DataTag2->addNewChild( '', "ds:X509SubjectName" );
			$dsX509SubjectNameTag2->appendTextNode($record{"globalUserName"});
	
my $ChargeTag = $doc->createElement("Charge");
	$ChargeTag->appendTextNode($record{"charge"});

my $StatusTag = $doc->createElement("Status");
	$StatusTag->appendTextNode($record{"status"});

my $WallDurationTag = $doc->createElement("WallDuration");
	$WallDurationTag->appendTextNode($record{"wallDuration"});

my $CpuDurationTag = $doc->createElement("CpuDuration");
	$CpuDurationTag->appendTextNode($record{"cpuDuration"});

my $EndTimeTag = $doc->createElement("EndTime");
	$EndTimeTag->appendTextNode($record{"endTime"});

my $StartTimeTag = $doc->createElement("StartTime");
	$StartTimeTag->appendTextNode($record{"startTime"});

my $MachineNameTag = $doc->createElement("MachineName");
	$MachineNameTag->appendTextNode($record{"machineName"});

my $HostTag = $doc->createElement("Host");
	$HostTag->appendTextNode($record{"ceHost"});
	$HostTag->setAttribute('urf:primary','true');
	$HostTag->setAttribute('urf:description','Computing Element');

my @nodeTags;

foreach my $node (@executingNodes)
{

	my $Host1Tag = $doc->createElement("Host");
		my $nodeName,my $processor;
		($nodeName,$processor) = ( $node =~ /^(.*)\/(\d+)$/);
		$Host1Tag->appendTextNode("$nodeName");
		$Host1Tag->setAttribute("urf:description","WorkerNodeProcessor$processor");
		push(@nodeTags,$Host1Tag);
}

my $QueueTag = $doc->createElement("Queue");
	$QueueTag->appendTextNode($record{"queue"});

my $ProjectNameTag = $doc->createElement("ProjectName");
	$ProjectNameTag->appendTextNode($record{"projectName"});

my $JobNameTag = $doc->createElement("JobName");
	$JobNameTag->appendTextNode($record{"jobName"});

my $Memory1 = $doc->createElement("Memory");
	$Memory1->appendTextNode($record{"physicalMemory"});
	$Memory1->setAttribute('urf:metric','max');
	$Memory1->setAttribute('urf:storageUnit','KB');
	$Memory1->setAttribute('urf:type','Physical');

my $Memory2 = $doc->createElement("Memory");
	$Memory2->appendTextNode($record{"virtualMemory"});
	$Memory2->setAttribute('urf:metric','max');
	$Memory2->setAttribute('urf:storageUnit','KB');
	$Memory2->setAttribute('urf:type','Virtual');

my $ProcessorsTag = $doc->createElement("Processors");
	$ProcessorsTag->appendTextNode($#executingNodes+1);

my $TimeInstantCTimeTag = $doc->createElement("TimeInstant");
	$TimeInstantCTimeTag->setAttribute('urf:type',"ctime");
	$TimeInstantCTimeTag->appendTextNode($record{"timeInstantCTime"});

my $TimeInstantQTimeTag = $doc->createElement("TimeInstant");
	$TimeInstantQTimeTag->setAttribute('urf:type',"qtime");
	$TimeInstantQTimeTag->appendTextNode($record{"timeInstantQTime"});


my $TimeInstantETimeTag = $doc->createElement("TimeInstant");
	$TimeInstantETimeTag->setAttribute('urf:type',"etime");
	$TimeInstantETimeTag->appendTextNode($record{"timeInstantETime"});

my $ServiceLevelSi2KTag = $doc->createElement("ServiceLevel");
	$ServiceLevelSi2KTag->setAttribute('urf:type',"si2k");
	$ServiceLevelSi2KTag->appendTextNode($record{"sericeLevelSi2K"});

my $ServiceLevelSf2KTag = $doc->createElement("ServiceLevel");
	$ServiceLevelSf2KTag->setAttribute('urf:type',"sf2k");
	$ServiceLevelSf2KTag->appendTextNode($record{"sericeLevelSf2K"});

my $ServiceLevelGlueCEInfoTotalCPUsTag = $doc->createElement("ServiceLevel");
	$ServiceLevelGlueCEInfoTotalCPUsTag->setAttribute('urf:type',"GlueCEInfoTotalCPUs");
	$ServiceLevelGlueCEInfoTotalCPUsTag->appendTextNode($record{"serviceLevelGlueCEInfoTotalCPUs"});

my $ResourceDgasAccountingProcedureTag = $doc->createElement("Resource");
	$ResourceDgasAccountingProcedureTag->setAttribute('urf:description',"DgasAccountingProcedure");
	$ResourceDgasAccountingProcedureTag->appendTextNode($record{"dgasAccountingProcedure"});

my $ResourceVomsFqanTag = $doc->createElement("Resource");
	$ResourceVomsFqanTag->setAttribute('urf:description',"VomsFqan");
	$ResourceVomsFqanTag->appendTextNode($record{"vomsFqan"});

my $ResourceGroupTag = $doc->createElement("Resource");
	$ResourceGroupTag->setAttribute('urf:description',"LocalGroupId");
	$ResourceGroupTag->appendTextNode($record{"group"});

my $ResourceExecCeTag = $doc->createElement("Resource");
	$ResourceExecCeTag->setAttribute('urf:description',"ExecCe");
	$ResourceExecCeTag->appendTextNode($record{"execCe"});

my $ResourceLrmsServerTag = $doc->createElement("Resource");
	$ResourceLrmsServerTag->setAttribute('urf:description',"LrmsServer");
	$ResourceLrmsServerTag->appendTextNode($record{"lrmsServer"});

my $ResourceVoOriginTag = $doc->createElement("Resource");
	$ResourceVoOriginTag->setAttribute('urf:description',"DgasVoOrigin");
	$ResourceVoOriginTag->appendTextNode($record{"voOrigin"});

$root->appendChild($RecordIdentityTag);
$root->appendChild($JobIdentityTag);
$root->appendChild($UserIdentityTag);
$root->appendChild($JobNameTag);
$root->appendChild($ChargeTag);
$root->appendChild($StatusTag);
$root->appendChild($Memory1);
$root->appendChild($Memory2);
$root->appendChild($TimeInstantCTimeTag);
$root->appendChild($TimeInstantQTimeTag);
$root->appendChild($TimeInstantETimeTag);
$root->appendChild($ServiceLevelSi2KTag);
$root->appendChild($ServiceLevelSf2KTag);
$root->appendChild($ServiceLevelGlueCEInfoTotalCPUsTag);
$root->appendChild($WallDurationTag);
$root->appendChild($CpuDurationTag);
$root->appendChild($ProcessorsTag);
$root->appendChild($EndTimeTag);
$root->appendChild($StartTimeTag);
$root->appendChild($MachineNameTag);
$root->appendChild($HostTag);

foreach my $nodeTag ( @nodeTags )
{
	$root->appendChild($nodeTag);
}

$root->appendChild($QueueTag);
$root->appendChild($ProjectNameTag);
$root->appendChild($ResourceGroupTag);
$root->appendChild($ResourceDgasAccountingProcedureTag);
$root->appendChild($ResourceVomsFqanTag);
$root->appendChild($ResourceExecCeTag);
$root->appendChild($ResourceLrmsServerTag);
$root->appendChild($ResourceVoOriginTag);


#my %tags = (
#    RecordIdentity => '',
#    JobIdentity => '',
#    UserIdentity => '',
#);


#for my $name (keys %tags) {
#    my $tag = $doc->createElement($name);
#    my $value = $tags{$name};
#    $tag->appendTextNode($value);
#    $root->appendChild($tag);
#}

$doc->setDocumentElement($root);
print $doc->toString(1);
