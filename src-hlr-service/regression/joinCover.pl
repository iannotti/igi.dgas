#!/usr/bin/perl
use Term::ANSIColor;
use strict;

my $COVFILE = $ARGV[0];
my $SOURCECODE = $ARGV[1];

print "reading $COVFILE and joining it with $SOURCECODE\n";
my %hitsTable = ();
my $hitCounter = 0;
open ( COVFILEH, "coverperl $COVFILE |" ) || die "Couldn't open $COVFILE!\n";
while ( <COVFILEH> )
{
	my $line = $_;
	chop ($line);
	for ($line) {           # trim white space in $variable, cheap
       		s/^\s+//;
        	s/\s+$//;
        }
	if ( $line =~ "line" )
	{
		my $hit;
		my $lineNumber;
		for ($line) {           # trim white space in $variable, cheap
		       s/line//;
                }
		($hit, $lineNumber) = split(/\s+/, $line, 2);
		$hitsTable{$lineNumber} = $hit;
		if ( $hit >= "1" ) 
		{
			$hitCounter++;
		}
	}
	if ( $line =~ "::" )
	{
		#manege subrutines.	
		if ( $line =~ /(\d*).*::(.*)/ )
		{
			$hitsTable{$2} = $1;
			if ( $1 >= "1" ) 
			{
				$hitCounter++;
			}
		}
	}
	
}

close COVFILEH;

my $lineCounter = 0;
open ( SOURCEH, "cat --number $SOURCECODE |" ) || die "Couldn't open $SOURCECODE!\n";
while ( <SOURCEH> )
{
	my $n;
	my $code;
	my $flag = " ";
	my $line =$_;
	chop $line;
	for ( $line ) {
		s/^\s+//;
	}
	($n, $code ) = split(/\s/, $line, 2);
 	my $countBuff = "";
	if ( exists $hitsTable{$n} )
	{
		$flag = "+";
		$lineCounter++;
		$countBuff = $hitsTable{$n};
	}
	else
	{
		$countBuff=" ";
	}
	my $color ="";
	if ( ( $countBuff eq "0" ) )
	{
		$color = color("bold red");
	}
	print "$flag",$color,"$n => $countBuff\t :",color("reset"),"  $code\n";
} 
close SOURCEH;
my $coverage = $hitCounter/$lineCounter;
print "\nCrossed lines: $hitCounter; Source lines: $lineCounter; Per line coverage: $coverage \n";
