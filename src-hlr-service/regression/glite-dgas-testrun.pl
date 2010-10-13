#!/usr/bin/perl
use Term::ANSIColor;
use Getopt::Long;
use Env qw($EDG_WL_TEST_LOG_DIR  $EDG_WL_TEST_CONF $PWD);


my $FAILS=0;
my $SUCCES=0;
my $COUNTER=0;
my $FAILED_TESTS="";
my $WARNING=0;
my $FORCE='';
my $VERBOSE='';
my $DEBUG='';
my $FILENAME=$ARGV[0];
my $LOGDIR="./";
my $CONFFILE="";
my $commandPrefix="";

Env::import();

$BASEDIR = `dirname $0`;
chomp $BASEDIR;

if ( $EDG_WL_TEST_LOG_DIR ne '' )
{
	$LOGDIR=$EDG_WL_TEST_LOG_DIR;
	if (!( -d $LOGDIR ))
	{
		`mkdir $LOGDIR`;
	}
}

GetOptions ("force" => \$FORCE, 
            "verbose" => \$VERBOSE, 
            "debug" => \$DEBUG, 
	"logdir=s" => \$LOGDIR,
	'conf=s' => \$CONFFILE
	);

$FILENAME=$ARGV[0];

# try opening the input file

if ( $VERBOSE )
{
	print "Logging in dir: $LOGDIR\n";
}
$EDG_WL_TEST_LOG_DIR=$LOGDIR;
if ( $FORCE )
{
	print "Warning: the test is in force mode!\n";
} 
if ( $CONFFILE ne '' )
{
	chomp $CONFFILE;
	if ( $VERBOSE )
	{
		print "Using configuartion file: $CONFFILE\n";
	}
	$EDG_WL_TEST_CONF=$CONFFILE;
	$commandPrefix = "$commandPrefix . $CONFFILE ;";
}
else
{
	print "You must specify the configuration file!\n";
	print "USAGE:\n\n";
	print "testRun <OPTIONS>\n\n";
	print "OPTIONS are:\n\n";
	print "--force      Force exiting with exit status 0\n";
	print "--verbose    Verbose output\n";
	print "--debug      Adds debug information\n";
	print "--conf	    MANDATORY, used to specify the main configuation file\n";
	print "--logdir     Used to specify where to log files\n";
	exit 1;
} 

@FILENAME_ARRAY= split /\//, $FILENAME;
$LOGFILENAME = pop @FILENAME_ARRAY;
$LOGFILE="$LOGDIR/$LOGFILENAME.log";
open (INPUT, $FILENAME);

#try opening the log file

open (LOG, ">> $LOGFILE");

while ( $line = <INPUT> )
{
	next if $line =~ /^\s*#/;
	$LOGCHANNEL=">>$LOGFILE";
	$MOVE_TO_COL="\033[50G";
	$BACKGROUND=0;
	$REDIRECT=0;
	$modifier ="";
	$isbold ="";
	$comment = 0;
	chomp $line;
	($name,$visible,$op,$exe)= split /:/, $line,4;
	s/"/\"/g, $exe;
	print LOG "\n";
	print LOG "################################################################################\n";
	print LOG "Performing test: $name\ncommand: ";
	
	@exeBuff = split /\s+/, $exe;
	$command="";
	foreach $token ( @exeBuff )
	{
		if ( $token eq "&" )
		{
			$BACKGROUND=1;
		}
		else
		{
		  $command = "$command $token";
		}
	}
	if ( $visible == 2 )
	{
		$comment = 1;
	}
	if ( $op eq "v" )
	{
		$LOGCHANNEL= "";
		$modifier= color("bold yellow"),"+";
		$isbold = "bold";
	#	$MOVE_TO_COL="\033[60G";
	}
	if ( $op =~ ">" )
	{
		$LOGCHANNEL= $op;
		$REDIRECT = 1;
	}
	$commandBuffer = $command;
	if ( $BACKGROUND == 1)
	{
		$command = "$command $LOGCHANNEL 2>&1 &"
	}
	else
	{
		$command = "$command $LOGCHANNEL 2>&1"
	}
	if ( $op eq "." )
	{
		$commandPrefix = "$commandPrefix . $command ;";
	}
	else
	{
		$command = "$commandPrefix $command";

		`$commandPrefix echo "$commandBuffer" $LOGCHANNEL 2>&1`;
		print LOG "--------------------------------------------------------------------------------\n";
		system($command);
		if ($BACKGROUND == 1)
		{
			sleep 5;
		}
		$exit_status  = $? >> 8;
       	 	$signal_num  = $? & 127;
       		$dumped_core = $? & 128;
		if ($REDIRECT == 1)
		{
			
		}
	}
	
	print LOG "\n";
	print LOG "--------------------------------------------------------------------------------\n";
	print LOG "test: $name\n log: $LOGCHANNEL\n exit status: $exit_status\n";
	print LOG "################################################################################\n";
	$expected_status = 0;
	if ( $visible >= 1 )
	{
		if ( $visible == 1) { print $modifier, "$COUNTER) ",$name,color("reset"),$MOVE_TO_COL; }
		if ( $visible == 2) { print $modifier,$name,color("reset"),$MOVE_TO_COL; }
		if ( substr($op,0,1) eq "!" )
		{
			my $new_exit_status = 255;
			my $buffer=substr($op,2);
			$buffer =~ s/\)//g;
			$expected_status = $buffer;
			my @w = split /,/, $buffer;
			foreach $check ( @w )
			{
				if ( $exit_status == $check )
				{
					if ( $DEBUG ){ print "!($check) matches!"};
					$new_exit_status = 0;
				}
			}
			if ( $buffer eq "a" )
                        {
				if ($DEBUG){ print "!a: Exit status ";}	
				if ( $exit_status != 0 )
				{
					if ( $DEBUG ){ print "!= 0";}
                                	$new_exit_status = 0;
				}
				else
				{
					if ( $DEBUG ){ print "== 0";}
					$new_exit_status = 1;
				}
                        }
			$exit_status = $new_exit_status;
		}
		#warning check.
		if ( substr($op,0,1) eq "w" )
		{
			$buffer=substr($op,2);
			$buffer =~ s/\)//g;
			@w = split /,/, $buffer;
			foreach $check ( @w )
			{
				if ( $exit_status == $check )
				{
					$WARNING=1;
				}
			}
			if ( $buffer eq "a" )
			{
				$WARNING=1;
			}
		}
		else
		{
			$WARNING = 0;
		}
		
		if ( $comment == 0 )
		{
			if ( $exit_status == 0 )
			{
				print color("$isbold green"), "success\n", color("reset");
				$SUCCESS=$SUCCESS+1;
			}
			else
			{
				if ( $WARNING == 1 )
				{
					print color("$isbold yellow"), "warning ($exit_status)\n", color("reset");
				}
				else
				{
					print color("$isbold red"), "failure ($exit_status),expected: $expected_status\n", color("reset");
					$FAILS=$FAILS+1;
					$FAILED_TESTS="$FAILED_TESTS:$COUNTER"
				}
			}
			$COUNTER = $COUNTER+1;
		}
		else
		{
			print "\n";
		}
	}
		

}

close($INPUT);
close($LOG);

print "Executed N: ", $SUCCESS+$FAILS, " tests.\n";
print "Failures: $FAILS\n";
print "Success: $SUCCESS\n";

if ( $FAILS != 0 )
{
	print "Failed tests:\n $FAILED_TESTS\n";
	print "A complete log is available in the file $LOGFILE\n";
	if ( $FORCE == 0 )
	{
		exit $FAILS;
	}
	else
	{
		exit 0;
	}	
}
else
{
	exit 0;
}
