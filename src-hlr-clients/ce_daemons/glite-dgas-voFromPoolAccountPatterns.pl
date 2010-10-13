#!/usr/bin/perl -w

# written by A. Guarise (guarise@to.infn.it) and R.M. Piro (piro@to.infn.it)

my $file = $ARGV[0];
my $uid = $ARGV[1];

use POSIX;
use strict;

sub parsePoolDefs
{
        my $defsfile = $_[0];
        my @defs = ();
        if (!open (DEFS,"$defsfile"))
	{
	    print "ERROR opening file: $defsfile. Cannot determine VO from configured pool accounts!\n";
	}
	else
	{
	    my $line;
	    while ( $line = <DEFS> )
	    {
                next if ($line =~ /^\s*\#+/);
                next if ($line =~ /^\n/);
                next if ($line =~ /^\s*\n/);
                push (@defs, $line);
	    }
	    close (DEFS);
	}

        return @defs;
}


sub getPoolfromUid {
        my $uid = shift(@_);
        my @definitions = @_;
        my $userVo = "";
	my $def;
        foreach $def (@definitions)
        {
                chomp $def;
                print "Checking pool account pattern '$def' from configuration file.\n";
		if ( $def =~ /^s/ )
		{
			$def = substr ($def,2);
			my $match;
			my $subst;
			my $mod;
			($match,$subst,$mod) = split ("/",$def);
			$uid =~ s/$match/$subst/g;
			print "s/$match/$subst/g -> $uid\n";
		}
                if ( $uid =~ /$def/ )
                {
                        $userVo = $1;         # userVo
                        return $userVo;
                }
        }
        return "";
}


my @defArray = ();
@defArray = &parsePoolDefs($file);
my $vo = &getPoolfromUid ($uid,@defArray);
print "userVo = '$vo'\n";

