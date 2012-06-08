#!/usr/bin/perl -w

use strict;
use Getopt::Long;
use Net::Stomp;


#usage: cat MESSAGE_FILE | ./dgas-stompProducer.pl -u system -P manager -d /queue/foo -s dgas-broker.to.infn.it:61613

my $destination ="";#/queue/foo
my $server ="";#dgas-broker.to.infn.it:61613
my $port ="61613";
my $user ="";#system
my $Password ="";#manager
my $help;


GetOptions (
        'destination=s' => \$destination,
        'd=s' => \$destination,
        'server=s' => \$server,
        's=s' => \$server,
        'user=s' => \$user,
        'u=s' => \$user,
        'Password=s' => \$Password,
        'P=s' => \$Password,
        'h' => \$help
);

($server,$port) = ($server =~ /^(.*):(.*)$/);
my $buffer ="";
if ( -t STDIN )
{
	$help = "true";
}
else
{
	while (<>)
	{
    	    $buffer .= $_; 
	}
}
if (
	$destination eq "" ||
	$server eq "" ||
	$buffer eq ""
	)
	{
		$help = "true";
	}

if ( $help )
{
	my $helpMessage = "dgas-stompProducer.pl\n";
	$helpMessage .= "This is a message producer using the STOMP protocol.\n";
	$helpMessage .= "Usage:\n\n";
	$helpMessage .= "cat MESSAGEFILE | /usr/libexec/dgas-stompProducer.pl -d destinationQueue -s brokerurl:port [ -u user [-P password] ]\n";
	$helpMessage .= "\nCommand OPTIONS:\n\n";
	$helpMessage .= "-d --destination    The message broker queue name. Example: /queue/foo specifies the queue named 'foo'.\n";
	$helpMessage .= "-s --server         The message broker host address and port. Example: dgas-broker.to.infn.it:61613\n";
	$helpMessage .= "-u --user           The message broker user name if any.\n";
	$helpMessage .= "-P --Password		 The message broker password for the user specified with -u option. If any.\n";
	$helpMessage .= "-h                  This help message.\n";
	$helpMessage .= "\n The command reads any input from standard input and sends it as amessage to the specified broker.\n";
	print $helpMessage . "\n";
}

 # send a message to the queue '$destination'  (to send to the queue:foo the syntax is -d /queue/foo)
  my $stomp = Net::Stomp->new( { hostname => $server, port => $port } );
 $stomp->connect( { login => $user, passcode => $Password } );
  $stomp->send(
      { destination => $destination, body => $buffer } );
  $stomp->disconnect;