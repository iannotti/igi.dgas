#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/dgas-consumers/consumers/AMQConsumer.h"

#define OPTION_STRING "3hv:B:t:c:TQAu:p:n:s:i:ND"

using namespace std;

int system_log_level = 9; 
bool needs_help = 0;
int verbosity = 3;
string brokerUri = "";
string topic = "";
string configFile = "";
string useTopics = "";
string clientAck ="";
string username = "";
string password = "";
string clientId = "";
string name = "";
string selector = "";
string noLocal = "";
string durable = "";
//string configFile = GLITE_DGAS_DEF_CONF;

void help(string progname)
{
	cerr<< endl;
        cerr<< "DGAS AMQ Consumer" <<endl;
        cerr<< "Version :" << VERSION << endl ;
        cerr<< "Author: A.Guarise <andrea.guarise@to.infn.it>"<< endl;
        cerr<< endl << "Usage: " << endl;
        cerr<< progname << " [OPTIONS]" << endl << endl;;
        cerr<< "OPTIONS:" <<endl;
        cerr<< "-B  --brokerUri <URI>    The URI specifying the listening AMQ Broker. " << endl;
        cerr<< "-t  --topic <dgas topic> Specifies the queue to poll for incoming messages." << endl;
        cerr<< "-c  --config <confFile>  HLR configuration file name, if different" << endl;
        cerr<< "-u  --username <amq username>  AMQ user if needed for authentication" << endl;
        cerr<< "-p  --password <amq password>  AMQ password for user 'user' if needed for authentication" << endl;
        cerr<< "-i  --clientId <amq clientId>  unique identifier for the connection of a durable subscriber" << endl;
        cerr<< "-n  --name <subscription name>  set a name to identify the subscription" << endl;
        cerr<< "-s  --selector <selector>  pass a selector string to the consumer" << endl;
        cerr<< "-N  --nolocal  set CMS noLocal flag" << endl;
        cerr<< "-T  --useTopic  Use Topic" << endl;
        cerr<< "-Q  --useQueue  Use Queue" << endl;
        cerr<< "-A  --clientAck  Enable consumer client ack mode" << endl;
        cerr<< "-D  --durable  Enable consumer as durable" << endl;
        cerr<< "-h  --help               Print this help message." << endl;
}

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"verbosity",1,0,'v'},
		{"brokerUri",1,0,'B'},
		{"topic",1,0,'t'},
		{"config",1,0,'c'},
		{"username",1,0,'u'},
		{"password",1,0,'p'},
		{"clientId",1,0,'i'},
		{"name",1,0,'n'},
		{"selector",1,0,'s'},
		{"noLocal",0,0,'N'},
		{"useTopic",0,0,'T'},
		{"useQueue",0,0,'Q'},
		{"clientAck",0,0,'A'},
		{"durable",0,0,'D'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'v': verbosity=atoi(optarg); break;
			case 'B': brokerUri=optarg; break;
			case 't': topic=optarg; break;
			case 'c': configFile=optarg; break;
			case 'u': username=optarg; break;
			case 'p': password=optarg; break;
			case 'i': clientId=optarg; break;
			case 'n': name=optarg; break;
			case 's': selector=optarg; break;
			case 'N': noLocal ="true"; break;
			case 'T': useTopics ="true"; break;
			case 'Q': useTopics ="false"; break;
			case 'A': clientAck ="true"; break;
			case 'D': durable ="true"; break;
			case 'h': needs_help =true; break;		  
			default : break;
		}
	return 0;
}

int main (int argc, char *argv[])
{
	options(argc, argv);
	if (needs_help)
	{
		help(argv[0]);
		return 0;
	}
	consumerParms parms;
	parms.amqBrokerUri = brokerUri;
	parms.dgasAMQTopic = topic;
	parms.confFileName = configFile;
	parms.useTopics = useTopics;
	parms.clientAck = clientAck;
	parms.amqUsername = username;
	parms.amqPassword = password;
	parms.amqClientId = clientId;
	parms.noLocal = noLocal;
	parms.selector = selector;
	parms.name = name;
	parms.durable = durable;
	int res = AMQConsumer(parms);
	if ( verbosity > 0 )
	{
		cout << "Return code:" << res << endl;
	}
	if ( res != 0 )
	{
		if ( verbosity > 1 )
		{
			hlrError e;
			cerr << e.error[int2string(res)] << endl;
		}
	}
	return res;
}

