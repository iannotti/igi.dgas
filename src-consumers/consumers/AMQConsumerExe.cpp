#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/dgas-consumers/consumers/AMQConsumer.h"

#define OPTION_STRING "3hv:B:t:c:"

using namespace std;

int system_log_level = 9; 
bool needs_help = 0;
int verbosity = 3;
string brokerUri = "";
string topic = "";
string configFile = "";
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
        cerr<< "-b  --brokerUri <URI>    The URI specifying the listening AMQ Broker. " << endl;
        cerr<< "-t  --topic <dgas topic> Specifies the queue to poll for incoming messages." << endl;
        cerr<< "-c  --config <confFile>  HLR configuration file name, if different" << endl;
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

