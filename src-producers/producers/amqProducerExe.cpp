#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "dgas/msg-common/amqProducer.h"

#define OPTION_STRING "3hv:B:c:t:TQAPRu:p:"

#define GLITE_DGAS_DEF_CONF "/etc/dgas/dgas_sensors.conf"

using namespace std;

bool needs_help = 0;
int verbosity = 1;
string brokerUri = "";
string amqTopic = "";
bool useTopics = false;
bool clientAck = false;
bool persistentDelivery = false;
bool requestReply = false;
string username = "";
string password = "";
string configFile = GLITE_DGAS_DEF_CONF;
string outputMessage;

void help()
{
	cerr << "DGAS ActiveMQ Producer" << endl;
        cerr << "Author: Andrea Guarise <andrea.guarise@to.infn.it>" << endl;
        cerr << "Version:" << VERSION << endl;
        cerr << "Usage:" << endl;
        cerr << "dgas-amqProducer  <OPTIONS> [USAGE RECORD LIST]" << endl;
        cerr << "Where options are:" << endl;
        cerr << "-h  --help                       Display this help and exit." << endl;
        cerr << "-v  --verbosity <verbosity>      (0-3) default 3 maximum verbosity" << endl;
        cerr << "-B  --brokerUri <URI>  The complete URI for the AMQ broker." << endl;
        cerr << "-t  --topic <AMQ Topic>  topic for the messages." << endl;
        cerr << "-u  --username <amq username>  AMQ user if needed for authentication" << endl;
        cerr << "-p  --password <amq password>  AMQ password for user 'user' if needed for authentication" << endl;
        cerr << "-T  --useTopic  Use Topic" << endl;
        cerr << "-Q  --useQueue  Use Queue" << endl;
        cerr << "-P  --usePersistent  Sets the message delivery to Persistent" << endl;
        cerr << "-A  --clientAck  Enable consumer client ack mode" << endl;
        //cerr << "-R  --requestReply Enable request-reply messaging scenario." << endl;//FIXME should implement
        cerr << "-c  --config <file>  config file name." << endl;
        cerr << endl;
}

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"verbosity",1,0,'v'},
		{"brokerUri",1,0,'B'},
		{"amqTopic",1,0,'t'},
		{"config",1,0,'c'},
		{"username",1,0,'u'},
		{"password",1,0,'p'},
		{"useTopic",0,0,'T'},
		{"useQueue",0,0,'Q'},
		{"clientAck",0,0,'A'},
		{"usePersistent",0,0,'P'},
		{"requestReply",0,0,'R'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'v': verbosity=atoi(optarg); break;
			case 'B': brokerUri=optarg; break;
			case 't': amqTopic=optarg; break;
			case 'c': configFile=optarg; break;
			case 'u': username=optarg; break;
			case 'p': password=optarg; break;
			case 'T': useTopics =true; break;
			case 'Q': useTopics =false; break;
			case 'A': clientAck =true; break;
			case 'P': persistentDelivery = true; break;
			case 'R': requestReply =true; break;
			case 'h': needs_help =1; break;		  
			default : break;
		}
	return 0;
}

int main (int argc, char *argv[])
{
	options(argc, argv);
	if (needs_help)
	{
		help();
		return 0;
	}
	AmqProducer amqProducer(
		brokerUri,
		username,
		password,
		amqTopic,
		useTopics,
		clientAck,
		persistentDelivery,
		verbosity
		);
	if ( configFile != "noconf" )
	{
		amqProducer.readConf(configFile);
	}
	if ( outputMessage == "" )
	{
		string textLine;
		while ( getline (cin, textLine, '\n'))
		{
			outputMessage += textLine += "\n";
		}
	}
	amqProducer.setOutputMessage(outputMessage);
	int res = amqProducer.run();
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

