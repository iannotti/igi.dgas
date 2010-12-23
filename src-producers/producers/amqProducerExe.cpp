#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/dgas-producers/producers/amqProducer.h"

#define OPTION_STRING "3hv:B:c:O:T:"

#define GLITE_DGAS_DEF_CONF "/etc/dgas_sensors.conf"

using namespace std;

bool needs_help = 0;
int verbosity = 3;
string brokerUri = "";
string amqOptions = "";
string amqTopic = "";
string configFile = dgasLocation() + GLITE_DGAS_DEF_CONF;

void help()
{
}

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"verbosity",1,0,'v'},
		{"brokerUri",1,0,'B'},
		{"amqOptions",1,0,'O'},
		{"amqTopic",1,0,'T'},
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
			case 'O': amqOptions=optarg; break;
			case 'T': amqTopic=optarg; break;
			case 'c': configFile=optarg; break;
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
	int res = dgasHlrRecordProducer(configFile, brokerUri, amqTopic);
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

