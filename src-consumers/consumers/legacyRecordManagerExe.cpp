#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/hlr-service/engines/atmResourceEngine.h"
#include "glite/dgas/dgas-consumers/consumers/legacyRecordManager.h"

#define OPTION_STRING "3hv:P:c:Rs"

using namespace std;

bool needs_help = false;
bool dryRun = false;
bool singleRun = false;
int system_log_level = 9;
int verbosity = 3;
string configFile = "";
string messageParser = "";
//string configFile = GLITE_DGAS_DEF_CONF;

void help(string progname)
{
        cerr<< endl;
        cerr<< "DGAS legacy record manager" <<endl;
        cerr<< "Version :" << VERSION << endl ;
        cerr<< "Author: A.Guarise <andrea.guarise@to.infn.it>"<< endl;
        cerr<< endl << "Usage: " << endl;
        cerr<< progname << " [OPTIONS]" << endl << endl;
        cerr<< "OPTIONS:" <<endl;
        cerr<< "-P  --messageParser <executable name>  A command that can translate the input usage record to a legacy UR." << endl;
        cerr<< "-c  --config <confFile>  HLR configuration file name, if different" << endl;
        cerr<< "-R  --dryRun      Do not actually inser the usage record in the Database. Useful to debug message parsers." << endl;
        cerr<< "-s  --singleRun   Do not run as a daemon. Process one iteration and exit." << endl;;
        cerr<< "-h  --help               Print this help message." << endl;
}

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"verbosity",1,0,'v'},
		{"messageParser",1,0,'P'},
		{"config",1,0,'c'},
		{"help",0,0,'h'},
		{"dryRun",0,0,'R'},
		{"singleRun",0,0,'s'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'v': verbosity=atoi(optarg); break;
			case 'P': messageParser=optarg; break;
			case 'c': configFile=optarg; break;
			case 'h': needs_help =true; break;		  
			case 'R': dryRun =true; break;		  
			case 's': singleRun =true; break;		  
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
	confParameters parms;
	parms.messageParser = messageParser;
	parms.verbosity = verbosity;
	parms.dryRun = dryRun;
	parms.singleRun = singleRun;
	int res = dgasHlrRecordConsumer(configFile, parms);
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

