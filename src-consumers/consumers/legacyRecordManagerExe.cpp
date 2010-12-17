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

#define OPTION_STRING "3hv:P:D:c:Rs"

using namespace std;

bool needs_help = false;
bool dryRun = false;
bool singleRun = false;
int system_log_level = 9;
int verbosity = 3;
string recordsDir = "";
string configFile = "";
string messageParser = "";
//string configFile = GLITE_DGAS_DEF_CONF;

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
		{"recordsDir",1,0,'D'},
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
			case 'D': recordsDir=optarg; break;
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
		help();
		return 0;
	}
	int res = dgasHlrRecordConsumer(configFile, recordsDir, messageParser, dryRun, singleRun);
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

