#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>

#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/hlr-clients/ping/pingClient.h"

#define OPTION_STRING "hds:t:"

using namespace std;

bool needs_help = 0;
bool debug = false;

string acct_id_buff = ""; 
int type_buff = 0;
vector<string> info_v;

void help()
{
	cerr << "DGAS ping client" << endl;
	cerr << "Author: Andrea Guarise <andrea.guarise@to.infn.it>" << endl << endl;
	cerr << "Usage:" << endl;
	cerr << "glite-dgas-ping <OPTIONS>" << endl;
	cerr << "Where options are:" << endl;
	cerr << "-h  --help                     Display this help and exit." << endl;
	cerr << "-d  --debug                    Display debug information." << endl;
	cerr << "-s  --server <HLR/PA contact>  Contact string of HLR or PA server." << endl;
	cerr << "                               The HLR contact string has the form:" << endl;
	cerr << "                               \"host:port:host_cert_subject\"" << endl;
	cerr << "-t  --type <ping type>         Ping Type: \"0\" = Normal, \"1\" = Status info" << endl;
	cerr << endl;
	
		
}

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"server",1,0,'s'},
		{"type",1,0,'t'},
		{"debug",0,0,'d'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 's': acct_id_buff=optarg; break;
			case 't': type_buff = atoi(optarg); break;
			case 'd': debug =true; break;		  
			case 'h': needs_help =1; break;		  
			default : break;
		}
		if (optind < argc) 
		{
                      while (optind < argc)
                         info_v.push_back(argv[optind++]);
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
	statusInfo status;
	status.engines="";
	status.uiRequests=0;
	status.ATMRequests=0;
	status.pingRequests=0;
	status.authErrors=0;
	
	errorInfo errors;
	errors.engines="";
	errors.uiErrors=0;
	errors.ATMErrors=0;
	errors.pingErrors=0;
	
	int res;
	string output;
	res = dgas_ping_client( 
			acct_id_buff,
		        type_buff,	
			&status, 
			&errors, 
			&output);
	if ( res != 0 )
	{
		cout << "Server: " << acct_id_buff << " Unreachable." << endl;
	}
	else
	{
		if ( type_buff == 1 )
		{
			cout << "Server status:" << endl << endl;
			cout << "Available Engines: " << status.engines << endl;
			if ( status.uiRequests != 0 )
			cout << "User Interface requests: " << status.uiRequests << endl;
			if ( status.ATMRequests != 0 )
			{
				cout << "ATM requests: " << status.ATMRequests << 
					"/" << errors.ATMErrors << endl;
			}
			if ( status.pingRequests != 0 )
			cout << "Ping requests: " << status.pingRequests << endl;
			if ( status.authErrors != 0 )
			cout << "Conenction auth arrors: " << status.authErrors << endl;
		}
		if ( type_buff == 0 )
		{
			cout << "Server alive." << endl;
		}
		if ( type_buff > 1 )
		{
			cout << status.serverMessage << endl;
		}
	}
	if ( debug )
	{
		cout << output << endl;
	}
	if ( res != 0 )
	{
		hlrError e;
		cerr << e.error[int2string(res)] << endl;
	}
	return res;
}

