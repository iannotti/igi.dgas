#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/dgas-producers/producers/legacyProducer.h"

#define OPTION_STRING "hv:l:s:"

using namespace std;

bool needs_help = 0;

string res_acct_bank_id_buff = ""; // HLR for this CE (URL)
int verbosity = 3;

void help()
{
	cerr << "DGAS Legacy Protocol Producer" << endl;
	cerr << "Author: Andrea Guarise <andrea.guarise@to.infn.it>" << endl;
	cerr << "Version:" << ATM_CLIENT_VERSION << VERSION << endl;
	cerr << "Usage:" << endl;
	cerr << "dgas-legacyProducer  <OPTIONS> [USAGE RECORD LIST]" << endl;
	cerr << "Where options are:" << endl;
	cerr << "-h  --help                       Display this help and exit." << endl;
	cerr << "-v  --verbosity <verbosity>      (0-3) default 3 maximum verbosity" << endl;
	cerr << "-l  --localbankid <HLR contact>  Contact string of the (local) Resource HLR. (deprecated, use -s instead)" << endl;
	cerr << "-s  --server <HLR contact>  Contact string of the (local) Resource HLR." << endl;
	cerr << endl;
	cerr << "The HLR an PA contact strings have the form: \"host:port:host_cert_subject\"." << endl;
	cerr << endl;

}

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"verbosity",1,0,'v'},
		{"localbankid",1,0,'l'},
		{"server",1,0,'s'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'v': verbosity=atoi(optarg); break;
			case 'l': res_acct_bank_id_buff = optarg; break;
			case 's': res_acct_bank_id_buff = optarg; break;
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

	int res;
	producerConfiguration pConf;
	pConf.hlrServer = res_acct_bank_id_buff;
	string input;
	ostringstream buf;
	char ch;
	while ( buf && cin.get(ch) )
                	buf.put(ch);
	input = buf.str();
	string output;
	res = ATM_client_toResource(
		input, 
		&output,
		pConf  );
	if ( verbosity > 2 )
	{
		cout << output << endl;
	}
	if ( verbosity > 0 )
	{
		cout << "Protocol Return code:" << res << endl;
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

