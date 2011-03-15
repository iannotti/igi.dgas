#include <string>
#include <iostream>
#include <getopt.h>
#include "glite/dgas/common/pa/libPA_comm.h"
#include "glite/dgas/pa-clients/paClient.h"

#define OPTION_STRING "s:t:c:h"

ofstream logStream;
int system_log_level = 7;
using namespace std;
bool needs_help = false;
string ceId_buff = "";
string server_buff = "";
int time_buff = 0;
int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"server",1,0,'s'},
		{"ceId",1,0,'c'},
		{"time",1,0,'c'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'c': ceId_buff = optarg; break;
			case 's': server_buff = optarg; break;
			case 't': time_buff = atoi(optarg); break;
			case 'h': needs_help =true; break;
			default : break;
		}
	return 0;
}

int help (const char *progname)
{
	cerr << "\n " << progname << endl;
	cerr << " Version: " << "Prototype" << endl;
	cerr << " Author: Andrea Guarise " << endl;
	cerr << " " << __DATE__ << endl <<endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS]" << endl;
	cerr << "-C --Conf <filename>      Configuration file name, if different from the" << endl;
	cerr << "                          default: '/etc/dgas/dgas_pa.conf'." << endl;
	cerr << "-s --server <PA contact>  Contact string of the PA to query." << endl;
	cerr << "                          The PA contact string is of the form:" << endl;
	cerr << "                          \"host:port:host_cert_subject\"" << endl;
	cerr << "-c --ceId <ceID>          Global resource ID (CE ID)." << endl;
	cerr << "-t --time <timestamp>     Timestamp for which to retrieve price information." << endl;
	cerr << "                          Specified in seconds since Jan. 1, 1970 O:00:00 GMT." << endl;
	cerr << endl;
	return 0;
}

int main (int argc, char *argv[])
{
	options ( argc, argv );
	if ( needs_help )
	{
		help(argv[0]);
		exit(1);
	}
	int res;
	price query_price;
	query_price.resID = ceId_buff;
	query_price.time = time_buff;
	query_price.priceValue = 0;
	res = dgas_pa_client(server_buff , &query_price );
	cout << "Got price:" << endl;
	cout << "resource: " << query_price.resID << endl;
	cout << "time: " << query_price.time << endl;
	cout << "price: " << query_price.priceValue << endl;
	return res;
}

