
// ---------------------------------------------------------------------------
//  Includes
// ---------------------------------------------------------------------------
#include <getopt.h>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>

#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/tls/SocketClient.h"
#include "glite/dgas/common/tls/SocketAgent.h"

#ifdef GSI
#include "glite/dgas/common/tls/GSISocketClient.h"
#include "glite/dgas/common/tls/GSISocketAgent.h"
#endif


// global definitions

#define OPTION_STRING "hs:f:"

using namespace std;
#ifndef GLITE_SOCKETPP
using namespace glite::wmsutils::tls::socket_pp;
#endif

bool needs_help = 0;
string hlrUrlBuff = "";
string fileName ="";


// ---------------------------------------------------------------------------
//
//  Usage()
//
// ---------------------------------------------------------------------------
void usage(const char* progname)
{
  cerr << endl << progname << " is a lightweight client for sending a generic" << endl
       << "message to a DGAS (HLR or PA) server" << endl << endl
       << "Usage:" << endl
       << progname << " [OPTIONS] (remote client)" << endl
       << "Ver " << VERSION << endl << endl
       << "OPTIONS:" << endl
       << "-h --help                     Print this help message." << endl
       << "-s --server <HLR/PA contact>  The contact string of the HLR/PA to contact." << endl
       << "-f --file <filename>          File containing the DGASML message for the" << endl
       << "                              server." << endl
       <<  endl;
}


int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"file",1,0,'f'},
		{"server",1,0,'s'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'f': fileName = optarg; break;
			case 's': hlrUrlBuff = optarg; break;
			case 'h': needs_help =1; break;		  
			default : break;
		}
	return 0;
}

int fileRead( string fileBuff, string *xmlBuff )
{
        ifstream inFile(fileBuff.c_str());
        if ( !inFile )
        {
                #ifdef DEBUG
                cerr << "DEBUG:" << __FILE__ << "," << __LINE__;
                cerr << ",Error openig the file:" << fileBuff << endl;
                #endif
                return 1;
        }
        else
        {
                ostringstream buf;
                char ch;
                while ( buf && inFile.get(ch) )
                buf.put(ch);
                *xmlBuff = buf.str();
                return 0;
        }
}


// ---------------------------------------------------------------------------
//
//  main
//
// ---------------------------------------------------------------------------
int main(int argc, char **argv)
{
	int retval = 0;
	options (argc, argv);
	if (needs_help)
	{
		usage(argv[0]);
		return 0;
	}
	vector<string> urlBuff;
	Split(':',hlrUrlBuff, &urlBuff);
	if ( urlBuff.size() != 3 )
	{
		return atoi(E_RUI_PARSE_ID);
	}
	string HlrHostname = urlBuff[0];
	int HlrPort = atoi((urlBuff[1]).c_str());
	string HlrContact = urlBuff[2];
	string xmlBuff;
	if ( fileRead( fileName, &xmlBuff ) != 0 )
	{
		cerr << "Error reading the input file" << endl;
	}
	
#ifdef DEBUG
	cerr << "XML message:\n";
	cerr << xmlBuff;
	cerr << "END of XML message\n";
#endif

#ifdef GSI
	GSISocketClient *theClient = new GSISocketClient(HlrHostname, HlrPort);
	theClient -> ServerContact(HlrContact);
#else
	SocketClient *theClient = new SocketClient(HlrHostname, HlrPort);
#endif
	theClient->SetTimeout(60);
	if ( !(theClient -> Open()) )
	{
		cerr << "ERROR: can't open conenction"<< endl;
		exit (1);
	}
#ifdef DEBUG
	cerr << "Connection opened: sending message ..." <<endl;
#endif
	if ( !(theClient -> Send(xmlBuff) ))
	{
		cerr << "Error sending message" << endl;
	}
	string xml_answer;
	if ( !(theClient -> Receive(xml_answer) ))
	{
		cerr << "Error receiving message" << endl;
	}
	else
	{
		theClient -> Close();
	}
	delete theClient;
	#ifdef DEBUG
	cerr << "Answer from the server:" <<endl;
	#endif
	cout << xml_answer  << endl;

	#ifdef DEBUG
	cerr << "Done." <<endl;
	#endif
	
    	return retval;
}
