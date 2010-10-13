#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>
#include <signal.h>
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/tls/GSISocketAgent.h"
#include "glite/dgas/common/tls/GSISocketClient.h"

#define URS_CLIENT_VERSION "0.1"
#define OPTION_STRING "hv:s:t:"

using namespace std;
using namespace glite::wmsutils::tls::socket_pp;


bool needs_help = false;
string server;
string table;
int verbosity = 0;

vector<string> info_v;

void
catch_alarm (int sig)
{
       exit(-1);
}

int  defConnTimeOut = 30;

void help()
{
	cerr << "DGAS Send Record client" << endl;
	cerr << "Author: Andrea Guarise <andrea.guarise@to.infn.it>" << endl;
	cerr << "Version:" << URS_CLIENT_VERSION << VERSION << endl;
	cerr << "Usage:" << endl;
	cerr << "glite-dgas-send-record <OPTIONS> [USAGE RECORD LIST]" << endl;
	cerr << "Where options are:" << endl;
	cerr << "-h  --help                       Display this help and exit." << endl;
	cerr << "-v  --verbosity <verbosity>      (0-3) default 3 maximum verbosity" << endl;
	cerr << "-s  --server <HLR contact>  Contact string of the Site HLR." << endl;
	cerr << "-t  --table <HLR table>  table on the HLR." << endl;
	cerr << endl;
	cerr << "The HLR an PA contact strings have the form: \"host:port:host_cert_subject\"." << endl;
	cerr << endl;
	cerr << "[USAGE RECORD LIST]:" << endl;
	cerr << "\"timestamp=<int>\" \"siteName=<string>\" \"vo=<string>\" \"voDefSubClass=<string>\"" << endl;
	cerr << "\"storage=<string>\" \"storageSubClass=<string>\" \"usedBytes=<int>\"" << endl;
	cerr << "\"freeBytes=<int>\""<< endl;
	cerr << endl;
}

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"verbosity",1,0,'v'},
		{"server",1,0,'s'},
		{"table",1,0,'t'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'v': verbosity=atoi(optarg); break;
			case 's': server = optarg; break;
			case 't': table = optarg; break;
			case 'h': needs_help =true; break;		  
			default : break;
		}
		if (optind < argc) 
		{
                      while (optind < argc)
                         info_v.push_back(argv[optind++]);
                }
	return 0;
}

void xml_compose(string& table, vector<string> info_v, string *xml)
{
	*xml = "<HLR type=\"sendRecord\">\n";
	*xml += "<BODY>\n";
	*xml += "<tableName>\n";
	*xml += table;
	*xml += "</tableName>\n";
	*xml += "<record>\n";
	vector<string>::iterator it = info_v.begin();
        while (it != info_v.end())
        {
                int pos = (*it).find_first_of( "=" );
                if ( pos != string::npos )
                {
                        vector<attribute> attributes;
                        string keyBuff = "";
                        string valueBuff = "";
                        keyBuff = (*it).substr(0, pos);
                        valueBuff = (*it).substr(pos+1, (*it).size()-pos);
                        attribute attrBuff = {keyBuff,valueBuff};
                        attributes.push_back(attrBuff);
                        *xml += tagAdd ("dgas:item", "", attributes );
                }
                it++;
        }
	*xml += "</record>\n";
	*xml += "</BODY>\n";
	*xml += "</HLR>\n";
	return;
}

int parse_xml(string& answer, string& uniqueChecksumAnswer )
{
		node nodeBuff = parse(&answer, "CODE");
		string errorMessageStatus=nodeBuff.text;
		nodeBuff.release();
		if ( errorMessageStatus != "0" )
		{
			cerr << "Error: got exit status:" << errorMessageStatus << endl;
			return ( atoi(errorMessageStatus.c_str()) );
		}
		else
		{
			bool goOn = true;
			while ( goOn )
			{	
				node jobInfoNode;
				jobInfoNode = parse(&answer, "dgas:item");
				if ( jobInfoNode.status == 0 )
				{
					attrType attributes;
                                        attributes = jobInfoNode.getAttributes();
					string buffer = parseAttribute ("uniqueChecksum", attributes);
                        	        if ( buffer != "")
                                	{
                                  	     uniqueChecksumAnswer = buffer;
                                	}	
					jobInfoNode.release();
				}
				else
				{
					goOn =false;
				}
			}
			return ( atoi(errorMessageStatus.c_str()) );
		}
}

int sendRecord ( string server, string table, vector<string>& info_v, string& answer)
{
	int res =0;
	string output_message;
	string input_message;
	string hlrHostname = "";
        int hlrPort = 56568;//default value
        string hlrContact = "";
	vector<string> urlBuff;
	Split(':',server, &urlBuff);
	if ( urlBuff.size() > 0 )
	{
		if ( urlBuff.size() > 0 ) hlrHostname = urlBuff[0];
                if ( urlBuff.size() > 1 ) hlrPort = atoi((urlBuff[1]).c_str());
                if ( urlBuff.size() > 2 ) hlrContact = urlBuff[2];
	}
	string message;
	xml_compose(table,info_v,&message);
	signal (SIGALRM, catch_alarm);
	alarm ( defConnTimeOut);
	GSISocketClient *theClient = new GSISocketClient(hlrHostname, hlrPort);
	theClient-> ServerContact(hlrContact);
	theClient->SetTimeout( defConnTimeOut );
	if ( !(theClient->Open()))
        {
                 res = atoi(E_NO_CONNECTION);
        }
	else
	{
		if ( !(theClient->Send(message)))
                {
                         res = atoi(E_SEND_MESSAGE);
                }
		if ( !(theClient->Receive(answer)) )
                {
                         res = atoi(E_RECEIVE_MESSAGE);
                }
		theClient->Close();
                if (res == 0)
                {
			string buff;
                        res = parse_xml(answer, buff);
			if ( buff != "" ) cout << buff << endl;
                }
	}
	return res;
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
	string answer;
	res = sendRecord( 
		server,
		table,
		info_v, 
		answer );
	if ( verbosity > 2 )
	{
		cout << answer << endl;
	}
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

