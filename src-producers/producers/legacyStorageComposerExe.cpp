#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>
#include <cstdio>
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"



#define URS_CLIENT_VERSION "0.1"
#define OPTION_STRING "ht:"

using namespace std;


bool needs_help = false;
string table;
int verbosity = 0;

vector<string> info_v;


void help()
{
	cerr << "DGAS Storage Composer" << endl;
	cerr << "Author: Andrea Guarise <andrea.guarise@to.infn.it>" << endl;
	cerr << "Version:" << URS_CLIENT_VERSION << VERSION << endl;
	cerr << "Usage:" << endl;
	cerr << "dgas-legacyStorageComposer <OPTIONS> [USAGE RECORD LIST]" << endl;
	cerr << "Where options are:" << endl;
	cerr << "-h  --help                       Display this help and exit." << endl;
	cerr << "-t  --table <HLR table>  table on the HLR." << endl;
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
		{"table",1,0,'t'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF )
		switch (option_char)
		{
			case 'v': verbosity=atoi(optarg); break;
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


int composeRecord ( string table, vector<string>& info_v )
{
	string message ="";
	xml_compose(table,info_v,&message);
	cout << message << endl;
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
	composeRecord(
		table,
		info_v
		);
	return 0;
}

