// $Id: hlrRGetInfo.cpp,v 1.1.2.1.4.5 2011/03/15 13:30:35 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
/***************************************************************************
 * Code borrowed from:
 *  authors   : 
 *             
 ***************************************************************************/

#include <iostream>
#include <getopt.h>

#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"   
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/base/int2string.h"   
#include "glite/dgas/common/base/libdgas_log.h"   
#include "glite/dgas/common/base/stringSplit.h"   
#include "glite/dgas/hlr-service/base/hlrResource.h"
#include "glite/dgas/hlr-service/base/notFor2LHLR.h"

#define OPTION_STRING "aTRhr:c:C:g:o:"
#define DGAS_DEF_CONF_FILE "/etc/dgas/dgas_hlr.conf"

using namespace std;

bool lazyAccountCheck = false; //just for build FIXME

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
int system_log_level = 7;
ofstream logStream;
bool all = false;
bool needs_help = false;
//bool resources = true;
string rid_buff = "";
string gid_buff = "";
string ceId = ""; 
string confFileName = DGAS_DEF_CONF_FILE;
string hlr_logFileName;
string outputType = "list";

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"all",0,0,'a'},
		{"rid",1,0,'r'},
		{"gid",1,0,'g'},
		{"ceId",1,0,'c'},
		{"Conf",1,0,'C'},
		{"help",0,0,'h'},
		{"output",1,0,'o'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'a': all=true; break;
			case 'r': rid_buff = optarg; break;
			case 'g': gid_buff = optarg; break;
			case 'c': ceId = optarg; break;
                                  // will contain the CE ID
			case 'C': confFileName = optarg; break;
			case 'h': needs_help =1; break;		  
			case 'o': outputType = optarg; break;
			default : break;
		}
	return 0;
}


int help (const char *progname)
{
        cerr << "\n " << progname << " " << VERSION << endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS] " << endl;
        cerr << "-C --Conf <confFile>     HLR configuration file name, if different" << endl;
        cerr << "                         from the default (/etc/dgas/dgas_hlr.conf)." << endl;
//	cerr << "-a --all                 Display information for all resources." <<endl;
        cerr << "-r --rid <resID>         String that identifies the resource in the database."<<endl;
        cerr << "-g --gid <groupId>       String that identifies a group in the HLR database."<<endl;
        cerr << "-c --ceId <ceID>         The global grid ID of the resource."<<endl;
	cerr << "-o --output <outputType>\n";
	cerr << "                     <outputType> can be: list (default), human\n";
	cerr << endl;
	return 0;	
}

int print ( hlrResource r )
{
	if ( outputType == "list" )
	{
		cout << "|";
		cout << r.rid << "|";
		cout << r.email << "|";
		cout << r.descr << "|";
		cout << r.ceId << "|";
		cout << r.gid << "|";
		cout << endl;
		return 0;
	}
	if ( outputType == "human" )
	{
	 	cout << "HLR resource id: " << r.rid << endl;
		cout << "description: " << r.descr << endl;
		cout << "ce Id: " << r.ceId << endl;
		cout << "HLR group id: " << r.gid << endl;
		cout << endl;	
		return 0;
	} 
	cerr << "Error: wrong output type, can be one of: list,human" << endl;
	return 1;
}


int main (int argc, char **argv)
{
	options ( argc, argv );
        map <string,string> confMap;
        if ( dgas_conf_read ( confFileName, &confMap ) != 0 )
	{
		cerr << "Error reading Configuration file: " << confFileName << endl;
		help(argv[0]);
		exit(1);
	}
        hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
        hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
        hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
        hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
	hlr_logFileName = (confMap["hlr_def_log"]).c_str();
	notfor2lHlr(confMap["is2ndLevelHlr"]);
	if (needs_help)
	{
		help(argv[0]);
		return 0;
	}
	logStream.open( hlr_logFileName.c_str() , ios::app);
	hlrResource r(rid_buff,"","",ceId,"",gid_buff);
/*	if ( !all )
	{
		int res = 0;
		res = r.get();
		if ( res != 0 )
		{
			cerr << "Error retrieving resource info" << endl;
			logStream.close();
			return res;
		}
		if ( resources )
		{
			cout << r << endl;	
		}
		logStream.close();
		return 0;
	}
	else
	{*/
		int res = 0;
		vector <hlrResource> rv;
		res = r.get(rv);
		if ( res != 0 )
		{
			cerr << "Error retrieving data" << endl;
			logStream.close();
			return res;
		}
		vector <hlrResource>::const_iterator rv_it = rv.begin();
		while ( rv_it != rv.end() )
		{
			print(*rv_it);
			++rv_it;
		}
		logStream.close();
		return 0;
//	}
}
