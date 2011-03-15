// $Id: hlrDelResource.cpp,v 1.1.2.1.4.5 2011/03/15 13:30:35 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Authors: Andrea Guarise <guarise@to.infn.it> 
//
/***************************************************************************
 * Code borrowed from: Andrea Guarise <guarise@to.infn.it>
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
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/hlr-service/base/hlrResource.h"
#include "glite/dgas/hlr-service/base/notFor2LHLR.h"


#define OPTION_STRING "hr:d:c:g:f:C:"
#define DGAS_DEF_CONF_FILE "/etc/dgas/dgas_hlr.conf"

using namespace std;

bool lazyAccountCheck = false; //just for build FIXME

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
ofstream logStream;
int system_log_level = 7;
bool needs_help = false;
string rid_buff = "";
string descr_buff = "";
string ceId = "";
string gid_buff = "";
string confFileName = DGAS_DEF_CONF_FILE;
string hlr_logFileName;

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"rid",1,0,'r'},
		{"descr",1,0,'d'},
		{"ceId",1,0,'c'},
		{"gid",1,0,'g'},
		{"Conf",1,0,'C'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'r': rid_buff = optarg; break;
			case 'd': descr_buff = optarg; break;
		  	case 'c': ceId = optarg; break;
			case 'g': gid_buff = optarg; break;
			case 'h': needs_help =true; break;		  
			case 'C': confFileName = optarg; break;
			default : break;
		}
	return 0;
}

int help (const char *progname)
{
        cerr << "\n " << progname << endl;
	cerr << " Version: "<< VERSION << endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS] " << endl;
        cerr << "-C --Conf <confFile>     HLR configuration file name, if different" << endl;
        cerr << "                         from the default (/etc/dgas/dgas_hlr.conf)." << endl;
        cerr << "-r --rid <resID>         String that identifies the resource in the HLR" << endl;
        cerr << "                         database (mandatory)."<<endl;
        cerr << "-d --descr <descr>       String that describes the resource."<<endl;
        cerr << "-c --ceId <ceID>         The global grid ID of the resource."<<endl;
        cerr << "-g --gid <groupID>       Specifies the group to which the resource is" << endl;
        cerr << "                         associated in the HLR database (mandatory)."<<endl;
	cerr << endl;
	return 0;	
}

int main (int argc, char **argv)
{
	options ( argc, argv );
        map <string,string> confMap;
        dgas_conf_read ( confFileName, &confMap );
        hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
        hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
        hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
        hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
	hlr_logFileName = (confMap["hlr_def_log"]).c_str();
	notfor2lHlr(confMap["is2ndLevelHlr"]);
	if (needs_help)
	{
	    help(argv[0]);
	    exit(0);
	}
	if ((gid_buff == "" || rid_buff == "" ) && !needs_help)
	{
	    cerr << "You forgot to specify on or more mandatory option(s): " << endl;
	    help(argv[0]);
	    exit(1);
	}
	logStream.open( hlr_logFileName.c_str() , ios::app);
	
	hlrResource r;
	
	r.rid = rid_buff;
	r.descr = descr_buff;
	r.ceId = ceId;
	r.gid = gid_buff;
	int res = r.del();
	if ( res == 0 )
	{
		cout << "record deleted!" << endl;
		logStream.close();
		return 0;
	}
	else
	{
		cerr << "error deleting the Resource!" << endl;
		cerr << "Exit status: " << res << endl;
		logStream.close();
		return res;
	}
		
}
