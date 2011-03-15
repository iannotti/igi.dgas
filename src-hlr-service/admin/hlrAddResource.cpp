// $Id: hlrAddResource.cpp,v 1.1.2.1.4.5 2011/03/15 13:30:35 aguarise Exp $
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
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/hlr-service/base/hlrResource.h"
#include "glite/dgas/hlr-service/base/notFor2LHLR.h"

#define GRIDMAP_USER "nobody"
#define E_GRIDMAP 3
#define OPTION_STRING "Fhr:d:c:e:g:f:C:S:D"
#define DGAS_DEF_CONF_FILE "/etc/dgas/dgas_hlr.conf"

using namespace std;

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
int system_log_level = 7; 
ofstream logStream;
bool needs_help = false;
bool force = false;
bool debug = false;
string rid_buff = "";
string descr_buff = "";
string email_buff = "";
string ce_id_buff = "";
string certSubject_buff = "";
string gid_buff = "";
string confFileName = DGAS_DEF_CONF_FILE;
string hlr_logFileName;

bool lazyAccountCheck =false;

ostream& operator << ( ostream& os, const hlrResource& r )
{
        os << "rid=" << r.rid << endl;
        os << "email=" << r.email << endl;
        os << "descr=" << r.descr << endl;
        os << "ceId=" << r.ceId << endl;
        os << "hostCertSubject=" << r.hostCertSubject << endl;
        os << "gid=" << r.gid << endl;
        return os;
}

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"Force",0,0,'F'},
		{"rid",1,0,'r'},
		{"descr",1,0,'d'},
		{"ceId",1,0,'c'},
		{"certSubject",1,0,'S'},
		{"email",1,0,'e'},
		{"gid",1,0,'g'},
		{"Conf",1,0,'C'},
		{"help",0,0,'h'},
		{"debug",0,0,'D'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'F': force=true; break;
			case 'r': rid_buff = optarg; break;
			case 'd': descr_buff = optarg; break;
		  	case 'c': ce_id_buff = optarg; break;
		  	case 'e': email_buff = optarg; break;
			case 'g': gid_buff = optarg; break;
			case 'h': needs_help =true; break;		  
			case 'D': debug =true; break;
			case 'C': confFileName = optarg; break;
			case 'S': certSubject_buff = optarg; break;
			default : break;
		}
	return 0;
}

int help (const char *progname)
{
        cerr << "\n " << progname << endl;
	cerr << " Version: " << VERSION << endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS]" << endl;
	cerr << "-h --help                Display this help." << endl;
	cerr << "-D --debug               Display additional info." << endl;
        cerr << "-F --Force               Adds the record in the HLR database even if the" << endl;
        cerr << "                         resource account already exists or the corresponding" << endl;
        cerr << "                         group or VO does not exist." << endl;
        cerr << "-C --Conf <confFile>     HLR configuration file name, if different" << endl;
        cerr << "                         from the default (/etc/dgas/dgas_hlr.conf)." << endl;
        cerr << "-r --rid <resID>         String that identifies the resource in the database."<<endl;
	cerr << "                         It can be set up freely by the HLR administrator, It must be unique per every resource added."<<endl;
	cerr << "-e --email <address>     E-mail address of contact person."<<endl;
        cerr << "-d --descr <descr>       String that describes the resource."<<endl;
	cerr << "-c --ceId <ceID>         The global grid ID of the resource."<<endl;
	cerr << "-S --certSubject <cert>  The subject of the resource's host certificate."<<endl;
        cerr << "-g --gid <groupID>       Specifies the group to which the resource is" << endl;
        cerr << "                         associated in the HLR database."<<endl;
	cerr << endl;
	return 0;	
}

int main (int argc, char **argv)
{
	options ( argc, argv );
        map <string,string> confMap;
	if ( dgas_conf_read ( confFileName, &confMap ) != 0 )
        {
                cerr << "Can't Open Configuration file: " << confFileName << endl;
                help(argv[0]);
                exit(1);
        }
	notfor2lHlr(confMap["is2ndLevelHlr"]);
        hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
        hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
        hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
        hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
	hlr_logFileName = (confMap["hlr_def_log"]).c_str();
	if (needs_help)
	{
		help(argv[0]);
		return 0;
	}
	logStream.open( hlr_logFileName.c_str() , ios::app);
	hlrResource r;
	r.rid = rid_buff;
	r.email = email_buff;
	r.descr = descr_buff;
	r.ceId = ce_id_buff; //it is right! hlrResource class has certSubject there!, need to be FIXED in the future, and the class member changhed to ceId
	r.gid = gid_buff;
	r.acl = certSubject_buff;

	if ( r.rid == "" || r.ceId == "" || r.acl == "" || r.gid == "" )
	{
		cerr << "Resource rid, gid, ceId and CE host CertSubject are mandatory parameters." << endl;
		help(argv[0]);
		logStream.close();
		return 1;
	}
	else
	{
		hlrResource resourceBuff;
		resourceBuff.ceId = r.ceId;
		if ( !force && resourceBuff.get() == 0 )
		{
			cerr << "A resource whit this Grid CE Id already exists, it is not possible to create the account!" << endl;
                        cerr << resourceBuff << endl;
                        return 3;	
		}
		else
		{
			if ( force )
			{
				cerr << "Warning! --Force specified, Overwriting!" << endl;
			}
		}
		if ( force || !r.exists() )
		{
			if (debug) cout << "Adding resource...";
			int res = r.put();
			if ( res == 0 )
			{
				cout << "Resource added!" << endl;
			}
			else
			{
				cout << "error adding resource!" << endl;
				cout << "Exit code:" << res << endl;
			}
			logStream.close();
			return res;
		}
		else
		{
			cerr << "Error: This resource already exists in the database!" << endl;
			if ( debug )
			{
				r.get();
				cerr << r << endl;
			}
			logStream.close();
			return 1;
		}
	}
		
}
