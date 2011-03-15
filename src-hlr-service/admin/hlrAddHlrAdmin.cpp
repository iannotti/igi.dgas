// $Id: hlrAddHlrAdmin.cpp,v 1.1.2.1.4.5 2011/03/15 13:30:35 aguarise Exp $
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
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/hlr-service/base/hlrAdmin.h"

#define OPTION_STRING "hFa:v:C:HVLSGR:"
#define GRIDMAP_USER "nobody"
#define E_GRIDMAP 3
#define DGAS_DEF_CONF_FILE "/etc/dgas/dgas_hlr.conf"
using namespace std;

bool lazyAccountCheck; //just for build

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
ofstream logStream;
int system_log_level = 7;
bool needs_help = false;
bool force = false;
bool isHlr = true;
bool isVo = false;
bool isLowLevelHLR = false;
bool isRecordSource = false;
bool isGroup = false;
string admin_buff = "";
string vo_buff = "";
string confFileName = DGAS_DEF_CONF_FILE;
string hlr_logFileName;
string vomsRole = "";

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"Force",0,0,'F'},
		{"HLR",0,0,'H'},
		{"VO",0,0,'V'},
		{"Group",0,0,'G'},
		{"lowLevelHlr",0,0,'L'},
		{"recordSource",0,0,'S'},
		{"vomsRole",1,0,'R'},
		{"admin",1,0,'a'},
		{"vo",1,0,'v'},
		{"Conf",1,0,'C'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'F': force=true; break;
			case 'H': isHlr=true; break;
			case 'L': isLowLevelHLR=true,isHlr=false; break;
			case 'S': isRecordSource=true,isHlr=false; break;
			case 'V': isVo=true,isHlr=false; break;
			case 'G': isGroup=true,isHlr=false; break;
			case 'a': admin_buff = optarg; break;
			case 'v': vo_buff = optarg; break;
			case 'R': vomsRole = optarg; break;
			case 'C': confFileName = optarg; break;
			case 'h': needs_help =true; break;		  
			default : break;
		}
	return 0;
}

int help (const char *progname)
{
        cerr << "\n " << progname << " " << VERSION << endl;
	cerr << " Author: Andrea Guarise " << endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS]" << endl;
	cerr << "-F --Force                 Adds the record in the HLR database even" << endl;
	cerr << "                           if the certificate subject is already listed" << endl;
	cerr << "                           as HLR admin." << endl;  
	cerr << "-C --Conf <confFile>       HLR configuration file name, if different" << endl;
	cerr << "                           from the default (/etc/dgas/dgas_hlr.conf)." << endl;
        cerr << "-a --admin <cert_subject>  x509 certificate subject of the admin." << endl;
        cerr << "-v --vo    <vo id>         Identificative for a Virtual Organisation" << endl;
        cerr << "-R --vomsRole    <voms Role>      voms ROLE that will be assigned to the requested HLR auth level. (affects just VOMS based auth.)" << endl;
        cerr << "-H --HLR                   Insert ACL for HLR admin (default)." << endl;
        cerr << "-V --VO                    Insert ACL for VO admin." << endl;
        cerr << "-G --Group                    Insert entry for voms Group admin (affects just VOMS based auth)." << endl;
        cerr << "-L --lowLevelHlr          Insert Role for lowLevel HLR sending Usage Records to this one (useful just on 2nd level HLRs)." << endl;
        cerr << "-S --recordSource          Insert Role for recordSource sending Usage Records to this one." << endl;
	cerr << endl;
	return 0;	
}
	
int addHlrAdmin()
{
	hlrAdmin a;
	a.acl = admin_buff;
	if ( a.acl == "" )
	{
		cerr << "ERROR: admin is a  mandatory parameter!" << endl;
		needs_help= true;
		return 1;
	}
	else
	{
		if ( force || !a.exists() )
		{
			int res = a.put();
			if ( res == 0 )
			{
				cout << "Record added!" << endl;
			}
			else
			{
				cout << "Error adding HLR admin!" << endl;
				cout << "received error code: " << res << endl;
			}
			return res;
		}
		else
		{
			cout << "HLR admin already registered!" << endl;
			return 1;
		}
	}
}

int addVoAdmin ()
{
	hlrVoAcl v;
	v.acl = admin_buff;
	v.voId = vo_buff;
	if ( (v.acl == "") || (v.voId == ""))
	{
		cerr << "ERROR: vo and admin are mandatory parameters!" << endl;
		needs_help= true;
		return 1;
	}
	else
	{
		if ( force || !v.exists() )
		{
			int res = v.put();
			if ( res == 0 )
			{
				cout << "Record added!" << endl;
			}
			else
			{
				cout << "Error adding VO admin!" << endl;
				cout << "received error code: " << res << endl;
			}
			return res;
		}
		else
		{
			cout << "VO admin already registered!" << endl;
			return 1;
		}
	}
}

int addllHLRRole ()
{
	roles r;
	r.dn = admin_buff;
	r.role = "lowLevelHlr";
	r.permission = "rw";
	r.queryType = "list,aggregate";
	r.queryAdd = "";
	if ( (r.dn == "") )
	{
		cerr << "ERROR: admin (DN) is mandatory!" << endl;
		needs_help= true;
		return 1;
	}
	else
	{
		if ( force || !r.exists() )
		{
			int res = r.put();
			if ( res == 0 )
			{
				cout << "Record added!" << endl;
			}
			else
			{
				cout << "Error adding lowLevel HLR!" << endl;
				cout << "received error code: " << res << endl;
			}
			return res;
		}
		else
		{
			cout << "VO admin already registered!" << endl;
			return 1;
		}
	}
}

int addRecordSource ()
{
	roles r;
	r.dn = admin_buff;
	r.role = "recordSource";
	r.permission = "rw";
	r.queryType = "";
	r.queryAdd = "";
	if ( (r.dn == "") )
	{
		cerr << "ERROR: admin (DN) is mandatory!" << endl;
		needs_help= true;
		return 1;
	}
	else
	{
		if ( force || !r.exists() )
		{
			int res = r.put();
			if ( res == 0 )
			{
				cout << "Record added!" << endl;
			}
			else
			{
				cout << "Error adding recordSource admin!" << endl;
				cout << "received error code: " << res << endl;
			}
			return res;
		}
		else
		{
			cout << "recordSource admin already registered!" << endl;
			return 1;
		}
	}
}



int addVomsRole()
{
	hlrVomsAuthMap v;
	v.voId = vo_buff;
	v.voRole = vomsRole;
	if ( isVo )
	{
		v.hlrRole = "voManager";	
	} 
	else if ( isGroup )
	{
		v.hlrRole = "groupManager";
	} 
	else
	{
		cerr << "not supported yet!" << endl;
		return 1;
	}
	if ( v.put() != 0 )
	{
		cerr << "error inserting voms auth entry!" << endl;
		return 2;
	}
	else
	{
		cerr << "Entry inserted." << endl;
		return 0;
	}
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
	hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
	hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
	hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
	hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
	hlr_logFileName = (confMap["hlr_def_log"]).c_str();
	int res = 0;
	logStream.open( hlr_logFileName.c_str() , ios::app);
	if ( vomsRole != "" )
	{
		return addVomsRole();	
	}
	if ( isHlr )
	{
		res = addHlrAdmin();
	}
	if ( isVo )
	{
		res = addVoAdmin();
	}
	if ( isLowLevelHLR )
	{
		res = addllHLRRole();
	}
	if ( isRecordSource )
	{
		res = addRecordSource();
	}
	if ( needs_help )
	{
		help(argv[0]);
	}
	logStream.close();
	return res;
}
