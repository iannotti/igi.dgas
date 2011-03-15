// $Id: hlrDelHlrAdmin.cpp,v 1.1.2.1.4.5 2011/03/15 13:30:35 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Authors: Stefano Barale <stefano.barale@to.infn.it>
/***************************************************************************
 * Code borrowed from: Andrea Guarise <andrea.guarise@to.infn.it>
 *  authors   : 
 *             
 ***************************************************************************/

#include <iostream>
#include <getopt.h>
#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/hlr-service/base/hlrAdmin.h"

#define OPTION_STRING "hHVLSAa:v:C:r:"
#define DGAS_DEF_CONF_FILE "/etc/dgas/dgas_hlr.conf"

using namespace std;


bool lazyAccountCheck = false; //just for build FIXME

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
ofstream logStream;
int system_log_level = 7;
bool isHlr=false;
bool isVo=false;
bool isLowLevelHlr=false;
bool isRecordSource=false;
bool isVomsAuth=false;
bool needs_help = false;
string admin_buff = "";
string vo_buff = "";
string vorole = "";
string confFileName = DGAS_DEF_CONF_FILE;
string hlr_logFileName;

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"admin",1,0,'a'},
		{"vo",1,0,'v'},
		{"Hlr",0,0,'H'},
		{"Vo",0,0,'V'},
		{"lowLevelHlr",0,0,'L'},
		{"recordSource",0,0,'S'},
		{"vomsAuth",0,0,'A'},
		{"vorole",1,0,'r'},
		{"Conf",1,0,'C'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'V': isVo=true; break;
			case 'H': isHlr=true; break;
			case 'L': isLowLevelHlr=true; break;
			case 'S': isRecordSource=true; break;
			case 'A': isVomsAuth=true; break;
			case 'a': admin_buff = optarg; break;
			case 'v': vo_buff = optarg; break;
			case 'r': vorole = optarg; break;
			case 'C': confFileName = optarg; break;
			case 'h': needs_help =true; break;		  
			default : break;
		}
	if ( !isHlr && !isVo && !isLowLevelHlr && !isRecordSource && !isVomsAuth )
		isHlr = true;
	return 0;
}


int help (const char *progname)
{
        cerr << "\n " << progname << " " << VERSION << endl;
	cerr << " Authors: Andrea Guarise " << endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS] " << endl;
        cerr << " -C --Conf <confFile>       HLR configuration file name, if different" << endl;
        cerr << "                           from the default (/etc/dgas/dgas_hlr.conf)." << endl;
        cerr << " -a --admin <cert_subject>  x509 certificate subject of the HLR admin." << endl;
        cerr << " -v --vo <vo ID>  name of the vo managed by the voAdmin" << endl;
	cerr << " -r --vorole <vomsRole>	        Specifies the voms authorisation role to delete (must specify --vo too)" << endl;
	cerr << " -H --hlrAdmin 	Delete an hlrAdmin auth. (Must specify --admin option too.)" << endl;
	cerr << " -V --voAdmin 	Delete a voAdmin auth. (Must specify -v option too)" << endl;
	cerr << " -L --lowLevelHlr 	Delete a lowLevelHlr auth. (Must specify --admin option too.)" << endl;
	cerr << " -S --recordSource 	Delete a recordSource auth. (Must specify --admin option too.)" << endl;
	cerr << " -A --vomsAuth 	Delete a voms authorisation role" << endl;
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

	if ( needs_help )
	{
		help(argv[0]);
		exit(1);
	}
	logStream.open( hlr_logFileName.c_str() , ios::app);
	
	if ( admin_buff == "" && !isVomsAuth )
	{
	    cerr << "You forgot to specify one or more mandatory option(s): " << endl;
	    help(argv[0]);
	    logStream.close();
	    exit(1);
	}
	
	if ( isHlr )
	{
		hlrAdmin a;
		a.acl = admin_buff;
		int res = 0;
		if ( a.exists() )
		{
			res = a.del();
			if ( res == 0 )
			{
				cout << "Record deleted!" << endl;
			}
			else
			{
				cerr <<"Error deleting the hlrAdmin, exit status: "<< res <<endl;
			}
		}
		else
		{
			cerr << "Error, hlrAdmin doesn't exists!" << endl;
			res = 1;
		}
		logStream.close();
		return res;
	}
	if ( isVo )
	{
		int res = 0;
		if ( vo_buff == "" || admin_buff == "" )
		{
			cerr << "must specyfy vo with -v and admin Dn with -a !!" << endl;
			help(argv[0]);
			res = 1;
		}
		else
		{
			hlrVoAcl v(vo_buff, admin_buff);
			if ( v.exists() )
			{
				res = v.del();
				if ( res==0 )
				{
					cout << "Record deleted!" << endl;
				}
				else
				{
					cerr <<"Error deleting the hlrAdmin, exit status: "<< res <<endl;
				}
			}
			else
			{
				cerr << "Error, voAdmin doesn't exists!" << endl;
				res =1;
			}
		}
               	logStream.close();
        	return res;
	}
	if ( isLowLevelHlr )
	{
		if ( admin_buff == "" )
                {
                        cerr << "must specify admin Dn with -a !!" << endl;
                        help(argv[0]);
                        return 1;
                }
                roles r(admin_buff, "lowLevelHlr");
                if ( r.exists() )
                {
                        int res = r.del();
                        if ( res==0 )
                        {
                                cout << "Record deleted!" << endl;
                        }
                        else
                        {
                                cerr <<"Error deleting the lowLevelHlr admin, exit status: "<< res <<endl;
                                logStream.close();
                                return res;
                        }
                }
                else
                {
                        cerr << "Error, lowLevelHlr admin doesn't exists!" << endl;                        logStream.close();
                        return 1;
                }
	}
	if ( isRecordSource )
	{
		if ( admin_buff == "" )
                {
                        cerr << "must specify admin Dn with -a !!" << endl;
                        help(argv[0]);
                        return 1;
                }
                roles r(admin_buff, "recordSource");
                if ( r.exists() )
                {
                        int res = r.del();
                        if ( res==0 )
                        {
                                cout << "Record deleted!" << endl;
                        }
                        else
                        {
                                cerr <<"Error deleting the recordSource admin, exit status: "<< res <<endl;
                                logStream.close();
                                return res;
                        }
                }
                else
                {
                        cerr << "Error, recordSource admin doesn't exists!" << endl;                        logStream.close();
                        return 1;
                }
	}

	if ( isVomsAuth )
	{
		if ( vo_buff == "" || vorole == "" )
		{
			cerr << "Must specify --vo and --vorole options !" << endl;
			help(argv[0]);
			return 1;
		}
		hlrVomsAuthMap v(vo_buff, vorole);
		if ( v.exists() )
		{
			int res = v.del();
			if ( res == 0 )
			{
				cerr << "Entry removed!" << endl;
			}
			else
			{
				cerr << "Error deleting entry:" << res << endl;
				logStream.close();
				return res;
			}
		}
		else
		{
			cerr << "Entry does not exists!" << endl;
			logStream.close();
			return 1;
		}
	}
	return 0;
}
