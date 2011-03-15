// $Id: hlrHlrAdminGetInfo.cpp,v 1.1.2.1.4.5 2011/03/15 13:30:35 aguarise Exp $
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

#define OPTION_STRING "ahHVLSA:C:R:r:"
#define DGAS_DEF_CONF_FILE "/etc/dgas/dgas_hlr.conf"

using namespace std;

bool lazyAccountCheck = false; //just for build FIXME

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
ofstream logStream;
int system_log_level = 7;
bool interactive = false;
bool all = false;
bool isHlr = false;
bool isVo = false;
bool isLowLevelHlr = false;
bool isRecordSource = false;
bool isVoms = false;
bool needs_help = false;
string admin_buff = "";
string confFileName = DGAS_DEF_CONF_FILE;
string hlr_logFileName;
string vo_id = "";
string vomsRole = "";
string hlrRole = "";

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"all",0,0,'a'},
		{"admin",1,0,'A'},
		{"Hlr",1,0,'H'},
		{"Vo",1,0,'V'},
		{"lowLevelHlr",1,0,'L'},
		{"recordSource",1,0,'S'},
		{"vo",1,0,'v'},
		{"vomsRole",1,0,'R'},
		{"hlrRole",1,0,'r'},
		{"Conf",1,0,'C'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'a': all=true; break;
			case 'A': admin_buff = optarg; break;
			case 'H': isHlr = true; break;
			case 'V': isVo = true; break;
			case 'L': isLowLevelHlr = true; break;
			case 'S': isRecordSource = true; break;
			case 'C': confFileName = optarg; break;
			case 'v': vo_id = optarg; break;
			case 'R': vomsRole = optarg;isVoms = true; break;
			case 'r': hlrRole = optarg;isVoms = true; break;
			case 'h': needs_help =true; break;		  
			default : break;
		}
	//set default
	if ( !isHlr && !isVo && !isLowLevelHlr && !isRecordSource && !isVoms )
		isHlr = true; 
	return 0;
}


int help (const char *progname)
{
        cerr << "\n " << progname << " " << VERSION << endl;
	cerr << " Author: Andrea Guarise " << endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS] " << endl;
        cerr << "-C --Conf <confFile>       HLR configuration file name, if different" << endl;
        cerr << "                           from the default (/etc/dgas/dgas_hlr.conf)." << endl;
	cerr << "-a --all	            Display the list of all admin roles."<<endl;
	cerr << "-H --Hlr	            Display the list of all HLR admins."<<endl;
	cerr << "-V --Vo	            Display the list of all Vo admins."<<endl;
	cerr << "-L --lowLevelHlr	            Display the list of all lowLevelHlr admins."<<endl;
	cerr << "-S --recordSource	            Display the list of all recordSource admins."<<endl;
        cerr << "-A --admin <cert_subject>  x509 certificate subject of the admin." << endl;
        cerr << "-v --vo <vo id>  vo id to use in the query." << endl;
        cerr << "-R --vomsRole <voms role>  voms role to be used in the query." << endl;
        cerr << "-r --hlrRole <hlr role>  hlr authorisation role to query for." << endl;
	cerr << endl;
	return 0;	
}

ostream& operator << (ostream &os, const hlrAdmin &a)
{
	
	os << "hlrAdmin: dn=" << a.acl;
	return os;
}

ostream& operator << (ostream &os, const roles &r)
{
	os << "hlrRole: "<<r.seqNumber << "|";
	os << "dn=" << r.dn << "|";
	os << "role=" << r.role << "|";
	os << "permission=" << r.permission << "|";
	os << "queryType=" << r.queryType << "|";
	os << "queryAdd=" << r.queryAdd << "|";
	return os;
}

ostream& operator << (ostream &os, const hlrVoAcl &v)
{
	os << "voAdmin: dn=" << v.acl << "|";
	os << "voId=" << v.voId << "|";
	return os;
}

ostream& operator << (ostream &os, const  hlrVomsAuthMap &v)
{
	os << "vomsRoleMap: voId=" << v.voId << "|";
	os << "vomsRole=" << v.voRole << "|";
	os << "hlrRole=" << v.hlrRole << "|";
	return os;
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
	if (needs_help)
	{
		help(argv[0]);
		return 0;
	}
	logStream.open( hlr_logFileName.c_str() , ios::app);
	hlrAdmin a(admin_buff);
	hlrVoAcl v("",admin_buff);
	roles r("","lowLevelHlr");
	roles s("","recordSource");
	hlrVomsAuthMap m(vo_id,vomsRole,hlrRole);
	if ( !all )
	{
		if ( isHlr )
		{
			int res = 0;
			res = a.get();
			if ( res != 0 )
			{
				cerr << "Error retrieving hlrAdmin info!" << endl;
				logStream.close();
				return 1;
			}
			else
			{
				cout << a << endl;
				logStream.close();
				return 0;
			}
		}
		if ( isVo )
		{
			int res = 0;
			vector <hlrVoAcl> vv;
                        res = v.get(vv);
                        if ( res != 0 )
                        {
                                cerr << "Error retrieving voAdmin info!" << endl;
                                logStream.close();
                                return 1;
                        }
                        else
                        {
                                cout << vv[0] << endl;
                                logStream.close();
                                return 0;
                        }
		}
		if ( isLowLevelHlr )
		{
			int res = 0;
			vector <roles> rv;		
                        res = r.get(rv);
                        if ( res != 0 )
                        {
                                cerr << "Error retrieving lowLevelHlr info!" << endl;
                                logStream.close();
                                return 1;
                        }
                        else
                        {
                                cout << rv[0] << endl;
                                logStream.close();
                                return 0;
                        }
		}
		if ( isRecordSource )
		{
			int res = 0;
			vector <roles> rv;		
                        res = s.get(rv);
                        if ( res != 0 )
                        {
                                cerr << "Error retrieving recordSource info!" << endl;
                                logStream.close();
                                return 1;
                        }
                        else
                        {
                                cout << rv[0] << endl;
                                logStream.close();
                                return 0;
                        }
		}

		if ( isVoms )
		{
			int res = 0;
			vector<hlrVomsAuthMap> vv;
			res = m.get(vv);
			if ( res != 0 )
			{
				cerr << "Error retrieving voms roles info!" << endl;
				logStream.close();
				return 1;
			}
			else
			{
				cout << vv[0] << endl;
				logStream.close();
				return 0;
			}
		}
	}
	else
	{
		int res = 0;
		vector < hlrAdmin > av;
		res = a.get(av);
		if ( res != 0 && res != 2 )
		{
			cerr << "Error retrieving hlrAdmin data" << endl;
			logStream.close();
			return 1;
		}
		vector < hlrAdmin >::const_iterator av_it = av.begin();
		while ( av_it != av.end() )
		{
			cout << *av_it << endl;
			++av_it;
		}
		
		//voAdmin
		vector <hlrVoAcl> vv;
		res = v.get(vv);
		if ( res != 0 && res != 2 )
		{
			cerr << "Error retrieving voAdmin data" << endl;
                        logStream.close();
                        return 1;
		}
		vector < hlrVoAcl >::const_iterator vv_it = vv.begin();
		while ( vv_it != vv.end() )
		{
			cout << *vv_it << endl;
			++vv_it;
		}
		
		//roles
		vector <roles> rv;
		res = r.get(rv);
		if ( res != 0 && res != 2 )
                {
                        cerr << "Error retrieving lowLevelHlr data" << endl;
                        logStream.close();
                        return 1;
                }
		vector < roles >::const_iterator rv_it = rv.begin();
                while ( rv_it != rv.end() )
                {
                        cout << *rv_it << endl;
                        ++rv_it;
                }
		//recordSource
		vector <roles> sv;
		res = s.get(sv);
		if ( res != 0 && res != 2 )
                {
                        cerr << "Error retrieving lowLevelHlr data" << endl;
                        logStream.close();
                        return 1;
		}
		vector < roles >::const_iterator sv_it = sv.begin();
                while ( sv_it != sv.end() )
                {
                        cout << *sv_it << endl;
                        ++sv_it;
                }
		vector <hlrVomsAuthMap> mv;
		res = m.get(mv);
		if ( res != 0 && res != 2 )
		{
			cerr << "Error retrieving vomsAuthmap data" << endl;
			logStream.close();
			return 1;
		}
		vector <hlrVomsAuthMap>::const_iterator mv_it = mv.begin();
		while ( mv_it != mv.end() )
		{
			cout << *mv_it << endl;
			++mv_it;
		}
		logStream.close();
		return 0;
	}
}
