// $Id: hlrSqlExec.cpp,v 1.1.2.1.4.4 2011/03/15 13:30:35 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
/***************************************************************************
 * Code borrowed from:
 *  authors   : 
 *             
 *  
 ***************************************************************************/


#include <iostream>
#include <iomanip>
#include <getopt.h>
#include "glite/dgas/hlr-service/base/hlrGenericQuery.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/int2string.h"

#define OPTION_STRING "C:Dh"
#define DGAS_DEF_CONF_FILE "/etc/dgas/dgas_hlr.conf"

using namespace std;

bool lazyAccountCheck = false;//for build FIXME

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;

int system_log_level = 7;
ofstream logStream;

string confFileName = DGAS_DEF_CONF_FILE;
bool debug = false;
bool needs_help = false;
vector<string> queries;

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"Conf",1,0,'C'},
		{"debug",0,0,'D'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'C': confFileName = optarg; break;
			case 'D': debug = true; break;
			case 'h': needs_help =true;; break;		  
			default : break;
		}
		if (optind < argc )
		{
			while (optind < argc)
                         queries.push_back(argv[optind++]);
		}
	return 0;
}

int help (const char *progname)
{
	cerr << "\n " << progname << endl;
        cerr << " Version: "<< VERSION << endl;
        cerr << " Author: Andrea Guarise " << endl;
        cerr << " 26/04/2006 " << endl <<endl;
	cerr << " Updates a reference table used in queries to the database." << endl;
        cerr << " usage: " << endl;
        cerr << " " << progname << " [OPTIONS] <list of space separated queries> " << endl;
        cerr << setw(30) << left << "-D --debug"<<"Ask for debug output" << endl;
	cerr << setw(30) << left << "-C --Conf"<<"HLR configuration file, if not the default: " << DGAS_DEF_CONF_FILE << endl;
        cerr << setw(30) << left <<"-h --help"<<"This help" << endl;
        cerr << endl;
	return 0;	
}


struct queryResult {
	hlrQueryResult result;
	size_t Fields;
	size_t Rows;
	};


int performQuery (string queryString, queryResult &result )
{
	if ( debug )
	{
		cerr << queryString << endl;
	}	
	hlrGenericQuery q(queryString);
	int res = q.query();
	if ( res == 0 )
	{
		result.result = q.queryResult;
		result.Fields = q.Columns();
		result.Rows = q.Rows();
		return 0;
	}
	return res;
}

int printQueryResult ( queryResult &r )
{
	vector<resultRow>::const_iterator it = (r.result).begin();
	while ( it != (r.result).end() )
	{
		cout << "|" << flush;
		for (size_t i = 0; i< (*it).size(); i++ )
		{
			cout << (*it)[i] << "|";
		}
		cout << endl;
		it++;
	}
	return 0;
}


int main (int argc, char **argv)
{
	options ( argc, argv );
        map <string,string> confMap;
        if ( dgas_conf_read ( confFileName, &confMap ) != 0 )
	{
		cerr << "Couldn't open configuration file: " <<
			confFileName << endl;
	}
        hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
        hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
        hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
        hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
	if (needs_help)
	{
		help(argv[0]);
		return 0;
	}
	vector<string>::const_iterator it = queries.begin();
	while ( it != queries.end() )
	{
		queryResult q;
		int res = performQuery( *it, q );
		if ( res != 0 )
		{
			cerr << "Query:" << *it << endl;
			cerr << "returned:" << res << endl;
		}	
		else
		{
			printQueryResult(q);
			if ( debug )
			{
				cout << "Rows:" << q.Rows;
				cout << ";Columns:" << q.Fields;
			}
		}
		it++;
	}
	return 0;
}
