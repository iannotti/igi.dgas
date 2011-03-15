// $Id: hlrCheckQueue.cpp,v 1.1.2.1.4.5 2011/03/15 13:30:35 aguarise Exp $
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
#include <time.h>

#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/stringSplit.h"

#include "glite/dgas/hlr-service/base/qTransaction.h"
#include "glite/dgas/hlr-service/base/notFor2LHLR.h"

#define OPTION_STRING "i:f:t:C:hs"
#define DGAS_DEF_CONF_FILE "/etc/dgas/dgas_hlr.conf"

using namespace std;

bool lazyAccountCheck= false;//just for build FIXME

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
const char * hlr_tmp_sql_dbname;
ofstream logStream;
int system_log_level = 7;
bool needs_help = false;
bool statistics = false;
string id_buff = "%";
string from_buff = "%";
string to_buff = "%";
string confFileName = DGAS_DEF_CONF_FILE;
string hlr_logFileName;
string qtransInsertLog;

struct stat {
		string priority; 
		string count;
		};

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"id",1,0,'i'},
		{"from",1,0,'f'},
		{"to",1,0,'t'},
		{"stat",0,0,'s'},
		{"Conf",1,0,'C'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'i': id_buff = optarg; break;
			case 'f': from_buff = optarg; break;
			case 't': to_buff = optarg; break;
			case 's': statistics =true; break;		  
			case 'C': confFileName = optarg; break;
			case 'h': needs_help =1; break;		  
			default : break;
		}
	return 0;
}

int help (const char *progname)
{
        cerr << "\n " << progname << endl;
	cerr << " Version: "<< VERSION << endl;
	cerr << " Author: Andrea Guarise " << endl;
	cerr << " 28/08/2001 " << endl <<endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS] " << endl;
        cerr << "-C --Conf <confFile>   HLR configuration file name, if different" << endl;
        cerr << "                       from the default (/etc/dgas/dgas_hlr.conf)." << endl;
        cerr << "-i --id <id_buffer>    The id of the transaction (grid job id)."<<endl;
        cerr << "-f --from <from_id>    The grid id of the account to be debited (userDN)."<<endl;
        cerr << "-t --to <to_id>        The grid id of the account to be credited (CE ID)."<<endl;
        cerr << "-s --statistics        Report statistics and not full records."<<endl;
	cerr << endl;
	return 0;	
}


ostream& operator << ( ostream& os, const qTransaction& t )
{
	
	os << "|";
	struct tm *timeLog;
	time_t timeBuff = atoi((t.timestamp).c_str());
	timeLog = localtime(&timeBuff);
	char timeBuffString[22];
	strftime(timeBuffString,sizeof(timeBuffString),"%Y %b %d %T",timeLog);
	os <<  timeBuffString << "|";
	os << t.statusTime << "|";
	os << t.priority << "|";
	os << t.transactionId << "|";
	os << t.gridUser << "|";
	os << t.gridResource << "|";
	os << t.urSource << "|";
	os << t.uniqueChecksum << "|";
	os << t.accountingProcedure << "|";
	os << t.logData << "|";
	return os;
}

int getqTrans( string &transactionId, string &from, string &to, vector<qTransaction> &q)
{
	db hlrTmp(hlr_sql_server,
                         hlr_sql_user,
                         hlr_sql_password,
                         hlr_tmp_sql_dbname);
	if (hlrTmp.errNo == 0)
	{
		string queryStr = "SELECT * FROM trans_queue WHERE transaction_id LIKE '";
		queryStr += transactionId;
		queryStr += "' AND ";
		queryStr += " gridUser LIKE '";
		queryStr += from;
		queryStr += "' AND ";
		queryStr += " gridResource LIKE '";
		queryStr += to;
		queryStr += "'";
		dbResult result = hlrTmp.query(queryStr);
		if ( hlrTmp.errNo == 0 )
		{
			if (result.numRows() == 0)
			{
				return 1;
			}
			else
			{
				for (unsigned int i=0; i< result.numRows(); i++)
				{
					qTransaction buffer;
					buffer.transactionId=result.getItem(i,0);
					buffer.gridUser=result.getItem(i,1);
					buffer.gridResource=result.getItem(i,2);
					buffer.urSource=result.getItem(i,3);
					buffer.timestamp = result.getItem(i,5);
					buffer.logData = result.getItem(i,6);
					buffer.priority = atoi(result.getItem(i,7).c_str());
					buffer.statusTime = atoi(result.getItem(i,8).c_str());
					buffer.uniqueChecksum = result.getItem(i,9);
					buffer.accountingProcedure = result.getItem(i,10);
					q.push_back(buffer);	
				}
			}
		}
		else
		{
			return hlrTmp.errNo;
		}
	}
	else
	{
		return hlrTmp.errNo;
	}
	return 0;
}

int getStats(string &transactionId, string &from, string &to, vector<stat> &output )
{
	db hlrTmp(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_tmp_sql_dbname);
	if (hlrTmp.errNo == 0)
	{
		string queryStr = "SELECT priority, count(*) FROM trans_queue WHERE transaction_id LIKE '";
		queryStr += transactionId;
		queryStr += "' AND ";
		queryStr += " gridUser LIKE '";
		queryStr += from;
		queryStr += "' AND ";
		queryStr += " gridResource LIKE '";
		queryStr += to;
		queryStr += "'";
		queryStr += " GROUP BY priority";
		dbResult result = hlrTmp.query(queryStr);
		if ( hlrTmp.errNo == 0 )
		{
			if (result.numRows() == 0)
			{
				return 1;
			}
			else
			{
				for (unsigned int i=0; i< result.numRows(); i++)
				{
					stat buffer;
					buffer.priority = result.getItem(i,0);
					buffer.count = result.getItem(i,1);
					output.push_back(buffer);
				}
			}
		}
		else
		{
			return hlrTmp.errNo;
		}
	}	
	else
	{
		return hlrTmp.errNo;
	}
	return 0;
}

int main (int argc, char **argv)
{
	options ( argc, argv );
        map <string,string> confMap;
        if ( dgas_conf_read ( confFileName, &confMap ) != 0 )
	{
		cerr << "Error opening the configuration file: " << confFileName << endl;
		help(argv[0]);
		exit(1);
	}
	notfor2lHlr(confMap["is2ndLevelHlr"]);
        hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
        hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
        hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
        hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
        hlr_tmp_sql_dbname = (confMap["hlr_tmp_sql_dbname"]).c_str();
	hlr_logFileName = (confMap["hlr_def_log"]).c_str();
	if (needs_help)
	{
		help(argv[0]);
		return 0;
	}
	logStream.open( hlr_logFileName.c_str() , ios::app);
	if ( statistics )
	{
		vector<stat> output;
		if ( getStats(id_buff,from_buff, to_buff, output) == 0)
		{
			vector<stat>::const_iterator it = output.begin();
			while ( it != output.end() )
			{
				cout << "priority:" << (*it).priority;
				cout << ",count:" << (*it).count << endl;
				it++;
			}
		}
	}
	else
	{
		vector<qTransaction> q;
		if ( getqTrans(id_buff,from_buff, to_buff, q) == 0)
		{
			int count = 1;
			vector<qTransaction>::const_iterator it = q.begin();
			while ( it != q.end() )
			{
				cout << count << "|" << *it << endl;
				it++;
				count++;
			}
		}
	}
	return 0;
}
