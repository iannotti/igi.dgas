// DGAS (DataGrid Accounting System) 
// Server Daemon and protocol engines.
// 
// $Id: hlrQtransMgrd.cpp,v 1.1.2.1.4.14 2012/06/01 09:10:34 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
/***************************************************************************
 * Code borrowed from:
 *  authors   : 
 *  copyright : 
 ***************************************************************************/
//
//    

#include <cstdlib>
#include <getopt.h>
#include <unistd.h>
#include <csignal>
//c++ includes
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>

#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/dgas_lock.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"

#include "glite/dgas/hlr-service/base/qTransaction.h"
#include "glite/dgas/hlr-service/base/hlrTransaction.h"
#include "atmResBankClient.h"
#include "atmResBankClient2.h"
#include "glite/dgas/hlr-service/engines/engineCmnUtl.h"
#include "glite/dgas/common/pa/libPA_comm.h"
#include <setjmp.h>
#include "../base/serviceVersion.h"

#define OPTION_STRING "hl:L:c:e"
//global variables
//

using namespace std;
#ifndef GLITE_SOCKETPP
using namespace glite::wmsutils::tls::socket_pp;
#endif
const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
const char * hlr_tmp_sql_dbname;
ofstream logStream;

int system_log_level = 7;
jmp_buf return_to_top_level;
volatile sig_atomic_t keep_going = 1;
int help_flag = 0;
bool error_flag = false;
string configFileName = "";
string logFileName = "";
string lockFileName = "";
int hlr_qmgr_tPerIter = 10;
int hlr_qmgr_expPeriod = 600;
int hlr_qmgr_pollPeriod = 20;
int hlr_jobAuth_cleanup_interval = 10;
string logBuff;
bool priceIsMandatory = true;
bool useATMVersion2 = false;
bool strictAccountCheck = false;
bool lazyAccountCheck = false;

string formula = "";
string qtransInsertLog;

void
catch_alarm (int sig);

int
print_help(const char* progname)
{
	cerr<< endl;
	cerr<< "DGAS hlrTqd" <<endl;
	cerr<< "Version :" << VERSION << endl ;
	cerr<< "Author: A.Guarise <andrea.guarise@to.infn.it>"<< endl;
	cerr<< endl << "Usage: " << endl;
	cerr<< progname << " [OPTIONS]" << endl << endl;;
	cerr<< "OPTIONS:" <<endl;
	cerr<< "-l  --log <logFile>      Set the log file name (overwrites the value defined" << endl;
	cerr<< "                         in the HLR configuration file)." << endl;
	cerr<< "-L  --Lock <lockFile>    Set the lock file name (overwrites the value defined" << endl;
	cerr<< "                         in the HLR configuration file)." << endl;
	cerr<< "-c  --config <confFile>  HLR configuration file name, if different" << endl;
	cerr<< "                         from the default (/etc/dgas/dgas_hlr.conf)." << endl;
	cerr<< "-e  --error              Process the transactions in fatal error state. (this can be obtained also by issuing a SIGHUP to the daemon process.)" << endl;
	cerr<< "-h  --help               Print this help message." << endl;
	return 0;
}//print_help()


int 
get_options (int argc, char **argv)
{
	int	option_char;
	int 	option_index;
	static	struct option long_options[] =
	{
			{"help",0,0,'h'},
			{"log",1,0,'l'},
			{"error",0,0,'e'},
			{"Lock",1,0,'L'},
			{"config",1,0,'c'},
			{0,0,0,0}
	};
	while (( option_char = getopt_long (argc,
			argv,
			OPTION_STRING,
			long_options,
			&option_index)) != EOF)
		switch (option_char)
		{
		case 'h': help_flag = 1; break;
		case 'l': logFileName = optarg; break;
		case 'e': error_flag = true; break;
		case 'L': lockFileName = optarg; break;
		case 'c': configFileName = optarg; break;
		default : break;
		}
	return 0;
}//get_options()

class fmap {
public:
	string key;
	string value;

	fmap ( string _key = "",
			string _value = ""):
				key(_key),value(_value){;};
};

int computeCostFormula ( string f, cmnLogRecords& l, price& p,int& cost)
{
	string logBuff = "Determining cost from formula:" + f;
	hlr_log( logBuff, &logStream, 5);
	db hlrDb ( hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
	);
	vector<fmap> mv;
	mv.push_back(fmap("cpuTime", int2string(l.cpuTime)));
	mv.push_back(fmap("wallTime", int2string(l.wallTime)));
	mv.push_back(fmap("price", int2string(p.priceValue)));
	vector<fmap>::const_iterator it = mv.begin();
	while ( it != mv.end() )
	{
		size_t pos = 0;
		while ( pos != string::npos )
		{
			pos = f.rfind((*it).key);
			if ( pos != string::npos )
			{
				f.erase(pos,((*it).key).size());
				f.insert(pos,(*it).value);
			}
		}
		it++;
	}
	string queryStr =  "SELECT " + f;
	dbResult result = hlrDb.query(queryStr);
	if ( hlrDb.errNo == 0)
	{
		int numRows = result.numRows();
		if ( numRows == 1 )
		{
			cost = atoi((result.getItem(0,0)).c_str());
			logBuff = "determined cost:" + f + "=";
			logBuff += int2string(cost);
			hlr_log( logBuff, &logStream, 5);
		}
		else
		{
			string logBuff = "Ambiguous formula." + f;
			hlr_log( logBuff, &logStream, 5);
			return 2;
		}
	}
	else
	{
		string logBuff = "Formula syntax error." + f;
		hlr_log( logBuff, &logStream, 2);
		return 1;
	}
	return 0;
}

/*
int computeJobCost(string& logData, qTransaction& t, int& cost)
{
	int res =0;
	string logBuff = "Computing job cost...";
	hlr_log( logBuff, &logStream, 5);
	//contacts the PA and retrieves the price for ceId
	//then computes the jobCost. 
	//Success = 0;
	cmnLogRecords l;
	price p;
	res = cmnParseLog(logData, l);
	if ( res == 0 )
	{
		if ( l.economicAccounting != "yes" )
		{
			cost = 0;
			hlr_log( "Economic accounting not requested.", &logStream, 4);
			return 0;
		}
		else
		{
			p.resID = l.ceId; 
			if ( t.timestamp != "" && t.timestamp != "0" )
			{
				p.time = atoi((t.timestamp).c_str()); 
			}
			else
			{
				p.time = time(NULL); 
			}
			res = dgas_pa_client( l.priceAuthority ,&p);
			if ( res == 0 )
			{

				if ( formula != "" )
				{
					if ( computeCostFormula(formula, l, p, cost) != 0 )
					{
						logBuff = "ERROR in cost formula:";
						logBuff += formula;
						hlr_log( logBuff, &logStream, 2);
						return 3;
					}
				}
				else
				{
					cost = ( p.priceValue * l.cpuTime )/100;
				}
			}
			else
			{
				logBuff = "Coluld not determine price!, PA returned:";
				logBuff += int2string(res);
				hlr_log( logBuff, &logStream, 1);
				//no price
				return 2;
			}
		}
	}
	logBuff = "...jobCost=" + int2string(cost);
	hlr_log( logBuff, &logStream, 4);
	return 0;
}
*/

string retrieveUserCertSubject(string& s)
{
	size_t pos = s.find("/CN=proxy");
	if ( pos != string::npos )
	{
		return s.substr(0,pos);
	}
	return s;
}

void mainLoop( int hlr_qmgr_tPerIter )
{
	int i = 0;
	while ( (i < hlr_qmgr_tPerIter) && keep_going)
	{
		time_t time0 = time(NULL);
		vector<string> keys;
		if ( qTrans::get(i, keys) == 0 && keep_going )
		{
			logBuff = "hlr_qMgr::mainLoop():priority:" + int2string(i);
			hlr_log( logBuff, &logStream, 7);
			vector<string>::const_iterator it = keys.begin();
			vector<string>::const_iterator it_end = keys.end();
			while ( (it != it_end) && keep_going )
			{
				logBuff = "hlr_qMgr::mainLoop():Processing:" + *it;
				hlr_log( logBuff, &logStream, 4);

				qTransaction trans;
				if (trans.get(*it) != 0 )
				{
					logBuff = "ERROR retrieving:" + *it;
					hlr_log( logBuff, &logStream, 2);
					it++;
					continue;
				}
				string logDataBuff = trans.logData;
				string userCertBuff = retrieveUserCertSubject(trans.gridUser);
				hlrTransaction t(
						0,
						trans.transactionId,
						userCertBuff,
						trans.gridResource,
						trans.urSource,
						0,
						trans.timestamp,
						trans.logData,
						trans.uniqueChecksum,
						trans.accountingProcedure
				);
				logBuff = "Processing: ";
				logBuff += trans.transactionId;
				hlr_log (logBuff, &logStream, 4);
				logBuff = "hlrTransaction, transaction_id: ";
				logBuff += t.id;
				logBuff += ",gridUser: ";
				logBuff += t.gridUser;
				logBuff += ",gridResource: ";
				logBuff += t.gridResource;
				logBuff += ",urSource: ";
				logBuff += t.urSource;
				logBuff += ",amount: ";
				logBuff += int2string(t.amount);
				logBuff += ",timeStamp: ";
				logBuff += t.timeStamp;
				logBuff += ",uniqueChecksum: ";
				logBuff += t.uniqueChecksum;
				logBuff += ",accountingProcedure: ";
				logBuff += t.accountingProcedure;
				hlr_log (logBuff, &logStream, 6);
				/*
				if ( computeJobCost(trans.logData, trans, t.amount) != 0)
				{
					if ( priceIsMandatory )
					{
						logBuff = "WARNING: Job cost is mandatory for this server, postponing";
						hlr_log( logBuff, &logStream, 1);
						if ((time(NULL) - trans.statusTime) > hlr_qmgr_expPeriod )
						{
							trans.priority++;
							trans.statusTime = time(NULL);
							trans.update();
						}
						it++;
						continue;
					}
				}
				*/
				logBuff = "Processing: " + t.id;
				hlr_log( logBuff, &logStream, 4);
				alarm(60);
				int res = 0;
				if ( useATMVersion2 )
				{
					res = dgasResBankClient::bankClient2( t );
				}
				else
				{
					res = dgasResBankClient::bankClient( t );
				}
				alarm(0);
				if ( res != 0 )
				{
					logBuff = "ERROR processing:" + t.id + ":" +int2string(res);
					hlr_log( logBuff, &logStream, 1);
					if ( ( res == atoi(ATM_E_DUPLICATED_A) ) || ( res == atoi(ATM_E_DUPLICATED_B) ))
					{
						logBuff = "Delete duplicated record:" + t.id;
						hlr_log( logBuff, &logStream, 4);
						trans.remove();
					}
					else
					{
						if ((time(NULL) - trans.statusTime) > hlr_qmgr_expPeriod )
						{
							logBuff = "Increasing priority:";
							trans.priority++;
							trans.statusTime = time(NULL);
							logBuff += "New Priority:" + int2string(trans.priority);
							logBuff += ",New StatusTime:" + int2string(trans.statusTime);
							int res = trans.update();
							logBuff += ",Ret:" + int2string(res);
							hlr_log( logBuff, &logStream, 7);
						}
					}
				}
				else
				{
					logBuff = "Processed:" + t.id;
					hlr_log( logBuff, &logStream, 4);

					if ( trans.remove() != 0 )
					{
						hlr_log( "ERROR removing processed record from queue", &logStream, 1);
					}
				}
				it++;
			}
		}
		i++;
		time_t time1 = time(NULL);
		float recSec = 0.0;
		if ( (time1-time0) != 0 ) recSec = ((float)i)/((float)(time1 - time0));
		string logBuff = "Elapsed:" + int2string(time1-time0) + "secs.Processed:" + int2string(i) + " records, with:" + int2string(recSec) + " rec/sec";
		hlr_log( logBuff, &logStream, 5);
	}
	return;
}


int processErrorTransactions(int i)
{
	mainLoop(i); 
	time_t currtime;
	currtime = time(NULL);
	string fileName = "/usr/var/hlr_tmp_dump-";
	fileName += int2string(currtime);
	fileName += ".sqldump";
	qTrans::archiveGreaterThan(i-1,fileName);
	qTrans::removeGreaterThan(i-1);
	return 0;
}

int putLock(string lockFile)
{
	dgasLock Lock(lockFile);
	if ( Lock.exists() )
	{
		//the lock file already exists, return an error.
		return 1;
	}
	else
	{
		//the lock file doesn't exists, therefore creates it
		//and insert some important information inside the file.
		if ( Lock.put() != 0 )
		{
			//there was an error creating the lock file.
			//exit with an error.
			return 2;
		}
		else
		{
			return 0;
		}
	}

}

int removeLock( string lockFile)
{
	dgasLock Lock(lockFile);
	if ( Lock.exists() )
	{
		//the lock file exits, remove it.
		if ( Lock.remove() != 0 )
		{
			hlr_log("ERROR removing lock file", &logStream, 1);
			//error removing the lock
			return 2;
		}
		else
		{
			hlr_log("lock file removed", &logStream, 4);
			return 0;
		}
	}
	else
	{
		hlr_log("lock file doesn't exists", &logStream, 1);
		return 1;
	}
}


void fatal_error_signal (int sig)
{
	keep_going = 0;
	cerr << "Exiting..." << endl;
	signal (sig, fatal_error_signal);
}

void sighup_signal (int sig)
{
	keep_going = 1;
	error_flag = true;
	cerr << "Set error_flag to true" << endl;
	//signal (sig, sighup_signal);
}

int main ( int argc, char * argv[] )
{
	get_options(argc, argv);
	if ( help_flag == 1 )
	{
		print_help(argv[0]);
		exit(1);
	}
	map <string,string> confMap;
	dgas_conf_read ( configFileName, &confMap );
	hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
	hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
	hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
	hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
	hlr_tmp_sql_dbname = (confMap["hlr_tmp_sql_dbname"]).c_str();
	hlr_qmgr_expPeriod = atoi((confMap["hlr_qmgr_expPeriod"]).c_str());
	hlr_qmgr_tPerIter = atoi((confMap["hlr_qmgr_tPerIter"]).c_str());
	hlr_qmgr_pollPeriod = atoi((confMap["hlr_qmgr_pollPeriod"]).c_str());
	hlr_jobAuth_cleanup_interval = atoi((confMap["hlr_jobAuth_cleanup_interval"]).c_str());
	if (hlr_jobAuth_cleanup_interval == 0)
		hlr_jobAuth_cleanup_interval = 10;
	if ( confMap["price_is_mandatory"] == "false" )
	{
		priceIsMandatory = false;
	}
	if ( confMap["costFormula"] != "" )
	{
		formula =  confMap["costFormula"];
	}
	if (logFileName == "" )
	{
		logFileName = confMap["hlr_qmgr_def_log"];
	}
	if ( bootstrapLog(logFileName, &logStream) != 0 )
	{
		cerr << "ERROR bootstrapping log file:" << logFileName << endl;
		exit(1);
	}
	if (lockFileName == "" )
	{
		lockFileName = confMap["hlr_qmgr_def_lock"];
		logBuff = "Using default lock file: " +
				lockFileName;
		hlr_log(logBuff, &logStream, 6);
	}
	if ( putLock(lockFileName) != 0 )
	{
		hlr_log("ERROR creating lock file. Startup failed", &logStream, 1);
		logStream.close();
		exit(atoi(E_LOCK_OPEN));
	}
	if ( confMap["systemLogLevel"] != "" )
	{
		system_log_level = atoi((confMap["systemLogLevel"]).c_str());
	}
	if ( confMap["accountCheckPolicy"] != "" )
	{
		if ( confMap["accountCheckPolicy"] == "strict")
		{
			strictAccountCheck = true;
			lazyAccountCheck =false;
		}
		if ( confMap["accountCheckPolicy"] == "lazy")
		{
			strictAccountCheck = false;
			lazyAccountCheck = true;
		}
	}
	if ( confMap["useATMVersion2"] == "true" )
	{
		useATMVersion2 = true;
	}
	logBuff = "Log level:" + confMap["systemLogLevel"];
	hlr_log(logBuff,&logStream,4);
	serviceVersion thisServiceVersion(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	if ( !thisServiceVersion.tableExists() )
	{
		thisServiceVersion.tableCreate();
	}
	thisServiceVersion.setService("dgas-hlr-qmgr");
	thisServiceVersion.setVersion(VERSION);
	thisServiceVersion.setHost("localhost");
	thisServiceVersion.setConfFile(configFileName);
	thisServiceVersion.setLockFile(lockFileName);
	thisServiceVersion.setLogFile(logFileName);
	thisServiceVersion.write();
	thisServiceVersion.updateStartup();
	signal (SIGTERM, fatal_error_signal);
	signal (SIGINT, fatal_error_signal);
	signal (SIGHUP, sighup_signal);
	signal (SIGALRM, catch_alarm);
	if (setjmp (return_to_top_level) != 0)
	{
		hlr_log("ERROR: Could not set long jmp!", &logStream, 1);
	}
	while (keep_going)
	{
		if ( error_flag )
		{
			hlr_log("start processErrorTransactions", &logStream, 4);
			error_flag= false;
			processErrorTransactions(hlr_qmgr_tPerIter+1);
			hlr_log("end processErrorTransactions", &logStream, 4);
		}
		hlr_log("Entering main loop", &logStream, 8);
		mainLoop(hlr_qmgr_tPerIter);
		for ( int s=0; s < hlr_qmgr_pollPeriod ; s++ )
		{		
			if (keep_going) sleep(1);
		}
	}

	hlr_log ( "Received termination signal:exiting...", &logStream, 4);
	removeLock(lockFileName);
	logStream.close();
	return 0;
}

void
catch_alarm (int sig)
{
	longjmp (return_to_top_level, 1);
}
