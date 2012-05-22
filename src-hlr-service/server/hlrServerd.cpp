// DGAS (DataGrid Accounting System) 
// Server Daemon and protocol engines.
// 
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
/***************************************************************************
 * Code borrowed from:
 *  authors   : Salvatore Monforte <salvatore.monforte@ct.infn.it>
 *  copyright : (C) 2001-2002 INFN
 ***************************************************************************/
//
//    


#include <cstdlib>
#include <getopt.h>
#include <csignal>
//c++ includes
#include <string>
#include <iostream>
#include <fstream>
//local includes
//hlr api includes
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgas_lock.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/localSecurity.h"
#include "daemonFactory.h"
#include "pingEngine.hpp"
#include "glite/dgas/common/base/dgasVersion.h"
#include "xmlHlrHelper.h"
//socketpp includes
#include "glite/dgas/common/tls/GSISocketServer.h"
#include "glite/dgas/common/tls/GSISocketAgent.h"
#include "../base/serviceVersion.h"
#define DEFAULT_CONFIG "/etc/dgas/dgas_hlr.conf"
#define OPTION_STRING "hp:l:L:c:"

using namespace std;
#ifndef GLITE_SOCKETPP
using namespace glite::wmsutils::tls::socket_pp;
#endif

ofstream logStream;

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
const char * hlr_tmp_sql_dbname;
string dgas_var_dir = "/usr/var/";
int system_log_level = 7;
int defConnTimeOut = 20;
int help_flag = 0;
int server_port = -1;
int threadNumber = 3;
int activeThreads = 0;
int authErrors=0;
int threadUsecDelay=0;
int threadPoolUsecDelay=0;
int recordsPerBulkInsert = 20;
int messageReservedMemory = 8196000;
bool strictAccountCheck = false;
bool lazyAccountCheck = false;
bool authUserSqlQueries = false;
bool is2ndLevelHlr = false;
bool useATMVersion2 = false;
bool checkVo = false;
bool useMergeTables = false;
bool deleteOnReset = true;
bool useBulkInsert = true;
string maxItemsPerQuery = "1000";
string acceptRecordsStartDate ="";
string acceptRecordsEndDate = "";
string recordsPerConnection = "2000";
string mergeTablesDefinitions = "/etc/dgas/glite_dgas_merge.def";
string logFileName = "";
string lockFileName = "";
string configFileName = DEFAULT_CONFIG;
string server_contact = "";
string hlr_gridMapFile = "/etc/grid-security/grid-mapfile";
string hlr_user = "";
string qtransInsertLog = "";
string authQueryTables = "jobTransSummary";

pthread_mutex_t auth_mutex = PTHREAD_MUTEX_INITIALIZER;

bool restart;

volatile sig_atomic_t keep_going = 1;
statusInfo serverStatus;
errorInfo  errorStatus;

listenerStatus lStatus;

int print_help(const char* progname)
{
	cerr<< endl;
	cerr<< "DGAS hlrServerd" <<endl;
	cerr<< "Version :" << VERSION << endl ;
	cerr<< "Author: A.Guarise <andrea.guarise@to.infn.it>"<< endl;
	cerr<< endl << "Usage: " << endl;
	cerr<< progname << " [OPTIONS]" << endl << endl;;
	cerr<< "OPTIONS:" <<endl;
	cerr<< "-p  --port <port_num>    Set the listening port (overwrites the value defined" << endl;
	cerr<< "                         in the HLR configuration file)." << endl;
	cerr<< "-l  --log <logFile>      Set the log file name (overwrites the value defined" << endl;
	cerr<< "                         in the HLR configuration file)." << endl;
	cerr<< "-L  --Lock <lockFile>    Set the lock file name (overwrites the value defined" << endl;
	cerr<< "                         in the HLR configuration file)." << endl;
	cerr<< "-c  --config <confFile>  HLR configuration file name, if different" << endl;
	cerr<< "                         from the default (/etc/dgas/dgas_hlr.conf)." << endl;
	cerr<< "-h  --help               Print this help message." << endl;
	return 0;
}//print_help()


int get_options (int argc, char **argv)
{
	int	option_char;
	int 	option_index;
	static	struct option long_options[] =
	{
			{"help",0,0,'h'},
			{"port",1,0,'p'},
			{"log",1,0,'l'},
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
		case 'p': server_port = atoi(optarg); break;
		case 'l': logFileName = optarg; break;
		case 'L': lockFileName = optarg; break;
		case 'c': configFileName = optarg; break;
		default : break;
		}
	return 0;
}//get_options()


void fatal_error_signal (int sig)
{
	keep_going = 0;
	cerr << "Exiting..." << endl;
	signal (sig, fatal_error_signal);
}

void pipe_signal (int sig)
{
	signal (sig, pipe_signal);
}

void usr1_signal (int sig)
{
	signal (sig, usr1_signal);
}

int main ( int argc, char * argv[] )
{
	string logString = "";
	get_options(argc, argv);
	if ( help_flag )
	{
		print_help(argv[0]);
		return 0;
	}
	map <string,string> confMap;
	if ( dgas_conf_read ( configFileName, &confMap ) != 0 )
	{
		cerr << "Can't open configuration file:"<< configFileName << endl;
		exit(1);
	}
	hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
	hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
	hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
	hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
	hlr_tmp_sql_dbname = (confMap["hlr_tmp_sql_dbname"]).c_str();
	server_contact = (confMap["hlr_server_contact"]).c_str();
	hlr_gridMapFile = (confMap["hlr_gridmapfile"]).c_str();
	hlr_user = (confMap["hlr_user"]).c_str();
	securityStruct security;
	security.hostProxyFile = confMap["hostProxyFile"];
	security.gridmapFile = confMap["gridmapFile"];

	serverStatus.engines="UI:ATM:PING";
	serverStatus.uiRequests=0;
	serverStatus.ATMRequests=0;
	serverStatus.pingRequests=0;

	if (server_port == -1)
	{
		server_port = atoi((confMap["hlr_def_port"]).c_str());
		if ( server_port == 0 )
		{
			cerr << "Please check that in "<< configFileName << endl;
			cerr << "The entry hlr_def_port is set up correctly" << endl;
			exit(1);
		}
	}
	if (logFileName == "")
	{
		logFileName = confMap["hlr_def_log"];
	}
	if ( bootstrapLog(logFileName, &logStream) != 0 )
	{
		cerr << "ERROR bootstrapping Log file " << endl;
		cerr << logFileName << endl;
		exit(1);
	}
	if ( changeUser(hlr_user) != 0 )
	{
		string logBuff= "Could not change user to ";
		logBuff += hlr_user;
		hlr_log(logBuff,&logStream,3);
	}
	if ( setSecurityEnvironment(security) != 0 )
	{
		hlr_log("ERROR setting security environment!",&logStream,2);
		exit(1);
	}
	hlr_log ( "Listening on port " + int2string(server_port), &logStream, 5 );
	if (lockFileName == "" )
	{
		lockFileName = confMap["hlr_def_lock"];
	}
	if ( putLock(lockFileName) != 0 )
	{
		//for some reasons it was impossible to create the lock
		//therefore the server won't be started.
		hlr_log("ERROR creating lock file. Startup failed",
				&logStream, 0);
		exit(atoi(E_LOCK_OPEN));
	}
	if (confMap["qtransInsertLog"] != "" )
	{
		qtransInsertLog = confMap["qtransInsertLog"];
		string logBuff = "Using transaction log file:";
		logBuff += qtransInsertLog;
		hlr_log(logBuff,&logStream,5);
	}
	if ( confMap["thread_number"] != "" )
	{
		threadNumber = atoi((confMap["thread_number"]).c_str());
	}
	string logBuff = "Number of threads for this server:";
	logBuff += int2string(threadNumber);
	hlr_log(logBuff,&logStream,5);
	if ( confMap["strictAccountCheck"] != "" )//deprecated
	{
		string strictAccountCheckBuff = confMap["strictAccountCheck"];
		if ( strictAccountCheckBuff == "true" )
		{
			strictAccountCheck = true;
			lazyAccountCheck =false;
		}
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
	if ( confMap["defConnTimeOut"] != "" )
	{
		defConnTimeOut = atoi((confMap["defConnTimeOut"]).c_str());
	}
	logBuff = "Timeout for this server:";
	logBuff += int2string(defConnTimeOut);
	hlr_log(logBuff,&logStream,5);
	if ( confMap["systemLogLevel"] != "" )
	{
		system_log_level = atoi((confMap["systemLogLevel"]).c_str());
	}
	logBuff = "Log level:";
	logBuff += int2string(system_log_level);
	hlr_log(logBuff,&logStream,4);
	if ( confMap["maxItemsPerQuery"] != "" )
	{
		maxItemsPerQuery = confMap["maxItemsPerQuery"];
	}
	logBuff = "Max number of rows retrieved per query:";
	logBuff += maxItemsPerQuery;
	hlr_log(logBuff,&logStream,5);
	if ( confMap["dgas_var_dir"] != "" )
	{
		dgas_var_dir = confMap["dgas_var_dir"];
	}
	if ( confMap["authUserSqlQueries"] == "true" )
	{
		authUserSqlQueries = true;
	}
	if ( confMap["authQueryTables"] != "" )
	{
		authQueryTables = confMap["authQueryTables"];
	}
	if ( confMap["useMergeTables"] == "true" )
	{
		useMergeTables = true;
	}
	if ( confMap["mergeTablesDefinitions"] != "" )
	{
		mergeTablesDefinitions = confMap["mergeTablesDefinitions"];
	}
	if ( confMap["useATMVersion2"] == "true" )
	{
		useATMVersion2 = true;
	}
	if ( confMap["threadUsecDelay"] != ""  )
	{
		threadUsecDelay = atoi((confMap["threadUsecDelay"]).c_str());
	}
	if ( confMap["threadPoolUsecDelay"] != ""  )
	{
		threadPoolUsecDelay = atoi((confMap["threadPoolUsecDelay"]).c_str());
	}
	if ( confMap["messageReservedMemory"] != ""  )
	{
		messageReservedMemory = atoi((confMap["messageReservedMemory"]).c_str());
	}
	//check for 2ndLevelHlr
	if ( confMap["is2ndLevelHlr"] == "true" )
	{
		is2ndLevelHlr = true;
		logBuff = "is2ndLevelHlr set to \"true\"";
		hlr_log(logBuff,&logStream,6);
		logBuff = "HLR initialized as concentrator (2nd level)";
		hlr_log(logBuff,&logStream,5);
		if ( confMap["checkUserVo"] == "true" )
		{
			checkVo = true;
		}
		if ( confMap["acceptRecordsStartDate"] != "" )
		{
			acceptRecordsStartDate = confMap["acceptRecordsStartDate"];
		}
		if ( confMap["acceptRecordsEndDate"] != "" )
		{
			acceptRecordsEndDate = confMap["acceptRecordsEndDate"];
		}
		if ( confMap["recordsPerConnection"] != "" )
		{
			recordsPerConnection = confMap["recordsPerConnection"];
		}
		if ( confMap["deleteOnReset"] == "false" )
		{
			deleteOnReset = false;
		}
		if ( confMap["useBulkInsert"] == "false" )
		{
			useBulkInsert = false;
		}
		if ( confMap["recordsPerBulkInsert"] != "" )
		{
			recordsPerBulkInsert = atoi((confMap["recordsPerBulkInsert"]).c_str());
		}
		logBuff = "Accepting records since (acceptRecordsStartDate):";
		logBuff += acceptRecordsStartDate;
		hlr_log(logBuff,&logStream,7); 
		logBuff = "Accepting records until (acceptRecordsEndDate):";
		logBuff += acceptRecordsEndDate;
		hlr_log(logBuff,&logStream,7); 
		logBuff = "Records accepted per iteration (recordsPerConnection):";
		logBuff += recordsPerConnection;
		hlr_log(logBuff,&logStream,7); 
		logBuff = "Records per bulk insert (recordsPerBulkInsert):";
		logBuff += int2string(recordsPerBulkInsert);
		hlr_log(logBuff,&logStream,7);
		logBuff = "Use bulk insert (useBulkInsert):";
		logBuff += ( useBulkInsert ) ? "true" : "false" ;
		hlr_log(logBuff,&logStream,7);
		serverStatus.engines="UI:CONCENTRATOR:PING";
	}
	serviceVersion thisServiceVersion(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	if ( !thisServiceVersion.tableExists() )
	{
		thisServiceVersion.tableCreate();
	}
	thisServiceVersion.setService("dgas-hlr-listener");
	thisServiceVersion.setVersion(VERSION);
	thisServiceVersion.setHost("localhost");
	thisServiceVersion.setConfFile(configFileName);
	thisServiceVersion.setLockFile(lockFileName);
	thisServiceVersion.setLogFile(logFileName);
	thisServiceVersion.write();
	thisServiceVersion.updateStartup();

	lStatus.server_port = server_port;
	lStatus.logFileName = logFileName;
	lStatus.lockFileName = lockFileName;
	lStatus.qtransInsertLog =qtransInsertLog;
	lStatus.threadNumber = threadNumber;
	lStatus.strictAccountCheck = strictAccountCheck;
	lStatus.defConnTimeOut = defConnTimeOut;
	lStatus.system_log_level = system_log_level;
	lStatus.maxItemsPerQuery = maxItemsPerQuery;
	lStatus.dgas_var_dir = dgas_var_dir;
	lStatus.authUserSqlQueries = authUserSqlQueries;
	lStatus.useMergeTables = useMergeTables;
	lStatus.mergeTablesDefinitions = mergeTablesDefinitions;
	lStatus.threadUsecDelay =threadUsecDelay;
	lStatus.threadPoolUsecDelay =threadPoolUsecDelay;
	lStatus.is2ndLevelHlr = is2ndLevelHlr;
	lStatus.checkVo = checkVo;
	lStatus.acceptRecordsStartDate = acceptRecordsStartDate;
	lStatus.acceptRecordsEndDate = acceptRecordsEndDate;
	lStatus.recordsPerConnection = recordsPerConnection;
	lStatus.deleteOnReset = deleteOnReset;
	lStatus.useBulkInsert = useBulkInsert;
	lStatus.recordsPerBulkInsert = recordsPerBulkInsert;
	// instantiate the Socket server
	GSISocketServer *theServer = new GSISocketServer(server_port,threadNumber);
	theServer->LimitedProxyMode(GSISocketServer::multi);
	if ( !(theServer -> Open()) )	
	{
		hlr_log( "ERROR opening the listener! Exiting", &logStream, 0 );
		removeLock(lockFileName);
		exit(atoi(E_SERVER_START));
	}
	else
	{
		pthread_attr_t attr;
		pthread_attr_init(&attr);
		pthread_t* thread[threadNumber];
		threadStruct* ts[threadNumber];
		signal (SIGTERM, fatal_error_signal);
		signal (SIGINT, fatal_error_signal);
		signal (SIGPIPE, pipe_signal);
		signal (SIGUSR1, usr1_signal);
		//startup threadNumber listeners.
		GSISocketAgent* theAgent[threadNumber];
		for (int i=0; ( i< threadNumber ) && keep_going; i++ )
		{
			theAgent[i] = NULL;
		}
		pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);//!!!
		while (keep_going)
		{
			int lastThread = 0;
			for (int i=0; ( i< threadNumber ) && keep_going; i++ )
			{
				if ( activeThreads >= threadNumber )
				{
					continue;
				}
				theAgent[i] = NULL;
				logString = "Listening for incoming connections";
				hlr_log(logString,&logStream,7);
				logString = "Creating agent:" + int2string(i);
				hlr_log(logString,&logStream,8);
				while ( keep_going )
				{
					theAgent[i] = theServer->Listen();
					if (theAgent[i])
					{
						theAgent[i] -> SetTimeout( defConnTimeOut );
						break;//while (inner,listen)
					}
				}
				lStatus.activeThreads = activeThreads;
				logString = "Number of active threads:" + int2string(lStatus.activeThreads);
				hlr_log(logString,&logStream,8);
				if ( theAgent[i] != NULL )
				{
					thread[i] = new pthread_t;
					ts[i] = new threadStruct;
					ts[i]->s = theServer;
					ts[i]->a = theAgent[i];
					ts[i]->tN = i;
					logString = "spawning thread" + int2string(i);
					hlr_log(logString,&logStream,8);
					int res = pthread_create(thread[i],&attr,&thrLoop,(void *)ts[i]);
					logString = "thread creation" + int2string(i) + ", returned:" + int2string(res);
					hlr_log(logString,&logStream,9);
					lastThread = i;
				}
			}
			usleep(threadPoolUsecDelay);
		}
	}			
	theServer->Close();
	delete theServer;
	hlr_log ("Server closed, removing lock file", &logStream, 4);
	removeLock(lockFileName);
	hlr_log ("Exiting", &logStream, 6);
	return 0;
}
