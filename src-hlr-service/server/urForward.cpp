#include <iostream>
#include <string>
#include <csignal>
#include <getopt.h>
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgas_lock.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "urForwardFactory.h"
#include "../base/serviceVersion.h"

#define DEFAULT_CONFIG "/etc/dgas/dgas_hlr.conf"

#define OPTION_STRING "hdl:L:c:f:r"

using namespace std;

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
const char * hlr_tmp_sql_dbname;
int system_log_level = 7;
ofstream logStream;

int forwardInterval = 86400; //Default forward interval in seconds.
volatile sig_atomic_t keep_going = 1;
string recordsPerConnection = "2000";
string acceptRecordsStartDate ="";
string acceptRecordsEndDate = "";
bool needsHelp = false;
bool isDaemon = false;
bool isReset = false;
string logFileName = "";
string lockFileName = "";
string configFileName =  DEFAULT_CONFIG;
string serversFileName = DEFAULT_CONFIG;


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
			//error removing the lock
			return 2;
		}
		else
		{
			return 0;
		}
	}
	else
	{
		return 1;
	}
}


int print_help(const char* progname)
{
	cerr << endl;
	cerr << progname << " version:" << VERSION << endl;
	cerr << endl << "Usage: " << endl;
	cerr << progname << "[OPTIONS]" << endl;
	cerr << "OPTIONS:" <<endl;
	cerr << "-h --help		Prints this help." << endl;
	cerr << "-d --daemon		Runs as daemon." << endl;
	cerr << "-r --reset		Send to the 2L HLRs the request to reset the records for this HLR. records will be republished then." << endl;
	cerr << "-l  --log <logFile>      Set the log file name (overwrites the value defined" << endl;
	cerr<< "-L  --Lock <lockFile>    Set the lock file name (overwrites the value defined" << endl;
	cerr<< "-c  --config <confFile>  HLR configuration file name, if different" << endl;
	cerr<< "-f  --file <serverFile>  File containing listo of 2nd level HLRs to forward usage records to." << endl;
	return 0;
}

int
get_options (int argc, char **argv)
{
	int     option_char;
	int     option_index;
	static  struct option long_options[] =
	{
			{"help",0,0,'h'},
			{"daemon",0,0,'d'},
			{"reset",0,0,'r'},
			{"log",1,0,'l'},
			{"Lock",1,0,'L'},
			{"config",1,0,'c'},
			{"file",1,0,'f'},
			{0,0,0,0}
	};
	while (( option_char = getopt_long (argc,
			argv,
			OPTION_STRING,
			long_options,
			&option_index)) != EOF)
		switch (option_char)
		{
		case 'h': needsHelp = true; break;
		case 'd': isDaemon = true; break;
		case 'r': isDaemon = false; isReset=true; break;
		case 'l': logFileName = optarg; break;
		case 'L': lockFileName = optarg; break;
		case 'c': configFileName = optarg; break;
		case 'f': serversFileName = optarg; break;
		default : break;
		}
	return 0;

}

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

int main ( int argc, char * argv[] )
{
	int res = 0;
	string logBuff= "";
	get_options(argc, argv);
	if ( needsHelp )
	{
		print_help(argv[0]);
		return 0;
	}
	confParams conf;
	conf.defConnTimeout = 240;
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
	if (logFileName == "")
	{
		logFileName = confMap["urForwardLog"];
	}
	if ( bootstrapLog(logFileName, &logStream) != 0 )
	{
		cerr << "Error bootstrapping the Log file:" << endl;
		cerr << logFileName<< endl;
		exit(1);
	}
	if ( isReset )
	{
		if ( lockFileName == "" )
		{
			lockFileName = "/tmp/urResetRequest.lock";
		}
	}
	else
	{
		if (lockFileName == "" )
		{
			lockFileName = confMap["urForwardLock"];
		}
	}
	if ( putLock(lockFileName) != 0 )
	{
		hlr_log("Startup failed: Error creating the lock file.",
				&logStream, 1);
		exit(atoi(E_LOCK_OPEN));
	}
	if ( confMap["defConnTimeOut"] != "" )
	{
		conf.defConnTimeout = atoi((confMap["defConnTimeOut"]).c_str());
	}
	if ( confMap["systemLogLevel"] != "" )
	{
		system_log_level = atoi((confMap["systemLogLevel"]).c_str());
	}
	logBuff = "Log level:";
	logBuff += int2string(system_log_level);
	hlr_log(logBuff,&logStream,5);
	if ( confMap["sendRecordsStartDate"] != "" )
	{
		conf.sendRecordsStartDate = confMap["sendRecordsStartDate"] ;
	}
	if ( confMap["sendRecordsEndDate"] != "" )
	{
		conf.sendRecordsEndDate = confMap["sendRecordsEndDate"] ;
	}
	if ( confMap["recordsPerConnection"] != "" )
	{
		recordsPerConnection = confMap["recordsPerConnection"];
	}
	if ( confMap["serversFile"] != "" )
	{
		serversFileName = confMap["serversFile"];
	}
	conf.serversFile = serversFileName;
	conf.recordsPerConnection = recordsPerConnection;
	if ( confMap["forwardPeriod"] != "" )
	{
		forwardInterval = atoi((confMap["forwardPeriod"]).c_str());
	}
	logBuff = "Start sending records (sendRecordsStartDate) from:";
	logBuff += conf.sendRecordsStartDate;
	hlr_log(logBuff,&logStream,4);
	logBuff = "Sending until (sendRecordsEndDate):";
	logBuff += conf.sendRecordsEndDate;
	hlr_log(logBuff,&logStream,4);
	logBuff = "Number of record sent per iteration (recordsPerConnection):";
	logBuff += conf.recordsPerConnection;
	hlr_log(logBuff,&logStream,4);
	serviceVersion thisServiceVersion(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	if ( !thisServiceVersion.tableExists() )
	{
		thisServiceVersion.tableCreate();
	}
	thisServiceVersion.setService("dgas-hlr-urforward");
	thisServiceVersion.setVersion(VERSION);
	thisServiceVersion.setHost("localhost");
	thisServiceVersion.setConfFile(configFileName);
	thisServiceVersion.setLockFile(lockFileName);
	thisServiceVersion.setLogFile(logFileName);
	thisServiceVersion.write();
	thisServiceVersion.updateStartup();
	database dBase(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	urForward forwarder (conf, dBase);
	signal (SIGTERM, fatal_error_signal);
	signal (SIGINT, fatal_error_signal);
	if ( isReset )
	{
		res = forwarder.reset();
		if ( res != 0 )
		{
			string logBuff = "Error sending reset request:" + int2string(res);
			hlr_log (logBuff, &logStream, 2);
		}
	}
	else
	{
		while ( keep_going )
		{
			forwarder.run();
			if ( !isDaemon )
				break;
			for ( int s=0; s < forwardInterval ; s++ )
			{
				if (keep_going) sleep(1);
			}
		}
	}	
	hlr_log ("Removing lock file.", &logStream, 6);
	removeLock(lockFileName);
	hlr_log ("Exiting.", &logStream, 7);
	return res;	
}

