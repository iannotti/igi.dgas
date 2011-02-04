// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: legacyRecordManager.cpp,v 1.1.2.5 2011/02/04 09:41:16 aguarise Exp $
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

// ---------------------------------------------------------------------------
//  Includes
// ---------------------------------------------------------------------------
#include <unistd.h>
#include <csignal>
#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <map>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgas_lock.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/dgas-consumers/consumers/legacyRecordManager.h"
#include "glite/dgas/hlr-service/engines/atmResourceEngine.h"

#define E_CONFIG 10
#define E_BROKER_URI 11

typedef pair <string,unsigned char> fileType;

ofstream logStream;
const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
const char * hlr_tmp_sql_dbname;
string qtransInsertLog;
bool strictAccountCheck;
bool lazyAccountCheck;

volatile sig_atomic_t goOn = 1;

static int
     one (const struct dirent64 *unused)
     {
       return 1;
     }

void exit_signal (int sig)
{
	goOn = 0;
	signal (sig, exit_signal);
}

int putLock(string lockFile)
{
        dgasLock Lock(lockFile);
        if ( Lock.exists() )
        {
                return 1;
        }
        else
        {
                if ( Lock.put() != 0 )
                {
                        return 2;
                }
                else
                {
                        return 0;
                }
        }
}

int removeLock(string lockFile)
{
        dgasLock Lock(lockFile);
        if ( Lock.exists() )
        {
                if ( Lock.remove() != 0 )
                {
                        hlr_log("Error removing the lock file", &logStream, 1);
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

int recordList(string& recordsDir, vector<fileType>& records)
{
       struct dirent64 **eps;
       int n;
	//On some version ofglibc scandir and scandir64 have mem leaks.
	n = scandir64 (recordsDir.c_str(), &eps, one, alphasort64);
	if (n >= 0)
	{
		int cnt;
		for (cnt = 0; cnt < n; ++cnt)
		{
			fileType buff(eps[cnt]->d_name,eps[cnt]->d_type);
			records.push_back(buff);
		}
        }
	else
	{
		hlr_log("Couldn't open the directory",&logStream,2);
	}
	free(eps);
	return 0;
}

int processRecord (string& fileName, string command , bool dryRun)
{
	size_t pos = command.find("MESSAGEFILE");
	if ( pos != string::npos )
	{
		command.erase(pos,12);
		command.insert(pos,fileName);
	}
	string logBuff = "Command:" + command;
	hlr_log(logBuff, &logStream, 8);
	string message;
	FILE *output;
	output = popen (command.c_str(),"r");
	if ( !output )
	{
		return 1;
	}
	ssize_t bytes_read = 0;
	size_t nbytes = 1024;
	char *buffString;
	while ( bytes_read != -1 )
	{
		buffString = (char *) malloc (nbytes+1);
		bytes_read = getline (&buffString, &nbytes, output);
		if ( bytes_read != -1 )
		{
			message += buffString;
		}
		free(buffString);
	}
	//read
	if ( pclose(output) != 0 )
	{
		return 3;
	}
	if ( dryRun )
	{
		string logBuff = "Dry Run.";
		hlr_log(logBuff, &logStream, 8);
		return -123;
	}
	else
	{
		connInfo connectionInfo;
		string dummy;
		int res = 0;
        	attrType attributes;
        	node Node = parse(&message, "HLR");
		if ( Node.status !=0 )
        	{
			string logBuff = "Error parsing Message:" + message;
			hlr_log (logBuff, &logStream, 3);
                 	res = Node.status;
        	}
		else
		{
			string logBuff = "Message:" + message;
			hlr_log (logBuff, &logStream, 7);
			
			res = ATMResource::ATMResourceEngine ( message, connectionInfo, &dummy);
			logBuff = "Answer:" + dummy;
			hlr_log (logBuff, &logStream, 6);
			logBuff = "Exit status:" + int2string(res);
			hlr_log (logBuff, &logStream, 6);
		}
		return res;
	}
	return 0;
}

int unlink (string& fileName)
{
	return unlink(fileName.c_str());
	return 0;
}

int dgasHlrRecordConsumer (string& confFileName, string& recordsDir, string& commandBuff, bool dryRun, bool singleRun)
{
	int returncode = 0;
	string logFileName;
	string lockFileName;
	map <string,string> confMap;
        if ( dgas_conf_read ( confFileName, &confMap ) != 0 )
        {
                cerr << "WARNING: Error reading conf file: " << confFileName <<
endl;
                cerr << "There can be problems processing the transaction" << endl;
                return E_CONFIG;

        }
	if (logFileName == "")
        {
                logFileName = confMap["managerLogFileName"];
        }
	if ( bootstrapLog(logFileName, &logStream) != 0 )
        {
                cerr << "Error bootstrapping Log file " << endl;
                cerr << logFileName << endl;
                exit(1);
        }
	if ( lockFileName == "" )
        {
                if ( confMap["managerLockFileName"] != "" )
                {
                        lockFileName= confMap["managerLockFileName"];
                }
                else
                {
                        cerr << "WARNING: Error reading conf file: " << confFileName << endl;
                        return E_BROKER_URI;
                }
        }
        if ( putLock(lockFileName) != 0 )
        {
                hlr_log("hlr_qMgr: Startup failed, Error creating the lock file.", &logStream, 1);
                exit(1);
        }
	hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
        hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
        hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
        hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
        hlr_tmp_sql_dbname = (confMap["hlr_tmp_sql_dbname"]).c_str();
	if ( recordsDir == "" )
	{
		if ( confMap["recordsDir"] != "" )
		{
			recordsDir= confMap["recordsDir"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << confFileName << endl;
			return E_BROKER_URI;
		}
	}
	if ( commandBuff == "" )
	{
		if ( confMap["messageParsingCommand"] != "" )
		{
			commandBuff= confMap["messageParsingCommand"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << confFileName << endl;
			return E_BROKER_URI;
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
	signal (SIGTERM, exit_signal);
    	signal (SIGINT, exit_signal);
	int counter =0;
	time_t t0 = time(NULL);
	while ( goOn )
	{
		//connInfo connectionInfo;
		vector<fileType> records;
		returncode = recordList(recordsDir, records);
		vector<fileType>::iterator it = records.begin();
		while ( goOn )
		{
			for (int t=0; (t < 4) && ( it != records.end()) ; t++ )
			{
				if ( (*it).second == DT_REG)
				{ 
					string logBuff = "file:" + (*it).first;
					hlr_log ( logBuff, &logStream, 5);
					if ( ((*it).first).find("DGASAMQ") != string::npos )
					{
						string fileBuff = recordsDir + "/" + (*it).first;
						int res = processRecord(fileBuff,commandBuff,dryRun);
						if ( res == 0 ||
							res == 64 ||
							res == 65 ||
							res == 69 ||
							res == 70 ||
							res == 71 ||
							res == 73)
						{
							res = unlink(fileBuff);
						}
					}
				}
				if ( (*it).second == DT_DIR)
				{
					string logBuff = "dir:" + (*it).first;
					hlr_log(logBuff, &logStream, 5); 
				}
				it++;
				counter ++;
				if ( counter >= 100 )
				{
					time_t t1 = time(NULL);
					int et = t1-t0;
					if ( et >0 )
					{
						int rec_min = (counter*60)/et;
						string logBuff = "Rec/min = " + int2string(rec_min);
						hlr_log ( logBuff, &logStream, 5);
						counter =0;
						t0 = time(NULL);
					}
				}
			}	
		}
		if ( singleRun ) break;
		for (int i=0; (i< 10) && goOn; i++)
		{
			sleep(1);
		}
	}
	removeLock(lockFileName);
	return returncode;
}

