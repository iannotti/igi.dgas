// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: legacyRecordManager.cpp,v 1.1.2.12 2011/02/11 10:16:11 aguarise Exp $
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
#include <sys/wait.h>
#include <dirent.h>
#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <map>

#include "glite/dgas/hlr-service/base/db.h"
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

struct messageType {
	string message;
	string id;
	string status;
};

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

int getMessages( vector<messageType>& records, int numRecordsPerIter)
{
	string queryBuffer = "SELECT id,status,message FROM messages ORDER by status limit " + int2string(numRecordsPerIter);
	db hlrDb ( hlr_sql_server,
                        hlr_sql_user,
                        hlr_sql_password,
                        hlr_tmp_sql_dbname
                );
	if ( hlrDb.errNo == 0 )
	{
		dbResult result = hlrDb.query( queryBuffer );
		
		if ( hlrDb.errNo == 0 )
		{
			int numRows = result.numRows();
			if ( numRows > 0 )
			{
				for (int i = 0;i < numRows; i++ )
				{
					messageType messageBuff;
					messageBuff.id = result.getItem(i,0);
					messageBuff.status = result.getItem(i,1);
					messageBuff.message= result.getItem(i,2);
					records.push_back(messageBuff);
				}
			}
			return 0;
		}
		else
		{
			string logBuff = "Error in query: " + queryBuffer + ":" + int2string(hlrDb.errNo);
			hlr_log(logBuff, &logStream, 3);
			return hlrDb.errNo;
		}
	}
	else
	{
		string logBuff = "Error connecting to DB:" + int2string(hlrDb.errNo);
		hlr_log(logBuff, &logStream, 3);
		return hlrDb.errNo;
	}
}

int processRecord (string& inputMessage, string command , bool dryRun)
{
	command = "echo -n \'" + inputMessage + "\' |" + command;
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

int delRecord(string& id)
{
	db hlrDb ( hlr_sql_server,
                                hlr_sql_user,
                                hlr_sql_password,
                                hlr_tmp_sql_dbname
                        );
	if ( hlrDb.errNo == 0 )
	{ 
		string queryStr;
		queryStr = "DELETE FROM messages WHERE id=";
		queryStr += id;
		hlrDb.query(queryStr);
		if ( hlrDb.errNo != 0 )
		{
			string logBuff = "Error in query: " + queryStr + ":" + int2string(hlrDb.errNo);
			hlr_log(logBuff, &logStream, 3);
			return hlrDb.errNo;
		}
		else
		{
			string logBuff = "Deleted:" + id;
			hlr_log(logBuff, &logStream, 7);
			return 0;
		}
	}
	else
	{
		string logBuff = "Error connecting to DB:" + int2string(hlrDb.errNo);
		hlr_log(logBuff, &logStream, 3);
		return hlrDb.errNo;
	}
}

int lowerPriority(string& id)
{
	db hlrDb ( hlr_sql_server,
                                hlr_sql_user,
                                hlr_sql_password,
                                hlr_tmp_sql_dbname
                        );
	if ( hlrDb.errNo == 0 )
	{ 
		string queryStr;
		queryStr = "UPDATE messages SET status = status + 1 WHERE id=";
		queryStr += id;
		hlrDb.query(queryStr);
		if ( hlrDb.errNo != 0 )
		{
			string logBuff = "Error in query: " + queryStr + ":" + int2string(hlrDb.errNo);
			hlr_log(logBuff, &logStream, 3);
			return hlrDb.errNo;
		}
		else
		{
			string logBuff = "Updated:" + id;
			hlr_log(logBuff, &logStream, 7);
			return 0;
		}
	}
	else
	{
		string logBuff = "Error connecting to DB:" + int2string(hlrDb.errNo);
		hlr_log(logBuff, &logStream, 3);
		return hlrDb.errNo;
	}
}

int dgasHlrRecordConsumer (string& confFileName, confParameters& parms)
{
	int returncode = 0;
	int threadNumber = 4;
	int waitFor = 1;
	int numRecordsPerIter = 1000;
	string logFileName;
	string lockFileName;
	map <string,string> confMap;
        if ( dgas_conf_read ( confFileName, &confMap ) != 0 )
        {
                cerr << "WARNING: Error reading conf file: " << confFileName << endl;
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
	if ( parms.messageParser == "" )
	{
		if ( confMap["messageParsingCommand"] != "" )
		{
			parms.messageParser = confMap["messageParsingCommand"];
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
	if ( confMap["thread_number"] != "" )
	{
		threadNumber = atoi((confMap["thread_number"]).c_str());
	}
	
	if ( confMap["managerTimeWait"] != "" )
	{
		waitFor = atoi((confMap["managerTimeWait"]).c_str());
	}
	if ( confMap["managerRecordPerInterval"] != "" )
	{
		numRecordsPerIter = atoi((confMap["managerRecordPerInterval"]).c_str());
	}
	signal (SIGTERM, exit_signal);
    	signal (SIGINT, exit_signal);
	int counter =0;
	time_t t0 = time(NULL);
	while ( goOn )
	{
		//connInfo connectionInfo;
		vector<messageType> messages;
		returncode = getMessages(messages, numRecordsPerIter);
		vector<messageType>::iterator it = messages.begin();
		while ( goOn && ( it != messages.end()) )
		{
			vector<pid_t> childPids;
			for (int t=0; (t < threadNumber ) && ( it != messages.end()) ; t++ )
			{
				pid_t pid;
				pid = fork();
				if ( pid < 0  )
				{
					//out of resources
				}
				else if ( pid == 0 )
				{
					//child
					string logBuff = "message:" + (*it).message;
					hlr_log ( logBuff, &logStream, 9);
					int res = processRecord((*it).message,parms.messageParser,parms.dryRun);
					if ( res == 0 ||
						res == 64 ||
						res == 65 ||
						res == 69 ||
						res == 70 ||
						res == 71 ||
						res == 73)
					{
						//remove record
						res = delRecord((*it).id);
					}
					else
					{
						res = lowerPriority((*it).id);
						//record status++;
					}
					_exit(0);
				}
				else
				{
					//parent
					childPids.push_back(pid);
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
			}//for
			vector<pid_t>::iterator pid_it = childPids.begin();
			while ( pid_it != childPids.end() )	
			{
				int status;
				if  ( waitpid (*pid_it, &status, 0) != *pid_it )
				{
					status = -1;
				}
				pid_it++;
			}
		}//while (goOn)
		for (int i=0; (i <= waitFor ) && goOn; i++)
		{
			sleep(1);
		}
		if ( parms.singleRun ) break;
	}
	removeLock(lockFileName);
	return returncode;
}

