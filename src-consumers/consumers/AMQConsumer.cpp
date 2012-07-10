// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: AMQConsumer.cpp,v 1.1.2.61 2012/07/10 09:12:11 aguarise Exp $
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

#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <map>
#include <sys/stat.h>

#include <decaf/lang/Thread.h>
#include <decaf/lang/Runnable.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/core/ActiveMQConnection.h>
#include <activemq/transport/DefaultTransportListener.h>
#include <activemq/library/ActiveMQCPP.h>
#include <decaf/lang/Integer.h>
#include <activemq/util/Config.h>
#include <decaf/util/Date.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/BytesMessage.h>
#include <cms/MapMessage.h>
#include <cms/ExceptionListener.h>
#include <cms/MessageListener.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgas_lock.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/dgas-consumers/consumers/AMQConsumer.h"
#include "../../src-hlr-service/base/serviceVersion.h"

#define E_CONFIG 10
#define E_BROKER_URI 11

/*
using namespace activemq;
using namespace activemq::core;
using namespace activemq::transport;
using namespace decaf::lang;
using namespace decaf::util;
using namespace decaf::util::concurrent;
using namespace cms;
using namespace std;
*/

ofstream logStream;
/*
const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_tmp_sql_dbname;
const char * hlr_sql_dbname;
*/

/*
bool is_number(const std::string& s)
{
	std::string::const_iterator it = s.begin();
	while (it != s.end() && std::isdigit(*it))
		++it;
	return !s.empty() && it == s.end();
}
*/
/*
void exit_signal(int sig)
{
	goOn = 0;
	signal(sig, exit_signal);
	cerr << "Got sigint" << endl;
}
*/

/*
int putLock(string lockFile)
{
	dgasLock Lock(lockFile);
	if (Lock.exists())
	{
		return 1;
	}
	else
	{
		if (Lock.put() != 0)
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
	if (Lock.exists())
	{
		if (Lock.remove() != 0)
		{
			//hlr_log("Error removing the lock file", &logStream, 1);
			return 2;
		}
		else
		{
			//hlr_log("lock file removed", &logStream, 4);
			return 0;
		}
	}
	else
	{
		//hlr_log("lock file doesn't exists", &logStream, 1);
		return 1;
	}
}
*/

int AMQConsumer::readConf(std::string& configFile)
{
	int returncode = 0;
	map < string, string > confMap;
	if (dgas_conf_read(configFile, &confMap) != 0)
	{
		cerr << "WARNING: Error reading conf file: " << configFile << endl;
		cerr << "There can be problems processing the transaction" << endl;
		return E_CONFIG;

	}
	if (logFileName == "")
	{
		if (confMap["consumerLogFileName"] != "")
		{
			logFileName = confMap["consumerLogFileName"];
			if (bootstrapLog(logFileName, &logStream) != 0)
			{
				cerr << "Error bootstrapping Log file " << endl;
				cerr << logFileName << endl;
				exit(1);
			}
		}
		else
		{
			//nolog file
		}

	}
	if (amqBrokerUri == "")
	{
		if (confMap["amqBrokerUri"] != "")
		{
			amqBrokerUri = confMap["amqBrokerUri"];
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << configFile
					<< endl;
			return E_BROKER_URI;
		}
	}
	if (amqTopic == "")
	{
		if (confMap["amqTopic"] != "")
		{
			amqTopic = confMap["amqTopic"];
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << configFile
					<< endl;
			return E_BROKER_URI;
		}
	}
	if ((confMap["durableSubscription"] == "yes")
			|| (confMap["durableSubscription"] == "true"))
	{
		durable = true;
	}
	if ((confMap["noLocal"] == "yes") || (confMap["noLocal"] == "true"))
	{
		noLocal = true;
	}
	if ((confMap["useTopics"] == "yes") || (confMap["useTopics"] == "true"))
	{
		useTopics = true;
	}
	if ((confMap["clientAck"] == "yes") || (confMap["clientAck"] == "true"))
	{
		clientAck = true;
	}
	if (name == "")
	{
		if (confMap["name"] != "")
		{
			name = confMap["name"];
		}
	}
	if (selector == "")
	{
		if (confMap["selector"] != "")
		{
			selector = confMap["selector"];
		}
	}
	if (amqUsername == "")
	{
		if (confMap["amqUsername"] != "")
		{
			amqUsername = confMap["amqUsername"];
		}
	}
	if (amqPassword == "")
	{
		if (confMap["amqPassword"] != "")
		{
			amqPassword = confMap["amqPassword"];
		}
	}
	if (amqClientId == "")
	{
		if (confMap["amqClientId"] != "")
		{
			amqClientId = confMap["amqClientId"];
		}
	}
	return 0;
}

void AMQConsumer::run()
{
	int returncode = 0;
	activemq::library::ActiveMQCPP::initializeLibrary();
	// Create the consumer
	SimpleAsyncConsumer consumer(amqBrokerUri, amqTopic, useTopics, clientAck,
			name, selector, noLocal, durable, amqUsername, amqPassword,
			amqClientId, messageNumber);
	// Start it up and it will listen forever.

	Thread consumerThread(&consumer);
	consumerThread.start();
	consumer.waitUntilReady();

	signal(SIGTERM, exit_signal);
	signal(SIGINT, exit_signal);
	// Wait for consumerThread to exit.
	consumerThread.join();
	return;
}

/*
int AMQRecordConsumer(recordConsumerParms& parms)
{
	int returncode = 0;
	map < string, string > confMap;
	if (dgas_conf_read(parms.configFile, &confMap) != 0)
	{
		cerr << "WARNING: Error reading conf file: " << parms.configFile
				<< endl;
		cerr << "There can be problems processing the transaction" << endl;
		return E_CONFIG;

	}
	if (parms.logFileName == "")
	{
		if (confMap["consumerLogFileName"] != "")
		{
			parms.logFileName = confMap["consumerLogFileName"];
			if (bootstrapLog(parms.logFileName, &logStream) != 0)
			{
				cerr << "Error bootstrapping Log file " << endl;
				cerr << parms.logFileName << endl;
				exit(1);
			}
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << parms.configFile
					<< endl;
			return E_BROKER_URI;
		}

	}
	if (parms.lockFileName == "")
	{
		if (confMap["consumerLockFileName"] != "")
		{
			parms.lockFileName = confMap["consumerLockFileName"];
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << parms.configFile
					<< endl;
			return E_BROKER_URI;
		}
	}
	if (parms.foreground != "true")
	{
		if (putLock(parms.lockFileName) != 0)
		{
			hlr_log("hlr_qMgr: Startup failed, Error creating the lock file.",
					&logStream, 1);
			exit(1);
		}
	}
	if (parms.hlrSqlTmpDBName == "")
	{
		if (confMap["hlr_tmp_sql_dbname"] != "")
		{
			parms.hlrSqlTmpDBName = confMap["hlr_tmp_sql_dbname"];
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << parms.configFile
					<< endl;
			return E_BROKER_URI;
		}
	}

	if (parms.hlrSqlDBName == "")
	{
		if (confMap["hlr_sql_dbname"] != "")
		{
			parms.hlrSqlDBName = confMap["hlr_sql_dbname"];
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << parms.configFile
					<< endl;
			return E_BROKER_URI;
		}
	}

	if (parms.hlrSqlServer == "")
	{
		if (confMap["hlr_sql_server"] != "")
		{
			parms.hlrSqlServer = confMap["hlr_sql_server"];
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << parms.configFile
					<< endl;
			return E_BROKER_URI;
		}
	}

	if (parms.hlrSqlUser == "")
	{
		if (confMap["hlr_sql_user"] != "")
		{
			parms.hlrSqlUser = confMap["hlr_sql_user"];
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << parms.configFile
					<< endl;
			return E_BROKER_URI;
		}
	}

	if (parms.hlrSqlPassword == "")
	{
		if (confMap["hlr_sql_password"] != "")
		{
			parms.hlrSqlPassword = confMap["hlr_sql_password"];
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << parms.configFile
					<< endl;
			return E_BROKER_URI;
		}
	}
	hlr_sql_server = (parms.hlrSqlServer).c_str();
	hlr_sql_user = (parms.hlrSqlUser).c_str();
	hlr_sql_password = (parms.hlrSqlPassword).c_str();
	hlr_tmp_sql_dbname = (parms.hlrSqlTmpDBName).c_str();
	hlr_sql_dbname = (parms.hlrSqlDBName).c_str();
	serviceVersion thisServiceVersion(hlr_sql_server, hlr_sql_user,
			hlr_sql_password, hlr_sql_dbname);
	if (!thisServiceVersion.tableExists())
	{
		thisServiceVersion.tableCreate();
	}
	thisServiceVersion.setService("dgas-AMQConsumer");
	thisServiceVersion.setVersion(VERSION);
	thisServiceVersion.setHost("localhost");
	thisServiceVersion.setConfFile(parms.configFile);
	thisServiceVersion.setLockFile(parms.lockFileName);
	thisServiceVersion.setLogFile(parms.logFileName);
	thisServiceVersion.write();
	thisServiceVersion.updateStartup();
	//check if Database  exists. Create it otherwise.
	db
			hlrDb(hlr_sql_server, hlr_sql_user, hlr_sql_password,
					hlr_tmp_sql_dbname);
	if (hlrDb.errNo != 0)
	{
		hlr_log("Error connecting to SQL database", &logStream, 2);
		exit(1);
	}
	string queryString = "DESCRIBE messages";
	dbResult queryResult = hlrDb.query(queryString);
	if (hlrDb.errNo != 0)
	{
		hlr_log("Table messages doesn't exists, creating it.", &logStream, 5);
		queryString = "CREATE TABLE messages";
		queryString += " (";
		queryString += " id bigint(20) unsigned auto_increment, ";
		queryString += " status int DEFAULT '0', ";
		queryString += " message blob, ";
		queryString += " primary key (id) , key(status))";
		hlrDb.query(queryString);
		if (hlrDb.errNo != 0)
		{
			hlr_log("Error creating table messages.", &logStream, 2);
			exit(1);
		}
	}

	//move to AMQConsumer.run() from here.


	std::string outputType = parms.outputType;
	long int numMessages = -1;
	if (parms.messageNumber != "" && is_number(parms.messageNumber))
	{
		numMessages = atol((parms.messageNumber).c_str());
	}

	//if (outputType == "file")
	//{
	//	consumer.setDir(parms.outputDir);
	//	std::string logBuff = "Messages will be written inside directory: "
	//			+ parms.outputDir;
	//	hlr_log(logBuff, &logStream, 6);
	//}
	AMQConsumer consumer();

	// All CMS resources should be closed before the library is shutdown.
	if (parms.foreground != "true")
		removeLock(parms.lockFileName);
	string logBuff = "Removing:" + parms.lockFileName;
	hlr_log(logBuff, &logStream, 1);
	return returncode;
}
*/

