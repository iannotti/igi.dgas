// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: AMQConsumer.cpp,v 1.1.2.30.2.1 2012/07/25 09:46:11 aguarise Exp $
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


using namespace activemq;
using namespace activemq::core;
using namespace activemq::transport;
using namespace decaf::lang;
using namespace decaf::util;
using namespace decaf::util::concurrent;
using namespace cms;
using namespace std;

ofstream logStream;
const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_tmp_sql_dbname;
const char * hlr_sql_dbname;

volatile sig_atomic_t goOn = 1;

class SimpleAsyncConsumer : public ExceptionListener,
			public MessageListener,
			public DefaultTransportListener 
{
private:
	Connection* connection;
	Session* session;
	Destination* destination;
	MessageConsumer* consumer;
	Topic* topic;
	bool useTopic;
	bool clientAck;
	std::string brokerURI;
	std::string destURI;	
	std::string username;
	std::string password;
	std::string clientId;
	std::string name;
	std::string selector;
	bool noLocal;
	bool durable;

public:
	SimpleAsyncConsumer( const std::string& brokerURI,
		const std::string& destURI,
		bool useTopic = false,
		bool clientAck = false,
		std::string name = "",
		std::string selector ="",
		bool nolocal = false,
		bool durable = false,
		std::string username = "",
		std::string password = "",
				std::string clientId = "")
	{
		this->connection = NULL;
		this->session = NULL;
		this->destination = NULL;
		this->consumer = NULL;
		this->useTopic = useTopic;
		this->brokerURI = brokerURI;
		this->destURI = destURI;
		this->clientAck = clientAck;
		this->username = username;
		this->password = password;
		this->clientId = clientId;
		this->name = name;
		this->selector = selector;
		this->noLocal = noLocal;
		this->durable = durable;
	}
	
	virtual ~SimpleAsyncConsumer() throw()
	{
		this->cleanup();	
	}

	void close() 
	{
		this->cleanup();
	}

	void runConsumer() 
	{
		try 
		{
			// Create a ConnectionFactory
			ActiveMQConnectionFactory* connectionFactory =
               			new ActiveMQConnectionFactory( brokerURI );
			// Create a Connection
				connection = connectionFactory->createConnection(username, password );
			delete connectionFactory;
			if ( clientId != "" )
			{
				connection->setClientID(clientId);
			}
			ActiveMQConnection* amqConnection = dynamic_cast<ActiveMQConnection*>( connection );
			if( amqConnection != NULL ) 
			{
				amqConnection->addTransportListener( this );
			}
			connection->start();

			connection->setExceptionListener(this);
			// Create a Session
			if( clientAck ) 
			{
				session = connection->createSession( Session::CLIENT_ACKNOWLEDGE );
			} 
			else 
			{
				session = connection->createSession( Session::AUTO_ACKNOWLEDGE );
			}
			// Create the destination (Topic or Queue)
			if( useTopic ) 
			{

				if ( !durable )
				{
						destination = session->createTopic( destURI );
						consumer = session->createConsumer( destination );
				}
				else
				{
						topic  = session->createTopic( destURI );
						consumer = session->createDurableConsumer(topic,name,selector,noLocal);
				}

			} 
			else 
			{
				destination = session->createQueue( destURI );
				consumer = session->createConsumer( destination );
			}
			// Create a MessageConsumer from the Session to the Topic or Queue

			consumer->setMessageListener( this );

		}
		catch (CMSException& e) 
		{
			e.printStackTrace();
		}
	}	
		
	// Called from the consumer since this class is a registered MessageListener.
	virtual void onMessage( const Message* message ) throw()
	{
		static long int count = 0;
		try
		{
			count++;
			const TextMessage* textMessage =
				dynamic_cast< const TextMessage* >( message );
			string text = "";
			if( textMessage != NULL ) 
			{
				text = textMessage->getText();
			} 
			else 
			{
				text = "NOT A TEXTMESSAGE!";
			}
			
			//FIXME WRITE MESSAGE in DATABASE
			db hlrDb ( hlr_sql_server,
				hlr_sql_user,
				hlr_sql_password,
				hlr_tmp_sql_dbname	
			);
			if ( hlrDb.errNo != 0 )
			{
				hlr_log("Error connecting to SQL database",&logStream,2);
				exit(1);
			}
			string messageQuery = "INSERT INTO messages SET ";
			messageQuery += "message=";
			messageQuery += "\'" + hlrDb.escape_string(text) + "\'";
			hlrDb.query(messageQuery);
			if ( hlrDb.errNo != 0 )
			{
				hlr_log("Error Inserting message",&logStream,1);
			}
			else
			{
				string logBuff = "Message written in DB.";
				hlr_log(logBuff, &logStream, 7);
				if( clientAck ) 
				{
					message->acknowledge();
				}
			}
		}
		catch (CMSException& e) 
		{
			e.printStackTrace();
		}
	}

	// If something bad happens you see it here as this class is also been
	// registered as an ExceptionListener with the connection.
	virtual void onException( const CMSException& ex AMQCPP_UNUSED ) 
	{
		hlr_log("CMS Exception occurred.  Shutting down client.", &logStream, 1);
		exit(1);
	}	
	
	virtual void transportInterrupted() 
	{
		hlr_log("The Connection's Transport has been Interrupted.", &logStream, 3);
	}
	
	virtual void transportResumed() 
	{
		hlr_log("The Connection's Transport has been Restored.", &logStream, 3);
	}

private:

	void cleanup()
	{
		//*************************************************
	        // Always close destination, consumers and producers before
	        // you destroy their sessions and connection.
	        //*************************************************
		
		// Destroy resources.
		try
		{
			if( destination != NULL ) delete destination;
		}
		catch (CMSException& e) {}
        	destination = NULL;

		try
		{
			if( consumer != NULL ) delete consumer;
		}
		catch (CMSException& e) {}
		consumer = NULL;

		// Close open resources.
		try
		{
			if( session != NULL ) session->close();
			if( connection != NULL ) connection->close();
		}
		catch (CMSException& e) {}

		// Now Destroy them
		try
		{
			if( session != NULL ) delete session;
		}
		catch (CMSException& e) {}
		session = NULL;

		try
		{
			if( connection != NULL ) delete connection;
		}
		catch (CMSException& e) {}
        	connection = NULL;
	}	
};

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

int AMQConsumer (consumerParms& parms)
{
	int returncode = 0;
	map <string,string> confMap;
        if ( dgas_conf_read ( parms.confFileName, &confMap ) != 0 )
        {
                cerr << "WARNING: Error reading conf file: " << parms.confFileName << endl;
                cerr << "There can be problems processing the transaction" << endl;
                return E_CONFIG;

        }
	if ( parms.logFileName == "" )
	{
		if ( confMap["consumerLogFileName"] != "" )
		{
			parms.logFileName= confMap["consumerLogFileName"];
	        	if ( bootstrapLog(parms.logFileName, &logStream) != 0 )
		        {
        		        cerr << "Error bootstrapping Log file " << endl;
                		cerr << parms.logFileName << endl;
	                	exit(1);
       		 	}
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << parms.confFileName << endl;
			return E_BROKER_URI;
		}
		
	}
	if ( parms.lockFileName == "" )
	{
		if ( confMap["consumerLockFileName"] != "" )	
		{
			parms.lockFileName= confMap["consumerLockFileName"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << parms.confFileName << endl;
			return E_BROKER_URI;
		}
	}
	if ( putLock(parms.lockFileName) != 0 )
	{
		hlr_log("hlr_qMgr: Startup failed, Error creating the lock file.", &logStream, 1);
		exit(1);
	}
	if ( parms.amqBrokerUri == "" )
	{
		if ( confMap["amqBrokerUri"] != "" )
		{
			parms.amqBrokerUri= confMap["amqBrokerUri"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << parms.confFileName << endl;
			return E_BROKER_URI;
		}
	}
	if ( parms.dgasAMQTopic == "" )
	{
		if ( confMap["dgasAMQTopic"] != "" )
		{
			parms.dgasAMQTopic= confMap["dgasAMQTopic"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << parms.confFileName << endl;
			return E_BROKER_URI;
		}
	}
	if ( parms.durable == "" )
			{
				if ( confMap["durable"] != "" )
				{
					parms.durable = confMap["durable"];
				}
			}
	if ( parms.noLocal == "" )
				{
					if ( confMap["noLocal"] != "" )
					{
						parms.noLocal = confMap["noLocal"];
					}
				}
	if ( parms.name == "" )
				{
					if ( confMap["name"] != "" )
					{
						parms.name = confMap["name"];
					}
				}
	if ( parms.selector == "" )
					{
						if ( confMap["selector"] != "" )
						{
							parms.selector = confMap["selector"];
						}
					}
	if ( parms.useTopics == "" )
		{
			if ( confMap["useTopics"] != "" )
			{
				parms.useTopics = confMap["useTopics"];
			}
		}
	if ( parms.clientAck == "" )
			{
				if ( confMap["clientAck"] != "" )
				{
					parms.clientAck = confMap["clientAck"];
				}
			}
	if ( parms.amqUsername == "" )
			{
				if ( confMap["amqUsername"] != "" )
				{
					parms.amqUsername = confMap["amqUsername"];
				}
			}
	if ( parms.amqPassword == "" )
			{
				if ( confMap["amqPassword"] != "" )
				{
					parms.amqPassword = confMap["amqPassword"];
				}
			}
	if ( parms.amqClientId == "" )
				{
					if ( confMap["amqClientId"] != "" )
					{
						parms.amqClientId = confMap["amqClientId"];
					}
				}
	if ( parms.hlrSqlTmpDBName == "" )
	{
		if ( confMap["hlr_tmp_sql_dbname"] != "" )
		{
			parms.hlrSqlTmpDBName = confMap["hlr_tmp_sql_dbname"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << parms.confFileName << endl;
			return E_BROKER_URI;
		}
	}
	
	if ( parms.hlrSqlDBName == "" )
		{
			if ( confMap["hlr_sql_dbname"] != "" )
			{
				parms.hlrSqlDBName = confMap["hlr_sql_dbname"];
			}
			else
			{
			 	cerr << "WARNING: Error reading conf file: " << parms.confFileName << endl;
				return E_BROKER_URI;
			}
		}

	if ( parms.hlrSqlServer == "" )
	{
		if ( confMap["hlr_sql_server"] != "" )
		{
			parms.hlrSqlServer = confMap["hlr_sql_server"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << parms.confFileName << endl;
			return E_BROKER_URI;
		}
	}

	if ( parms.hlrSqlUser == "" )
	{
		if ( confMap["hlr_sql_user"] != "" )
		{
			parms.hlrSqlUser = confMap["hlr_sql_user"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << parms.confFileName << endl;
			return E_BROKER_URI;
		}
	}

	if ( parms.hlrSqlPassword == "" )
	{
		if ( confMap["hlr_sql_password"] != "" )
		{
			parms.hlrSqlPassword = confMap["hlr_sql_password"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << parms.confFileName << endl;
			return E_BROKER_URI;
		}
	}
	hlr_sql_server = (parms.hlrSqlServer).c_str();
	hlr_sql_user = (parms.hlrSqlUser).c_str();
	hlr_sql_password = (parms.hlrSqlPassword).c_str();
	hlr_tmp_sql_dbname = (parms.hlrSqlTmpDBName).c_str();
	hlr_sql_dbname = (parms.hlrSqlDBName).c_str();
	serviceVersion thisServiceVersion(hlr_sql_server,
					hlr_sql_user,
					hlr_sql_password,
					hlr_sql_dbname);
			if ( !thisServiceVersion.tableExists() )
			{
				thisServiceVersion.tableCreate();
			}
			thisServiceVersion.setService("dgas-AMQConsumer");
			thisServiceVersion.setVersion(VERSION);
			thisServiceVersion.setHost("localhost");
			thisServiceVersion.setConfFile(parms.confFileName);
			thisServiceVersion.setLockFile(parms.lockFileName);
			thisServiceVersion.setLogFile(parms.logFileName);
			thisServiceVersion.write();
			thisServiceVersion.updateStartup();
	//check if Database  exists. Create it otherwise.
	db hlrDb ( hlr_sql_server,
		hlr_sql_user,
		hlr_sql_password,
		hlr_tmp_sql_dbname	
		);
	if ( hlrDb.errNo != 0 )
	{
		hlr_log("Error connecting to SQL database",&logStream,2);
		exit(1);
	}
	string queryString = "DESCRIBE messages";
	dbResult queryResult = hlrDb.query(queryString);
	if ( hlrDb.errNo != 0 )
	{
		hlr_log("Table messages doesn't exists, creating it.",&logStream,5);
		queryString = "CREATE TABLE messages";
		queryString += " (";
		queryString += " id bigint(20) unsigned auto_increment, ";
		queryString += " status int DEFAULT '0', ";
		queryString += " message blob, ";
		queryString += " primary key (id) , key(status))";
		hlrDb.query(queryString);
		if ( hlrDb.errNo != 0 )
		{
			hlr_log("Error creating table messages.",&logStream,2);
			exit(1);
		}
	}

	activemq::library::ActiveMQCPP::initializeLibrary();
	std::string brokerURI = parms.amqBrokerUri;
	
	std::string destURI = parms.dgasAMQTopic;
	//============================================================
    // set to true to use topics instead of queues
    // Note in the code above that this causes createTopic or
    // createQueue to be used in the consumer.
    //============================================================
    bool useTopics = false;
    if ( (parms.useTopics == "true" ) || ( parms.useTopics == "yes") )  useTopics = true;

    //============================================================
    // set to true if you want the consumer to use client ack mode
    // instead of the default auto ack mode.
    //============================================================
    bool clientAck = false;
    if ( (parms.clientAck == "true" ) || ( parms.clientAck == "yes") )  clientAck = true;

    bool noLocal = false;
        if ( (parms.noLocal == "true" ) || ( parms.noLocal == "yes") )  noLocal = true;

    bool durable = false;
        if ( (parms.durable == "true" ) || ( parms.durable == "yes") )  durable = true;

    std::string name = parms.name;
    std::string selector = parms.selector;
    std::string username = parms.amqUsername;
    std::string password = parms.amqPassword;
    std::string clientId = parms.amqClientId;

    // Create the consumer
    SimpleAsyncConsumer consumer( brokerURI,
    		destURI,
    		useTopics,
    		clientAck,
    		name,
    		selector,
    		noLocal,
    		durable,
    		username,
    		password,
    		clientId);

    // Start it up and it will listen forever.
    consumer.runConsumer();

    signal (SIGTERM, exit_signal);
    signal (SIGINT, exit_signal);
    // Wait to exit.
    while( goOn ) { sleep(1);};

    // All CMS resources should be closed before the library is shutdown.
    consumer.close();
    activemq::library::ActiveMQCPP::shutdownLibrary();
	removeLock(parms.lockFileName);
	string logBuff = "Removing:" + parms.lockFileName;
	hlr_log(logBuff, &logStream, 1);
	return returncode;
}

