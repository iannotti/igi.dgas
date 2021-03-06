// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: AMQConsumer.cpp,v 1.1.2.1 2010/12/16 13:22:19 aguarise Exp $
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

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgas_lock.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/hlr-activemq-consumer/hlr-consumer/AMQConsumer.h"


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
	bool useTopic;
	bool clientAck;
	std::string brokerURI;
	std::string destURI;	

public:
	SimpleAsyncConsumer( const std::string& brokerURI,
		const std::string& destURI,
		bool useTopic = false,
		bool clientAck = false ) 
	{
		this->connection = NULL;
		this->session = NULL;
		this->destination = NULL;
		this->consumer = NULL;
		this->useTopic = useTopic;
		this->brokerURI = brokerURI;
		this->destURI = destURI;
		this->clientAck = clientAck;
	}
	
	virtual ~SimpleAsyncConsumer()
	{
		this->cleanup();	
	}

	void close() 
	{
		this->cleanup();
	}

	string dir;
	
	string fileName(string messageNumber)
	{
		//DGAS log
                time_t curtime;
                struct tm *timeLog;
                curtime = time(NULL);
                timeLog = localtime(&curtime);
                char timeBuff[16];
                strftime(timeBuff,sizeof(timeBuff),"%Y%m%d%H%M%S",timeLog);
               	string buffer = "DGASAMQ" + (string)timeBuff + "_" + messageNumber; 
		return buffer;
	}

	void runConsumer() 
	{
		try 
		{
			// Create a ConnectionFactory
			ActiveMQConnectionFactory* connectionFactory =
               			new ActiveMQConnectionFactory( brokerURI );
			// Create a Connection
			connection = connectionFactory->createConnection();
			delete connectionFactory;
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
				destination = session->createTopic( destURI );
			} 
			else 
			{
				destination = session->createQueue( destURI );
			}
			// Create a MessageConsumer from the Session to the Topic or Queue
			consumer = session->createConsumer( destination );
			consumer->setMessageListener( this );

		}
		catch (CMSException& e) 
		{
			e.printStackTrace();
		}
	}	
		
	// Called from the consumer since this class is a registered MessageListener.
	virtual void onMessage( const Message* message )
	{
		static int count = 0;
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
			if( clientAck ) 
			{
				message->acknowledge();
			}
			
			//printf( "Message #%d, dir:%s, Received: %s\n", count, dir.c_str(), text.c_str() );
			std::ofstream fileS;
			string fname = dir + "/"+ fileName(int2string(count));
			fileS.open( fname.c_str() , ios::app);
			if ( !fileS)
			{
				//FIXME error open file
			}
			else
			{
				fileS << text << endl;
				fileS.close();
			}
			string logBuff = "Message written in " + fname;
			hlr_log(logBuff, &logStream, 7);
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
		if ( confMap["logFileName"] != "" )
		{
			parms.logFileName= confMap["logFileName"];
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
		if ( confMap["lockFileName"] != "" )	
		{
			parms.lockFileName= confMap["lockFileName"];
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
	if ( parms.recordsDir == "" )
	{
		if ( confMap["recordsDir"] != "" )
		{
			parms.recordsDir= confMap["recordsDir"];
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

	activemq::library::ActiveMQCPP::initializeLibrary();
	std::string brokerURI =
        //"failover:(tcp://127.0.0.1:61616"
        "failover:(" + parms.amqBrokerUri +//tcp://127.0.0.1:61616"
//        "?wireFormat=openwire"
//        "&connection.useAsyncSend=true"
//        "&transport.commandTracingEnabled=true"
//        "&transport.tcpTracingEnabled=true"
//        "&wireFormat.tightEncodingEnabled=true"
        ")";
	
	std::string destURI = parms.dgasAMQTopic;
	//============================================================
    // set to true to use topics instead of queues
    // Note in the code above that this causes createTopic or
    // createQueue to be used in the consumer.
    //============================================================
    bool useTopics = false;

    //============================================================
    // set to true if you want the consumer to use client ack mode
    // instead of the default auto ack mode.
    //============================================================
    bool clientAck = false;

    // Create the consumer
    SimpleAsyncConsumer consumer( brokerURI, destURI, useTopics, clientAck );

    consumer.dir = parms.recordsDir;
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

