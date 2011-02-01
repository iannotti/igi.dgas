// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: amqProducer.cpp,v 1.1.2.5 2011/02/01 14:38:00 aguarise Exp $
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
#include <signal.h>

#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <map>

#include <decaf/lang/Thread.h>
#include <decaf/lang/Runnable.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <decaf/lang/Long.h>
#include <decaf/util/Date.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/util/Config.h>
#include <activemq/library/ActiveMQCPP.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/BytesMessage.h>
#include <cms/MapMessage.h>
#include <cms/ExceptionListener.h>
#include <cms/MessageListener.h>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/dgas-producers/producers/amqProducer.h"

#define E_CONFIG 10
#define E_BROKER_URI 11


using namespace activemq;
using namespace activemq::core;
using namespace decaf;
using namespace decaf::lang;
using namespace decaf::util;
using namespace decaf::util::concurrent;
using namespace cms;
using namespace std;

class SimpleProducer : public Runnable {
private:
    
    Connection* connection;
    Session* session;
    Destination* destination;
    MessageProducer* producer;
    bool useTopic;
    bool clientAck;
    unsigned int numMessages;
    std::string brokerURI;
    std::string destURI;

public:

     int returnCode;
     
     SimpleProducer( const std::string& brokerURI,
                    unsigned int numMessages,
                    const std::string& destURI,
                    bool useTopic = false,
                    bool clientAck = false ){

        this->connection = NULL;
        this->session = NULL;
        this->destination = NULL;
        this->producer = NULL;
        this->numMessages = numMessages;
        this->useTopic = useTopic;
        this->brokerURI = brokerURI;
        this->destURI = destURI;
        this->clientAck = clientAck;
	this->returnCode =0;
    }

    
    virtual ~SimpleProducer(){
        cleanup();
    }

    void close() {
        this->cleanup();
    }

    virtual void run(vector<string>& textV) {
        try {

            // Create a ConnectionFactory
            auto_ptr<ActiveMQConnectionFactory> connectionFactory(
                new ActiveMQConnectionFactory( brokerURI ) );

            // Create a Connection
            try{
                connection = connectionFactory->createConnection();
                connection->start();
            } catch( CMSException& e ) {
                e.printStackTrace();
                throw e;
            }

            // Create a Session
            if( clientAck ) {
                session = connection->createSession( Session::CLIENT_ACKNOWLEDGE );
            } else {
                session = connection->createSession( Session::AUTO_ACKNOWLEDGE );
            }

            // Create the destination (Topic or Queue)
            if( useTopic ) {
                destination = session->createTopic( destURI );
            } else {
                destination = session->createQueue( destURI );
            }

            // Create a MessageProducer from the Session to the Topic or Queue
            producer = session->createProducer( destination );
            producer->setDeliveryMode( DeliveryMode::NON_PERSISTENT );

            // Create the Thread Id String
            string threadIdStr = Long::toString( Thread::getId() );

            // Create a messages
            vector<string>::iterator it = textV.begin();

                unsigned int ix = 0;
            while ( it != textV.end() ){
                TextMessage* message = session->createTextMessage( *it );

                message->setIntProperty( "Integer", ix );

                // Tell the producer to send the message
                printf( "Sent message #%d from thread %s\n", ix+1, threadIdStr.c_str() );
                producer->send( message );

                delete message;
                it++;
                ix++;
            }

        }catch ( CMSException& e ) {
            e.printStackTrace();
        }
    }

    virtual void run(string& text) {
        try {

            // Create a ConnectionFactory
            auto_ptr<ActiveMQConnectionFactory> connectionFactory(
                new ActiveMQConnectionFactory( brokerURI ) );

            // Create a Connection
            try{
                connection = connectionFactory->createConnection();
                connection->start();
            } catch( CMSException& e ) {
                e.printStackTrace();
		returnCode = 13;
                throw e;
            }

            // Create a Session
            if( clientAck ) {
                session = connection->createSession( Session::CLIENT_ACKNOWLEDGE );
            } else {
                session = connection->createSession( Session::AUTO_ACKNOWLEDGE );
            }

            // Create the destination (Topic or Queue)
            if( useTopic ) {
                destination = session->createTopic( destURI );
            } else {
                destination = session->createQueue( destURI );
            }

            // Create a MessageProducer from the Session to the Topic or Queue
            producer = session->createProducer( destination );
            //producer->setDeliveryMode( DeliveryMode::NON_PERSISTENT );
            producer->setDeliveryMode( DeliveryMode::PERSISTENT );

            // Create the Thread Id String
            string threadIdStr = Long::toString( Thread::getId() );

            // Create a messages

            for( unsigned int ix=0; ix<numMessages; ++ix ){
                TextMessage* message = session->createTextMessage( text );

                message->setIntProperty( "Integer", ix );

                // Tell the producer to send the message
                printf( "Sent message #%d from thread %s\n", ix+1, threadIdStr.c_str() );
                producer->send( message );

                delete message;
            }

        }catch ( CMSException& e ) {
	    if ( returnCode == 0 ) returnCode = 13;
            e.printStackTrace();
        }
    }

    virtual void run() {
        try {

            // Create a ConnectionFactory
            auto_ptr<ActiveMQConnectionFactory> connectionFactory(
                new ActiveMQConnectionFactory( brokerURI ) );

            // Create a Connection
            try{
                connection = connectionFactory->createConnection();
                connection->start();
            } catch( CMSException& e ) {
                e.printStackTrace();
                throw e;
            }

            // Create a Session
            if( clientAck ) {
                session = connection->createSession( Session::CLIENT_ACKNOWLEDGE );
            } else {
                session = connection->createSession( Session::AUTO_ACKNOWLEDGE );
            }

            // Create the destination (Topic or Queue)
            if( useTopic ) {
                destination = session->createTopic( destURI );
            } else {
                destination = session->createQueue( destURI );
            }

            // Create a MessageProducer from the Session to the Topic or Queue
            producer = session->createProducer( destination );
            producer->setDeliveryMode( DeliveryMode::NON_PERSISTENT );

            // Create the Thread Id String
            string threadIdStr = Long::toString( Thread::getId() );

            // Create a messages
            string text = (string)"Hello world! from thread " + threadIdStr;

            for( unsigned int ix=0; ix<numMessages; ++ix ){
                TextMessage* message = session->createTextMessage( text );

                message->setIntProperty( "Integer", ix );

                // Tell the producer to send the message
                printf( "Sent message #%d from thread %s\n", ix+1, threadIdStr.c_str() );
                producer->send( message );

                delete message;
            }

        }catch ( CMSException& e ) {
            e.printStackTrace();
        }
    }

private:

    void cleanup(){

        // Destroy resources.
        try{
            if( destination != NULL ) delete destination;
        }catch ( CMSException& e ) 
	{ 
		e.printStackTrace(); 
		returnCode = 1; 
	}
        destination = NULL;

        try{
            if( producer != NULL ) delete producer;
        }catch ( CMSException& e ) 
	{ 
		e.printStackTrace(); 
		returnCode = 2;
	}
        producer = NULL;

        // Close open resources.
        try{
            if( session != NULL ) session->close();
            if( connection != NULL ) connection->close();
        }catch ( CMSException& e ) 
	{ 
		e.printStackTrace(); 
		returnCode = 3;
	}

        try{
            if( session != NULL ) delete session;
        }catch ( CMSException& e ) 
	{ 
		e.printStackTrace(); 
		returnCode = 4;
	}
        session = NULL;

        try{
            if( connection != NULL ) delete connection;
        }catch ( CMSException& e ) 
	{ 
		e.printStackTrace(); 
		returnCode = 5;
	}
        connection = NULL;
    }  

};

int dgasHlrRecordProducer (string& confFileName, string amqBrokerUri, string amqTopic, string amqOptions)
{
	int returncode = 0;
	string output_message;
	map <string,string> confMap;
	if ( dgas_conf_read ( confFileName, &confMap ) != 0 )	
	{
		cerr << "WARNING: Could not read conf file: " << confFileName << 
endl;
		cerr << "There can be problems processing the transaction" << endl;
		if ( ( amqBrokerUri == "" ) || ( amqTopic == "" ) )
		{
			cerr << "Please specify amqBrokerUri and amqTopic." << endl;
			return E_CONFIG;
		}
		
	}

	if ( amqOptions == "" )
	{
		if ( confMap["amqOptions"] != "" )
		{
			amqOptions = confMap["amqOptions"];
		}
		else
		{
			amqOptions = "wireFormat=openwire"
				"&connection.useAsyncSend=true"
				"&transport.commandTracingEnabled=true"
				//"&transport.tcpTracingEnabled=true"
				"&wireFormat.tightEncodingEnabled=true";
		}
	}
	if ( amqBrokerUri == "" )
	{
		if ( confMap["amqBrokerUri"] != "" )
		{
			amqBrokerUri= confMap["amqBrokerUri"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << confFileName << endl;
			return E_BROKER_URI;
		}
	}
	if ( amqTopic == "" )
	{
		if ( confMap["dgasAMQTopic"] != "" )
		{
			amqTopic= confMap["dgasAMQTopic"];
		}
		else
		{
		 	cerr << "WARNING: Error reading conf file: " << confFileName << endl;
			return E_BROKER_URI;
		}
	}

	activemq::library::ActiveMQCPP::initializeLibrary();
	string textLine;
	while ( getline (cin, textLine, '\n'))
	{
		output_message += textLine += "\n";
	}	
	std::string brokerURI =
		""+ amqBrokerUri +
		//"failover:(tcp://hlr-test-29.to.infn.it:61616"
		"?" + amqOptions 
		//"wireFormat=openwire"
		//"&connection.useAsyncSend=true"
		//"&transport.commandTracingEnabled=true"
		//"&transport.tcpTracingEnabled=true"
		//"&wireFormat.tightEncodingEnabled=true"
		+ "";
	std::string destURI = amqTopic;
	bool useTopics = false;
	unsigned int numMessages = 1;
	
	SimpleProducer producer( brokerURI, numMessages, destURI, useTopics );
	producer.run(output_message);
	producer.close();
	returncode = producer.returnCode; 
	activemq::library::ActiveMQCPP::shutdownLibrary();
	return returncode;
	
}

