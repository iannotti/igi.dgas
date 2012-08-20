// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: asyncConsumer.h,v 1.1.2.1.2.3 2012/08/20 13:36:13 aguarise Exp $
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
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>
#include <csignal>

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

using namespace activemq;
using namespace activemq::core;
using namespace activemq::transport;
using namespace decaf::lang;
using namespace decaf::util;
using namespace decaf::util::concurrent;
using namespace cms;
using namespace std;

//this must be used by the caller to set a flag trapping a SIGINT signal on exit.
extern volatile sig_atomic_t goOn;

class SimpleAsyncConsumer: public ExceptionListener,
		public MessageListener,
		public Runnable,
		public DefaultTransportListener
{
private:
	CountDownLatch latch;
	Connection* connection;
	Session* session;
	Destination* destination;
	MessageConsumer* consumer;
	long waitMillis;

protected:

	Topic* topic;
	CountDownLatch* doneLatch;
	long int numMessages;
	bool useTopic;
	bool clientAck;
	std::string brokerURI;
	std::string topicName;
	std::string username;
	std::string password;
	std::string clientId;
	std::string name;
	std::string selector;
	bool noLocal;
	bool durable;

	//for readConf

	std::string logFileName;


public:
	SimpleAsyncConsumer() :
		latch(1)
	{
		std::cout << "SimpleAsyncConsumer()" << std::endl;
		//doneLatch = new CountDownLatch(-1);
	}
	;

	SimpleAsyncConsumer(const std::string& brokerURI,
			const std::string& destURI, bool useTopic = false,
			bool clientAck = false, std::string name = "",
			std::string selector = "", bool nolocal = false,
			bool durable = false, std::string username = "",
			std::string password = "", std::string clientId = "",
			long int numMessages = 1) :
		latch(1)
	{
		std::cout << "SimpleAsyncConsumer(with parms)" << std::endl;
		doneLatch = new CountDownLatch(numMessages);
		this->connection = NULL;
		this->session = NULL;
		this->destination = NULL;
		this->consumer = NULL;
		this->useTopic = useTopic;
		this->brokerURI = brokerURI;
		this->topicName = destURI;
		this->clientAck = clientAck;
		this->username = username;
		this->password = password;
		this->clientId = clientId;
		this->name = name;
		this->selector = selector;
		this->noLocal = noLocal;
		this->durable = durable;
		this->numMessages = numMessages;
	}

	virtual ~SimpleAsyncConsumer() throw()
	{
		std::cout << "~SimpleAsyncConsumer()" << std::endl;
		//delete doneLatch;
		this->cleanup();
	}

	void close()
	{
		std::cout << "close()" << std::endl;
		this->cleanup();
	}

	void waitUntilReady()
	{
		latch.await();
	}

	virtual int readConf(string& confFile);

	//this is the method executed as a thread by the caller.
	void run();

	// Called from the running thread when receiving a message since this class is a registered MessageListener.
	virtual void onMessage(const Message* message) throw();

	// If something bad happens you see it here as this class is also been
	// registered as an ExceptionListener with the connection.
	virtual void onException( const CMSException& ex AMQCPP_UNUSED );

	virtual void transportInterrupted();

	virtual void transportResumed();

	//this method is called within the onMessage method and is used to do something with the message received.
	//should be overridden by the caller class.
	virtual void useMessage(std::string messageString) = 0;
	//{
	//	std::cout << messageString << std::endl;
	//}

private:
	void cleanup();

};

