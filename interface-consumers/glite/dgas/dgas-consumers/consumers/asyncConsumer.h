// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: asyncConsumer.h,v 1.1.2.3 2012/07/10 08:51:49 aguarise Exp $
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

class SimpleAsyncConsumer: public ExceptionListener,
		public MessageListener,
		public Runnable,
		public DefaultTransportListener
{
private:
	CountDownLatch latch;
	CountDownLatch doneLatch;
	Connection* connection;
	Session* session;
	Destination* destination;
	MessageConsumer* consumer;
	Topic* topic;
	long waitMillis;
	long int numMessages;
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

	SimpleAsyncConsumer(const std::string& brokerURI,
			const std::string& destURI, bool useTopic = false,
			bool clientAck = false, std::string name = "",
			std::string selector = "", bool nolocal = false,
			bool durable = false,
			std::string username = "", std::string password = "",
			std::string clientId = "", long int numMessages = 1) :
		latch(1), doneLatch(numMessages)
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
		this->numMessages = numMessages;
	}

	virtual ~SimpleAsyncConsumer()
	{
		this->cleanup();
	}

	void close()
	{
		this->cleanup();
	}

	void waitUntilReady()
	{
		latch.await();
	}

	//this is the method executed as a thread by the caller.
	void run();

	// Called from the running thread when receiving a message since this class is a registered MessageListener.
	virtual void onMessage(const Message* message);

	// If something bad happens you see it here as this class is also been
	// registered as an ExceptionListener with the connection.
	virtual void onException( const CMSException& ex AMQCPP_UNUSED );

	virtual void transportInterrupted();

	virtual void transportResumed();

	//this method is called within the onMessage method and is used to do something with the message received.
	//should be overridden by the caller class.
	virtual void useMessage(std::string messageString)
	{
		std::cout << messageString << std::endl;
	}

private:
	void cleanup();

};




