// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: asyncConsumer.cpp,v 1.1.2.2 2012/07/10 08:49:08 aguarise Exp $
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
#include "glite/dgas/dgas-consumers/consumers/asyncConsumer.h"
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



	void SimpleAsyncConsumer::run()
	{
		try
		{
			//if oututType == file create message directory if it doesn't exists/
			if (outputType == "file")
			{
				if (!checkDir())
					createDir();
			}

			// Create a ConnectionFactory
			auto_ptr < ConnectionFactory > connectionFactory(
					ConnectionFactory::createCMSConnectionFactory(brokerURI));
			// Create a Connection
			connection
					= connectionFactory->createConnection(username, password);
			if (clientId != "")
			{
				connection->setClientID(clientId);
			}
			connection->start();

			connection->setExceptionListener(this);
			// Create a Session
			if (clientAck)
			{
				session
						= connection->createSession(Session::CLIENT_ACKNOWLEDGE);
			}
			else
			{
				session = connection->createSession(Session::AUTO_ACKNOWLEDGE);
			}
			// Create the destination (Topic or Queue)
			if (useTopic)
			{

				if (!durable)
				{
					destination = session->createTopic(destURI);
					consumer = session->createConsumer(destination);
				}
				else
				{
					topic = session->createTopic(destURI);
					consumer = session->createDurableConsumer(topic, name,
							selector, noLocal);
				}

			}
			else
			{
				destination = session->createQueue(destURI);
				consumer = session->createConsumer(destination);
			}
			// Create a MessageConsumer from the Session to the Topic or Queue

			consumer->setMessageListener(this);

			std::cout.flush();
			std::cerr.flush();
			latch.countDown();//latch goes to 0 and waitUntilReady can return.
			//doneLatch.await( waitMillis );//to be used if the consumer shoud not survive more thana given amount of time.
			while (goOn && doneLatch.getCount() != 0)
				doneLatch.await(1000);//wait for the countdown latch to reach zero.

		} catch (CMSException& e)
		{
			latch.countDown();
			e.printStackTrace();
		}
	}

	// Called from the consumer since this class is a registered MessageListener.
	virtual void SimpleAsyncConsumer::onMessage(const Message* message)
	{
		static long int count = 0;
		try
		{
			count++;
			const TextMessage* textMessage =
					dynamic_cast<const TextMessage*> (message);
			string text = "";
			if (textMessage != NULL)
			{
				text = textMessage->getText();
			}
			else
			{
				text = "NOT A TEXTMESSAGE!";
			}
			useMessage(text);
		} catch (CMSException& e)
		{
			e.printStackTrace();
		}
		doneLatch.countDown();
	}

	// If something bad happens you see it here as this class is also been
	// registered as an ExceptionListener with the connection.
	virtual void SimpleAsyncConsumer::onException( const CMSException& ex AMQCPP_UNUSED )
	{
		hlr_log("CMS Exception occurred.  Shutting down client.", &logStream, 1);
		exit(1);
	}

	virtual void SimpleAsyncConsumer::transportInterrupted()
	{
		hlr_log("The Connection's Transport has been Interrupted.", &logStream,
				3);
	}

	virtual void SimpleAsyncConsumer::transportResumed()
	{
		hlr_log("The Connection's Transport has been Restored.", &logStream, 3);
	}



	void SimpleAsyncConsumer::cleanup()
	{
		//*************************************************
		// Always close destination, consumers and producers before
		// you destroy their sessions and connection.
		//*************************************************

		// Destroy resources.
		try
		{
			if (destination != NULL)
				delete destination;
		} catch (CMSException& e)
		{
		}
		destination = NULL;

		try
		{
			if (consumer != NULL)
				delete consumer;
		} catch (CMSException& e)
		{
		}
		consumer = NULL;

		// Close open resources.
		try
		{
			if (session != NULL)
				session->close();
			if (connection != NULL)
				connection->close();
		} catch (CMSException& e)
		{
		}

		// Now Destroy them
		try
		{
			if (session != NULL)
				delete session;
		} catch (CMSException& e)
		{
		}
		session = NULL;

		try
		{
			if (connection != NULL)
				delete connection;
		} catch (CMSException& e)
		{
		}
		connection = NULL;
	}




