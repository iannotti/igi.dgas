// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: asyncConsumer.cpp,v 1.1.2.1.2.4 2012/08/21 08:03:18 aguarise Exp $
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

/*
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
*/
#include "glite/dgas/common/base/dgas_config.h"
#include "dgas/msg-common/asyncConsumer.h"


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

int SimpleAsyncConsumer::readConf(std::string& configFile)
{
	int returncode = 0;
	map < string, string > confMap;
	if (dgas_conf_read(configFile, &confMap) != 0)
	{
		cerr << "WARNING: Error reading conf file: " << configFile << endl;
		cerr << "There can be problems processing the transaction" << endl;
		return E_CONFIG;

	}
	if (brokerURI == "")
	{
		if (confMap["amqBrokerUri"] != "")
		{
			brokerURI = confMap["amqBrokerUri"];
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << configFile << endl;
			return E_BROKER_URI;
		}
	}
	if (topicName == "")
	{
		if (confMap["amqTopic"] != "")
		{
			topicName = confMap["amqTopic"];
		}
		else
		{
			cerr << "WARNING: Error reading conf file: " << configFile << endl;
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
		useTopic = true;
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
	if (username == "")
	{
		if (confMap["amqUsername"] != "")
		{
			username = confMap["amqUsername"];
		}
	}
	if (password == "")
	{
		if (confMap["amqPassword"] != "")
		{
			password = confMap["amqPassword"];
		}
	}
	if (clientId == "")
	{
		if (confMap["amqClientId"] != "")
		{
			clientId = confMap["amqClientId"];
		}
	}
	return 0;
}

void SimpleAsyncConsumer::run()
{
	try
	{
		std::cout << "SimpleAsyncConsumer::run()" << std::endl;
		// Create a ConnectionFactory
		auto_ptr<ConnectionFactory> connectionFactory(
				ConnectionFactory::createCMSConnectionFactory( brokerURI));
		// Create a Connection
		connection = connectionFactory->createConnection(username, password);
		if (clientId != "")
		{
			connection->setClientID(clientId);
		}
		connection->start();

		connection->setExceptionListener(this);
		// Create a Session
		if (clientAck)
		{
			std::cout << "SimpleAsyncConsumer::run():CLIENT_ACKNOWLEDGE"
					<< std::endl;
			session = connection->createSession(Session::CLIENT_ACKNOWLEDGE);
		}
		else
		{
			std::cout << "SimpleAsyncConsumer::run():AUTO_ACKNOWLEDGE"
					<< std::endl;
			session = connection->createSession(Session::AUTO_ACKNOWLEDGE);
		}
		// Create the destination (Topic or Queue)
		if (useTopic)
		{

			if (!durable)
			{
				destination = session->createTopic(topicName);
				consumer = session->createConsumer(destination);
			}
			else
			{
				topic = session->createTopic(topicName);
				consumer = session->createDurableConsumer(topic, name,
						selector, noLocal);
			}

		}
		else
		{
			destination = session->createQueue(topicName);
			consumer = session->createConsumer(destination);
		}
		// Create a MessageConsumer from the Session to the Topic or Queue

		consumer->setMessageListener(this);

		std::cout.flush();
		std::cerr.flush();
		latch.countDown();//latch goes to 0 and waitUntilReady can return.
		while (goOn && doneLatch->getCount() != 0)
			doneLatch->await(1000);//wait for the countdown latch to reach zero.


	} catch (CMSException& e)
	{
		latch.countDown();
		e.printStackTrace();
	}
}

// Called from the consumer since this class is a registered MessageListener.
void SimpleAsyncConsumer::onMessage(const Message* message) throw()
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
		if (clientAck)
		{
			textMessage->acknowledge();
		}

	} catch (CMSException& e)
	{
		e.printStackTrace();
	}
	doneLatch->countDown();
}

// If something bad happens you see it here as this class is also been
// registered as an ExceptionListener with the connection.
void SimpleAsyncConsumer::onException( const CMSException& ex AMQCPP_UNUSED )
{
	//hlr_log("CMS Exception occurred.  Shutting down client.", &logStream, 1);
	exit(1);
}

void SimpleAsyncConsumer::transportInterrupted()
{
	//hlr_log("The Connection's Transport has been Interrupted.", &logStream,
	//		3);
}

void SimpleAsyncConsumer::transportResumed()
{
	//hlr_log("The Connection's Transport has been Restored.", &logStream, 3);
}

void SimpleAsyncConsumer::cleanup()
{
	//*************************************************
	// Always close destination, consumers and producers before
	// you destroy their sessions and connection.
	//*************************************************
	std::cout << "SimpleAsyncConsumer::cleanup()" << std::endl;
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

