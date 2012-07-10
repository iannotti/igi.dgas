// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: AMQConsumer.cpp,v 1.1.2.68 2012/07/10 12:11:54 aguarise Exp $
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

#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/dgas-consumers/consumers/AMQConsumer.h"
#include "../../src-hlr-service/base/serviceVersion.h"


#define E_BROKER_URI 11
#define E_CONFIG 10

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




void exit_signal(int sig)
{
	goOn = 0;
	signal(sig, exit_signal);
}


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
	std::cerr << "A" << std::endl;
	SimpleAsyncConsumer consumer(amqBrokerUri, amqTopic, useTopics, clientAck,
			name, selector, noLocal, durable, amqUsername, amqPassword,
			amqClientId, messageNumber);
	// Start it up and it will listen forever.
	std::cerr << "B" << std::endl;
	Thread consumerThread(&consumer);
	std::cerr << "C" << std::endl;
	consumerThread.start();
	std::cerr << "D" << std::endl;
	consumer.waitUntilReady();
	std::cerr << "E" << std::endl;

	signal(SIGTERM, exit_signal);
	signal(SIGINT, exit_signal);
	std::cerr << "F" << std::endl;
	// Wait for consumerThread to exit.
	consumerThread.join();
	return;
}


