// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: amqConsumer.cpp,v 1.1.2.2 2012/07/20 14:01:59 aguarise Exp $
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

#include "dgas/msg-common/amqConsumer.h"


#define E_BROKER_URI 11
#define E_CONFIG 10


void exit_signal(int sig)
{
	goOn = 0;
	signal(sig, exit_signal);
}


//void AMQConsumer::registerConsumer(AMQConsumerStdOut* consumer)
void amqConsumer::registerConsumer(SimpleAsyncConsumer* consumer)
{
	std::cout << "AMQConsumer::registerConsumer" << std::endl;
	activemq::library::ActiveMQCPP::initializeLibrary();
	//run the registered consumer in a thread
	Thread consumerThread(consumer);
	consumerThread.start();
	//wait that the thread runner initialize itself correctly
	consumer->waitUntilReady();
	//register signal handlers
	signal(SIGTERM, exit_signal);
	signal(SIGINT, exit_signal);
	// Wait for consumerThread to exit.
	consumerThread.join();
	//activemq::library::ActiveMQCPP::shutdownLibrary();
	return;
}


