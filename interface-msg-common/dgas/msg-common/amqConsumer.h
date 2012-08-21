// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: amqConsumer.h,v 1.1.2.5 2012/08/21 08:03:17 aguarise Exp $
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

#include "dgas/msg-common/asyncConsumer.h"

#define AMQ_CONSUMER_VERSION "protoB"


using namespace std;

//this sets a flag trapping a SIGINT signal on exit.
volatile sig_atomic_t goOn = 1;


/*This is an example of a class overriding the useMessage method of the asyncConsumer parent class.
 *
class AMQConsumerStdOut: public SimpleAsyncConsumer {

public:

	AMQConsumerStdOut(const std::string& brokerURI,
				const std::string& destURI, bool useTopic = false,
				bool clientAck = false, std::string name = "",
				std::string selector = "", bool nolocal = false,
				bool durable = false,
				std::string username = "", std::string password = "",
				std::string clientId = "", long int numMessages = 1)
	{
				doneLatch = new CountDownLatch(numMessages);
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
				std::cerr << "AMQConsumerStdOut(params)" << std::endl;
	}
	//overrides AsyncConsumer useMessage() method. Can be overridden by parent classes if any.
	void useMessage(std::string messageString)
	{
		std::cout << "Remaining messages:"<< doneLatch->getCount() << std::endl;
		std::cout << "Message:"<< messageString << std::endl;
	}

};

As an example of code using this class:

//Instantiate an object of the AMQConsumer class;
AMQConsumer consumer;

//Instatinate a pointer to an object of the derived AMQConsumerStdOut class (declared as in the above example with
//at least an useMessage method). See the definition of AMQConsumer for the meaning of the constructor params.
AMQConsumerStdOut* consumerOutImpl = new AMQConsumerStdOut(
				parms.amqBrokerUri, parms.amqTopic, parms.useTopics,
				parms.clientAck, parms.name, parms.selector, parms.noLocal,
				parms.durable, parms.amqUsername, parms.amqPassword,
				parms.amqClientId, numMessages);

//call method registerConsumer. This will initiate the needed libraries and run an AMQ message listener thread.
//upon receiving a message the AMQConsumerStdOut::onMessage(std::string ) method will be executed.

consumer.registerConsumer(consumerOutImpl);

//free resources.
delete consumerOutImpl;

*/


class amqConsumer {

public:
	amqConsumer(){};
	//void registerConsumer(AMQConsumerStdOut* consumer);
	void registerConsumer(SimpleAsyncConsumer* consumer);

};



