// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: AMQConsumer.h,v 1.1.2.27 2012/07/10 14:26:22 aguarise Exp $
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

#include "glite/dgas/dgas-consumers/consumers/asyncConsumer.h"

#define AMQ_CONSUMER_VERSION "protoA"


using namespace std;

//this must be used by the caller to set a flag trapping a SIGINT signal on exit.
volatile sig_atomic_t goOn = 1;



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
	//overrides AsyncConsumer useMessage() method. Can be overridden by parent classes if any.
	void useMessage(std::string messageString)
	{
		std::cout << "AH AH AH:"<< messageString << std::endl;
	}

};


class AMQConsumer {

public:
	std::string logFileName;
	std::string amqBrokerUri;
	std::string amqUsername;
	std::string amqPassword;
	std::string amqClientId;
	std::string amqTopic;
	bool useTopics;
	bool clientAck;
	std::string name;
	std::string selector;
	bool noLocal;
	bool durable;
	bool foreground;
	long int messageNumber;


	AMQConsumer(
			std::string amqBrokerUri,
			std::string amqUsername = "",
			std::string amqPassword = "",
			std::string amqTopic = "")
	{
		this->amqBrokerUri = amqBrokerUri;
		this->amqUsername = amqUsername;
		this->amqPassword = amqPassword;
		this->amqTopic = amqTopic;
		this->clientAck = false;
		this->useTopics = false;
		this->durable = false;
		this->noLocal = false;
		this->foreground = false;
		this->messageNumber = 1;//Default: consume one message and exit. Set to -1 to go on forever.
	}
	int readConf(string& confFileName);
	void registerConsumer(AMQConsumerStdOut* consumer);

};



