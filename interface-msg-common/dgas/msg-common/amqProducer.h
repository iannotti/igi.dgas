// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: amqProducer.h,v 1.1.2.4 2012/08/20 13:36:13 aguarise Exp $
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
#include <memory>

#define AMQ_PRODUCER_VERSION "4.0.x"

class AmqProducer
{

private:
	std::string outputMessage;

public:

	std::string amqBrokerUri;
	std::string amqUsername;
	std::string amqPassword;
	std::string amqTopic;
	bool useTopics;
	bool clientAck;
	bool persistentDelivery;
	int verbosity;


	AmqProducer(std::string amqBrokerUri,
			std::string amqUsername, std::string amqPassword,
			std::string amqTopic, bool useTopics = false, bool clientAck = false, bool persistentDelivery = false,
			int verbosity = 0)
	{
		this->amqBrokerUri = amqBrokerUri;
		this->amqUsername = amqUsername;
		this->amqPassword = amqPassword;
		this->amqTopic = amqTopic;
		this->useTopics = useTopics;
		this->clientAck = clientAck;
		this->persistentDelivery = persistentDelivery;
		this->verbosity = verbosity;
	}

	void setOutputMessage(std::string outputMessage);
	int readConf(std::string& confFileName);
	int run();
};

inline void AmqProducer::setOutputMessage(std::string outputMessage)
{
	this->outputMessage = outputMessage;
}




