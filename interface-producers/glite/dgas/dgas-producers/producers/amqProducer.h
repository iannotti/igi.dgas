// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: amqProducer.h,v 1.1.2.12 2012/07/06 12:46:51 aguarise Exp $
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
	string outputMessage;

public:

	std::string amqBrokerUri;
	std::string amqUsername;
	std::string amqPassword;
	std::string amqTopic;
	bool useTopics;
	bool clientAck;
	int verbosity;


	AmqProducer(std::string amqBrokerUri,
			std::string amqUsername, std::string amqPassword,
			std::string amqTopic, bool useTopics = false, bool clientAck = false,
			int verbosity = 0)
	{
		this->amqBrokerUri = amqBrokerUri;
		this->amqUsername = amqUsername;
		this->amqPassword = amqPassword;
		this->amqTopic = amqTopic;
		this->useTopics = useTopics;
		this->clientAck = clientAck;
		this->verbosity = verbosity;
	}

	void setOutputMessage(string outputMessage);
	int readConf(string& confFileName);
	int run();
};

inline void AmqProducer::setOutputMessage(string outputMessage)
{
	this->outputMessage = outputMessage;
}




