// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: amqProducer.h,v 1.1.2.7 2012/07/06 12:05:41 aguarise Exp $
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

	string confFileName;
	std::string amqBrokerUri;
	std::string amqUsername;
	std::string amqPassword;
	std::string amqTopic;
	std::string useTopics;
	std::string clientAck;
	int verbosity;
    string getOutput_message() const;
    void setOutputMessage(string outputMessage);

	void AmqProducer(string confFileName, std::string amqBrokerUri,
			std::string amqUsername, std::string amqPassword,
			std::string amqTopic, std::string useTopics, std::string clientAck,
			int verbosity)
	{
		this->confFileName = confFileName;
		this->amqBrokerUri = amqBrokerUri;
		this->amqUsername = amqUsername;
		this->amqPassword = amqPassword;
		this->amqTopic = amqTopic;
		this->useTopics = useTopics;
		this->clientAck = clientAck;
		this->verbosity = verbosity;
	}

	int run();
};

inline void AmqProducer::setOutputMessage(string outputMessage)
{
	this->outputMessage = outputMessage;
}




