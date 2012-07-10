// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: AMQConsumer.h,v 1.1.2.14 2012/07/10 09:14:42 aguarise Exp $
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

#include "glite/dgas/dgas-consumers/consumers/asyncConsumer.h"

#define AMQ_CONSUMER_VERSION "protoA"


using namespace std;

//this must be used by the caller to set a flag trapping a SIGINT signal on exit.
extern volatile sig_atomic_t goOn;

/*
class recordConsumerParms {
	public:
		string configFile;
		string amqBrokerUri;
		string amqUsername;
		string amqPassword;
		string amqClientId;
		string lockFileName;
		string logFileName; 
		string dgasAMQTopic;
		string useTopics;
		string clientAck;
		string hlrSqlTmpDBName;
		string hlrSqlDBName;
		string hlrSqlServer;
		string hlrSqlUser;
		string hlrSqlPassword;
		string name;
		string selector;
		string noLocal;
		string durable;
		string foreground;
		string outputType;
		string outputDir;
		string messageNumber;
};
*/

class AMQConsumer: public SimpleAsyncConsumer {

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
	}
	int readConf(string& confFileName);
	void run();
	//overrides AsyncConsumer useMessage() method
	virtual void useMessage(std::string messageString)
	{
		std::cout << "AH AH AH" << std::endl;
	}

};

/*
class messageFactory {

private:
	std::string messageString;

public:

	void messageFactory (std::string messageString)
	{
		this->messageString = messageString;
	}
	int toDatabase(
				std::string hlrSqlDBName,
				std::string hlrSqlDBName,
				std::string hlrSqlServer,
				std::string hlrSqlUser,
				std::string hlrSqlPassword,
			);
	//int toDatabase(dbhandler); FIXME
	int toFile(std::string fileName);
	int toDir(std::string outputDir);
	int toStdout();
};
*/


