// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: AMQConsumer.h,v 1.1.2.5 2012/06/22 09:11:09 aguarise Exp $
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

#define AMQ_CONSUMER_VERSION "protoA"

using namespace std;

class consumerParms {
	public:
		string confFileName;
		string amqBrokerUri;
		string amqUsername;
		string amqPassword;
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
};

int AMQConsumer(consumerParms& parms);


