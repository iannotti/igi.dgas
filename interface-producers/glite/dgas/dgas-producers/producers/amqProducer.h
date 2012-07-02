// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: amqProducer.h,v 1.1.2.6 2012/07/02 09:39:58 aguarise Exp $
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

#define AMQ_PRODUCER_VERSION "protoA"

using namespace std;

class producerParms {
	public:
		string confFileName;
		string amqBrokerUri;
		string amqUsername;
		string amqPassword;
		string dgasAMQTopic;
		string useTopics;
		string clientAck;
		int verbosity;
};

int dgasHlrRecordProducer(producerParms& parms);


