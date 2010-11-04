// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: amqProducer.h,v 1.1.2.1 2010/11/04 13:30:16 aguarise Exp $
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

#define AMQ_PRODUCER_VERSION "protoA"

#define GLITE_DGAS_DEF_CONF "/opt/glite/etc/dgas_sensors.conf"

using namespace std;

int dgasHlrRecordProducer(string& config, string amqBrokerUri="",string amqTopic = "", string amqOptions = "");


