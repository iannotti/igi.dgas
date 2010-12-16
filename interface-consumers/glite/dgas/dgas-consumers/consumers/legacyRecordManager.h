// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: legacyRecordManager.h,v 1.1.2.1 2010/12/16 15:06:28 aguarise Exp $
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

int dgasHlrRecordConsumer(string& config, string& brokerUri, string& messageParser, bool dryRun, bool singleRun);


