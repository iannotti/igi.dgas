// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: legacyProducer.h,v 1.1.2.1 2010/10/21 11:56:48 aguarise Exp $
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

#include "glite/dgas/common/tls/GSISocketAgent.h"
#include "glite/dgas/common/tls/GSISocketClient.h"

#define ATM_CLIENT_VERSION "glite"

#define GLITE_DGAS_DEF_CONF "/opt/glite/etc/dgas_sensors.conf"

using namespace std;
#ifndef GLITE_SOCKETPP
using namespace glite::wmsutils::tls::socket_pp;
#endif

int ATM_client_toResource(
		string& res_acct_bank_id,
		string& input, 
		string *server_answer,
		string confFileName = GLITE_DGAS_DEF_CONF);


//Parse the XML input, returns 0 if success >1 otherwise, differerntly from
//the other client code, here we will not use Xerces since we want the code
//to be very light and easily portable.
int ATMc_parse_xml(string input_message);

