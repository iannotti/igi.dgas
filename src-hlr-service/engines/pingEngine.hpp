// DGAS (DataGrid Accounting System) 
// Server Daemon and protocol engines.
// 
// $Id: pingEngine.hpp,v 1.1.2.1.4.1 2010/10/19 09:11:04 aguarise Exp $
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
#ifndef PING_ENGINE_HPP
#define PING_ENGINE_HPP

#include <string>
#include <iostream>
#include <sstream>
#include <vector>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/int2string.h"

struct statusInfo {
	string engines;
	int uiRequests;
	int ATMRequests;
	int pingRequests;
	int authErrors;
};

struct errorInfo {
	string engines;
        int ATMErrors;
        int pingErrors;
};

void ping_engine( string &doc, connInfo &connectionInfo, statusInfo &status, errorInfo &errors, listenerStatus &lStatus, string *output );

#endif
