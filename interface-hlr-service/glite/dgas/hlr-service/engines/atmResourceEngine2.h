// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: atmResourceEngine2.h,v 1.1.2.1 2010/10/22 12:13:51 aguarise Exp $
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
#include <stdlib.h>
#include <vector>
// #include <map>
#include <string>
#include <sstream>

#include "engineCmnUtl.h"

using namespace std;

#define GRIS_PORT "2135"

#define ATM_RESOURCE_ENGINE_VERSION2 "glitev2"

namespace ATMResource{

//get the xml object from the daemon and parse the information contained in
//the request. Trigger the retrieve of a price for the corresponding resource.
//Compute the job cost. Trigger the credit-debit transaction. compose the answer
int ATMResourceEngine2( string &input, connInfo &connectionInfo, string *output );

};



