// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: atmResourceEngine.h,v 1.1.2.1.4.1 2010/10/19 09:11:04 aguarise Exp $
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

#include "glite/dgas/common/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/stringSplit.h"

#include "glite/dgas/hlr-service/base/hlrTransaction.h"
#include "glite/dgas/hlr-service/base/qTransaction.h"
//#include "glite/dgas/common/pa/libPA_comm.h"
//#include "glite/dgas/pa-clients/paClient.h"

using namespace std;

#define GRIS_PORT "2135"

#define ATM_RESOURCE_ENGINE_VERSION "glite"

namespace ATMResource{

//get the xml object from the daemon and parse the information contained in
//the request. Trigger the retrieve of a price for the corresponding resource.
//Compute the job cost. Trigger the credit-debit transaction. compose the answer
int ATMResourceEngine( string &input, connInfo &connectionInfo, string *output );

};



