// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: getRecordEngine.h,v 1.1.2.1.4.2 2010/12/13 10:18:36 aguarise Exp $
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
#include <string>
#include <sstream>

#include "glite/dgas/hlr-service/engines/engineCmnUtl.h"

#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/hlr-service/base/hlrAdmin.h"

using namespace std;

struct baseRecord
{
	string tableName;
        string timestamp;
        string uniqueChecksum;
	string siteName;
        string vo;
        string voDefSubClass;
        string storage;
        string storageDefSubClass;
        string usedBytes;
        string freeBytes;
};

int getRecordEngine( string &input, connInfo &connectionInfo, string *output );




