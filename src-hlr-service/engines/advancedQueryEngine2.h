// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: advancedQueryEngine2.h,v 1.1.2.1.4.3 2011/06/15 15:36:10 aguarise Exp $
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
#include <unistd.h>
#include <sys/types.h>

#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/hlr-service/base/hlrAdmin.h"
#include "glite/dgas/hlr-service/base/hlrGenericQuery.h"
#include "dbWaitress.h"

//#include "glite/dgas/common/ldap/LDAPSynchConnection.h"
//#include "glite/dgas/common/ldap/LDAPQuery.h"
//#include "glite/dgas/common/ldap/LDAPForwardIterator.h"
//#include "glite/dgas/common/ldap/LDIFObject.h"

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_sql_dbname;
extern string mergeTablesDefinitions;

using namespace std;

struct inputData
{
	string emailBuffer;
	string userCertBuffer;
	string resourceIDBuffer;
	string timeBuffer;
	string groupIDBuffer;
	string voIDBuffer;
	string fqanBuffer;
	string transactionTypeBuff;
	string frequencyBuffer;
	string timeIndexBuff;
	bool debug;
	bool aggregateFlag;
	bool listFlag;
	string queryTypeBuffer;
	bool itsHeading;
	bool extended;
	string jobId;
	string startTid;
	string lastTid;
	string aggregateStringBuff;
	string groupBy;
	string siteName;
	string urOrigin;
	string substDn;
	string orderBy;
	string itsFieldList;
};



//get the xml object from the daemon and parse the information contained in
//the request. Trigger the retrieve of a price for the corresponding resource.
//Compute the job cost. Trigger the credit-debit transaction. compose the answer
int advancedQueryEngine( string &input, connInfo &connectionInfo, string *output );




