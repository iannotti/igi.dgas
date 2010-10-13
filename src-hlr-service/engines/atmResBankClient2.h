// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: atmResBankClient2.h,v 1.1.2.1 2010/10/13 12:59:49 aguarise Exp $
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
#ifndef RESBANK_CLIENT2_H
#define RESBANK_CLIENT2_H

#include <string.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <sstream>
#include <fstream>

#include "glite/dgas/common/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "engineCmnUtl.h"

//#include "glite/dgas/common/ldap/LDAPSynchConnection.h"
//#include "glite/dgas/common/ldap/LDAPQuery.h"
//#include "glite/dgas/common/ldap/LDAPForwardIterator.h"
//#include "glite/dgas/common/ldap/LDIFObject.h"

#include "glite/dgas/hlr-service/base/hlrTransaction.h"
//#include "glite/dgas/hlr-service/base/jobAuth.h"
#include "glite/dgas/hlr-service/base/transInLog.h"

#ifndef GSI
#include "glite/dgas/common/tls/SocketAgent.h"
#include "glite/dgas/common/tls/SocketClient.h"
#else
#include "glite/dgas/common/tls/GSISocketAgent.h"
#include "glite/dgas/common/tls/GSISocketClient.h"
#endif

using namespace std;
#ifndef GLITE_SOCKETPP
using namespace glite::wmsutils::tls::socket_pp;
#endif
//parse XML and return value of STATUS tag

namespace dgasResBankClient
{
	int bankClient2(hlrTransaction &t);
};
#endif
