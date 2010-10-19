// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: paClient.h,v 1.1.2.1.4.1 2010/10/19 09:04:44 aguarise Exp $
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
#include <sstream>
#include <string>
#include <vector>

#ifndef GSI
#include "glite/dgas/common/tls/SocketAgent.h"
#include "glite/dgas/common/tls/SocketClient.h"
#else
#include "glite/dgas/common/tls/GSISocketAgent.h"
#include "glite/dgas/common/tls/GSISocketClient.h"
#endif
#include "glite/dgas/common/pa/libPA_comm.h"

#include "glite/dgas/common/pa/libPA_comm.h"

using namespace std;
#ifndef GLITE_SOCKETPP
using namespace glite::wmsutils::tls::socket_pp;
#endif

int cert2hostname( string cert_subject , string *hostname);

int get_defaultPA (string *pa_url_string);

int pac_parse_xml(string &input_message, price *res_price);

int dgas_pa_client(string _acct_pa_id , price *res_price);

void pa_xml_compose(price &price_query, string *xml);

