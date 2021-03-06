// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: atmClient.h,v 1.1.2.1 2010/11/09 09:36:06 aguarise Exp $
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

struct ATM_job_data
{
	string dgJobId;
	string time;
	string res_acct_PA_id;
	string res_acct_bank_id;
	string user_CertSubject;
	string res_grid_id;
	string economicAccountingFlag;
};

struct ATM_usage_info
{
	int cpu_time;
	int wall_time;
 	string mem;
	string vmem;
};

int ATM_client_toResource(
		ATM_job_data &job_data, 
		ATM_usage_info &usage_info,
		vector<string> info_v, 
		string *server_answer,
		string confFileName = GLITE_DGAS_DEF_CONF);


//Parse the XML input, returns 0 if success >1 otherwise, differerntly from
//the other client code, here we will not use Xerces since we want the code
//to be very light and easily portable.
int ATMc_parse_xml(string input_message);

//Compose the XML ATM_request message used to send the job info to the user
//DGAS server.
void ATMc_xml_compose(
		string requestType,
		ATM_job_data &job_data, 
		ATM_usage_info &usage_info,
		vector<string> info_v, 
		string *xml);


