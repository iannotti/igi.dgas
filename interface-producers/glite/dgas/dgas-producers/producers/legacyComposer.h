// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: legacyComposer.h,v 1.1.2.2 2011/03/15 10:38:34 aguarise Exp $
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

#define ATM_CLIENT_VERSION "glite"

#define GLITE_DGAS_DEF_CONF "/etc/dgas/dgas_sensors.conf"

using namespace std;

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

//Compose the XML ATM_request message used to send the job info to the user
//DGAS server.
void ATMc_xml_compose(
		string requestType,
		ATM_job_data &job_data, 
		ATM_usage_info &usage_info,
		vector<string> info_v, 
		string *xml);


