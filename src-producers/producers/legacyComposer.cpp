// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: legacyComposer.cpp,v 1.1.2.3 2011/01/04 10:22:12 aguarise Exp $
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
//prova    

// ---------------------------------------------------------------------------
//  Includes
// ---------------------------------------------------------------------------
#include <unistd.h>
#include <signal.h>

#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <map>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/dgas-producers/producers/legacyComposer.h"



int ATM_client_toResource(ATM_job_data &job_data, ATM_usage_info &usage_info, vector<string> info_v ,string *server_answer, string confFileName)
{
	int returncode = 0;
	string output_message;
	string input_message;
	vector<string> urlBuff;
	//not specified, get them form a configuration file.
	map <string,string> confMap;
	if ( dgas_conf_read ( confFileName, &confMap ) == 0 )
	{
		if ( job_data.res_acct_PA_id == "" )
                {
                        job_data.res_acct_PA_id =(confMap["res_acct_PA_id"]).c_str();
                }
                //see if we need to do economic accounting
                if (  job_data.economicAccountingFlag == "" )
                {
			job_data.economicAccountingFlag = (confMap["economicAccounting"]).c_str();
                }
		//se if we need to do economic accounting
	}
	//Now compose the XML ATM_request
	ATMc_xml_compose("ATM_request_toResource", job_data, usage_info, info_v, &output_message);
	cout << output_message << endl;
	return returncode;
	
}//ATM_client(string HLR_url_string, string HLR_contact_string ,ATM_job_data &job_data, ATM_usage_info &usage_info, string *server_answer)


//Compose the XML ATM_request message used to send the job info to the user
//DGAS server.
void ATMc_xml_compose(string requestType, ATM_job_data &job_data, ATM_usage_info &usage_info, vector<string> info_v, string *xml)
{

        // add client version to info vector:
        string cVString = "atmClientVersion=";
	cVString += ATM_CLIENT_VERSION; 
	cVString += VERSION;
        info_v.push_back(cVString);
	*xml = "<HLR type=\"";
	*xml += requestType;
        *xml += "\">\n";
	*xml += "<BODY>\n";
	*xml += "<JOB_PAYMENT>\n";
	*xml += "<EDG_ECONOMIC_ACCOUNTING>\n";
	*xml += job_data.economicAccountingFlag;
	*xml += "</EDG_ECONOMIC_ACCOUNTING>\n";
	*xml += "<DG_JOBID>\n";
	*xml += job_data.dgJobId;
	*xml += "\n</DG_JOBID>\n";
	*xml += "<SUBMISSION_TIME>\n";
	*xml += job_data.time;
	*xml += "\n</SUBMISSION_TIME>\n";
	*xml += "<RES_ACCT_PA_ID>\n";
	*xml += job_data.res_acct_PA_id;
	*xml += "\n</RES_ACCT_PA_ID>\n";
	*xml += "<RES_ACCT_BANK_ID>\n";
	*xml += job_data.res_acct_bank_id;
	*xml += "\n</RES_ACCT_BANK_ID>\n";
	*xml += "<USER_CERT_SUBJECT>\n";
	*xml += job_data.user_CertSubject;
	*xml += "\n</USER_CERT_SUBJECT>\n";
	*xml += "<RES_GRID_ID>\n";
	*xml += job_data.res_grid_id;
	*xml += "\n</RES_GRID_ID>\n";
	*xml += "<forceResourceHlrOnly>\n";
        *xml += "yes";
       	*xml += "\n</forceResourceHlrOnly>\n";
	*xml += "<JOB_INFO>\n";
	*xml += "<CPU_TIME>\n";
	*xml += int2string(usage_info.cpu_time);
	*xml += "\n</CPU_TIME>\n";
	*xml += "<WALL_TIME>\n";
	*xml += int2string(usage_info.wall_time);
	*xml += "\n</WALL_TIME>\n";
	*xml += "<MEM>\n";
	*xml += usage_info.mem;
	*xml += "\n</MEM>\n";
	*xml += "<VMEM>\n";
	*xml += usage_info.vmem;
	*xml += "\n</VMEM>\n";
	*xml += "</JOB_INFO>\n";
	*xml += "</JOB_PAYMENT>\n";
	*xml += "<AdditionalUR>\n";
	vector<string>::iterator it = info_v.begin();
	while (it != info_v.end())
	{
		int pos = (*it).find_first_of( "=" );
		if ( pos != string::npos )
		{
			vector<attribute> attributes;
			string keyBuff = "";
			string valueBuff = "";
			keyBuff = (*it).substr(0, pos);
			valueBuff = (*it).substr(pos+1, (*it).size()-pos);	
			attribute attrBuff = {keyBuff,valueBuff};
			attributes.push_back(attrBuff);
			*xml += tagAdd ("dgas:item", "", attributes );
		}
		it++;
	}
	*xml += "</AdditionalUR>\n";
	*xml += "</BODY>\n";
	*xml += "</HLR>\n";
	return;
}


