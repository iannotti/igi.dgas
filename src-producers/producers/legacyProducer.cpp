// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: legacyProducer.cpp,v 1.1.2.2 2010/11/04 13:37:49 aguarise Exp $
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
#include "glite/dgas/dgas-producers/producers/legacyProducer.h"

void
catch_alarm (int sig)
{
       exit(-1);
}

int  defConnTimeOut = 60;

int ATM_client_toResource(string& input ,string *server_answer, string confFileName)
{
	int returncode = 0;
	string output_message;
	vector<string> urlBuff;
	//not specified, get them form a configuration file.
	map <string,string> confMap;
	if ( dgas_conf_read ( confFileName, &confMap ) == 0 )
	{
		if ( job_data.res_acct_PA_id == "" )
		{
			job_data.res_acct_PA_id =(confMap["res_acct_PA_id"]).c_str();
		}	
		if ( job_data.res_acct_bank_id == "" )
		{
			job_data.res_acct_bank_id =(confMap["res_acct_bank_id"]).c_str();
		}	
		//se if we need to do economic accounting
		job_data.economicAccountingFlag = (confMap["economicAccounting"]).c_str();
	}
	else
	{
		cerr << "WARNING: Error reading conf file: " << confFileName << endl;
		cerr << "There can be problems processing the transaction" << endl;
	}
	Split(':',job_data.res_acct_bank_id, &urlBuff);
	if ( urlBuff.size() != 3 )
		return atoi(E_WRONG_RES_HLR);
	string resBankHostname = urlBuff[0];
	int resBankPort = atoi((urlBuff[1]).c_str());
	string resBankContact = urlBuff[2];
	//if the acct_PA_res_id and the acct_bank_res_id are
	
	signal (SIGALRM, catch_alarm);
	alarm ( defConnTimeOut);
	GSISocketClient *theClient = new GSISocketClient(resBankHostname, resBankPort);
	theClient-> ServerContact(resBankContact);
	theClient->SetTimeout( defConnTimeOut );
	if ( !(theClient->Open()))
	{
		 returncode = atoi(E_NO_CONNECTION); 
	}
	else
	{
		if ( !(theClient->Send(input_message)))
		{
			 returncode = atoi(E_SEND_MESSAGE);
		}
		if ( !(theClient->Receive(output_message)) )
		{
			 returncode = atoi(E_RECEIVE_MESSAGE);
		}
		theClient->Close();
		if (returncode == 0)
		{
			returncode = ATMc_parse_xml(output_message);
		}
		*server_answer = output_message;
	}
	delete theClient;
	return returncode;
	
}//ATM_client(string HLR_url_string, string HLR_contact_string ,ATM_job_data &job_data, ATM_usage_info &usage_info, string *server_answer)



//Parse the XML input, returns 0 if success >1 otherwise, differerntly from
//the other client code, here we will not use Xerces since we want the code
//to be very light and easily portable.
int ATMc_parse_xml(string input_message)
{
	//we only need the return code, the whole message can
	//be seen as a receipt
	string buffer;
	size_t pos; 
	pos = input_message.find( "<CODE>" );
	if ( pos == string::npos )
	{
		return atoi(E_PARSE_ERROR);
	}
	int start_pos = pos +6;
	pos = input_message.find( "</CODE>" );
	if ( pos == string::npos ) 
	{
		return atoi(E_PARSE_ERROR);
	}
	int end_pos = pos -1;
	buffer = input_message.substr( start_pos, end_pos-start_pos);
	//now isolate the code
	pos = buffer.find_first_of( "1234567890" );
	if ( pos == string::npos )
	{
		return atoi(E_PARSE_ERROR);
        }
	start_pos = pos;
	pos = buffer.find_first_of( " \n", start_pos );
	if ( pos == string::npos )
	{
		return atoi((buffer.substr(start_pos)).c_str());
        }
	else
	{
		end_pos = pos;
		return atoi((buffer.substr(start_pos, end_pos-start_pos)).c_str());
	}
	
}

