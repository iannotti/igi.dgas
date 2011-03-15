// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: legacyProducer.cpp,v 1.1.2.10 2011/03/15 13:30:36 aguarise Exp $
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

#define GLITE_DGAS_DEF_CONF "/etc/dgas/dgas_sensors.conf"

void
catch_alarm (int sig)
{
       exit(-1);
}

int  defConnTimeOut = 60;

int ATM_client_toResource(string& input ,string *server_answer, producerConfiguration& conf)
{
	if (conf.configFileName == "") conf.configFileName =  GLITE_DGAS_DEF_CONF;
	int returncode = 0;
	string output_message;
	//not specified, get them form a configuration file.
	map <string,string> confMap;
	if ( dgas_conf_read ( conf.configFileName, &confMap ) == 0 )
	{
		if ( conf.hlrServer == "" )
		{
			conf.hlrServer =(confMap["res_acct_bank_id"]).c_str();
		}	
	}
	int hlrPort = 56568;//default value
        string hlrContact = "";
        string hlrHostname;
	vector<string> urlBuff;
        Split(':', conf.hlrServer, &urlBuff);
        if ( urlBuff.size() > 0 )
        {
                if ( urlBuff.size() > 0 ) hlrHostname = urlBuff[0];
                if ( urlBuff.size() > 1 ) hlrPort = atoi((urlBuff[1]).c_str());
                if ( urlBuff.size() > 2 ) hlrContact = urlBuff[2];
        }
	else
	{
		return atoi(E_WRONG_RES_HLR);
	}
	//if the acct_PA_res_id and the acct_bank_res_id are
	
	signal (SIGALRM, catch_alarm);
	alarm ( defConnTimeOut);
	GSISocketClient *theClient = new GSISocketClient(hlrHostname, hlrPort);
	theClient-> ServerContact(hlrContact);
	theClient->SetTimeout( defConnTimeOut );
	if ( !(theClient->Open()))
	{
		 returncode = atoi(E_NO_CONNECTION); 
	}
	else
	{
		if ( !(theClient->Send(input)))
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



//Parse the XML input, returns 0 if success >1 otherwise
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

