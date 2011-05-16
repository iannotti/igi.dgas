// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: pingClient.cpp,v 1.1.2.1.4.4 2011/05/16 08:37:50 aguarise Exp $
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
#include <string>
#include <vector>
#include <iostream>

#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/hlr-clients/ping/pingClient.h"


void ping_xml_compose(int pingType, string *xml)
{
	//initialize the message
	//the message will be of type PA_query
	*xml = "<HLR type=\"ping_request\">\n<BODY>\n";
	*xml += "<PING>\n";
	*xml += "<TYPE>\n";
	*xml += int2string(pingType);
	*xml += "\n</TYPE>\n";
	*xml += "</PING>\n";
	*xml += "</BODY>\n</HLR>\n";
	return;
}//pa_xml_compose


int ping_parse_xml ( string &xmlInput, statusInfo *status, errorInfo *errors)
{
	string buffer;
	node Node;
	string tag = "STATUS_INFO";
	Node = parse (&xmlInput, tag); //isolate JOB_AUTH_INFO node
	if ( Node.status == 0 )
	{
		node tagBuff;
		tagBuff = parse ( &Node.text, "ENGINES");
		if ( tagBuff.status == 0 )
		{
			status->engines = tagBuff.text;
		}
		tagBuff = parse ( &Node.text, "serverMessage");
		if ( tagBuff.status == 0 )
		{
			status->serverMessage = tagBuff.text;
		}
		tagBuff = parse ( &Node.text, "UI_REQUESTS");
		if ( tagBuff.status == 0 )
		{
			status->uiRequests = atoi((tagBuff.text).c_str());
		}
		tagBuff = parse ( &Node.text, "ATM_REQUESTS");
		if ( tagBuff.status == 0 )
		{
			status->ATMRequests = atoi((tagBuff.text).c_str());
		}
		tagBuff = parse ( &Node.text, "PING_REQUESTS");
		if ( tagBuff.status == 0 )
		{
			status->pingRequests = atoi((tagBuff.text).c_str());
		}
		tagBuff = parse ( &Node.text, "ATM_ERRORS");
		if ( tagBuff.status == 0 )
		{
			errors->ATMErrors = atoi((tagBuff.text).c_str());
		}
		tagBuff = parse ( &Node.text, "AUTH_ERRORS");
		if ( tagBuff.status == 0 )
		{
			status->authErrors = atoi((tagBuff.text).c_str());
		}
	}
	return 0;
}


int dgas_ping_client(string &acct_id, int pingType, statusInfo *status,errorInfo *errors , string *server_answer)
{
	int returnCode = 0;
	string output_message; 
	string input_message;
	string hlrHostname = "";
	int hlrPort = 56568;//default value
	string hlrContact = "";
	vector<string> urlBuff;
	Split(':', acct_id, &urlBuff);
	if ( urlBuff.size() > 0 )
	{
		if ( urlBuff.size() > 0 ) hlrHostname = urlBuff[0];
		if ( urlBuff.size() > 1 ) hlrPort = atoi((urlBuff[1]).c_str());
		if ( urlBuff.size() > 2 ) hlrContact = urlBuff[2];
	}
	//compose the xml message.
	ping_xml_compose(pingType, &output_message);
	GSISocketClient *theClient = new GSISocketClient(hlrHostname,hlrPort);
	theClient-> ServerContact(hlrContact);
	if (!(theClient -> Open()))
	{
		returnCode = atoi(E_NO_CONNECTION) ;
	}
	else
	{	
		theClient -> SetTimeout(10);
		if ( !(theClient->Send(output_message)) )
		{
			returnCode =  atoi(E_SEND_MESSAGE);
		}
		if ( !(theClient->Receive(input_message)) )
		{
			returnCode =  atoi(E_RECEIVE_MESSAGE);
		}
		theClient->Close();
		delete theClient;
		*server_answer = input_message;
		if (returnCode == 0)
		{
			returnCode = ping_parse_xml(input_message, status, errors);
		}
	}
	return returnCode;
}//pa_client


