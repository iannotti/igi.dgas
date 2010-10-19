// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: paClient.cpp,v 1.1.2.1.4.1 2010/10/19 09:12:32 aguarise Exp $
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
#include <sstream>
#include <iostream>

#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/pa/libPA_comm.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/pa-clients/paClient.h"

#ifndef LOCAL_PA
#define LOCAL_PA "localhost:56567"
#endif
#define GRIS_PORT "2135"
#define ACCT_PA_ID  "ACCT_PA_ID"

extern ofstream logStream;

int pac_parse_xml ( string &xmlInput, price *priceRes)
{
	string buffer;
	node Node;
	string tag = "PRICE_INFO";
	Node = parse (&xmlInput, tag); //isolate PRICE_INFO node
	if ( Node.status == 0 )
	{
		node tagBuff;
		tagBuff = parse ( &Node.text, "ERROR");
		if ( tagBuff.status == 0 )
		{
			return atoi((tagBuff.text).c_str());//report the error
		}
		tagBuff = parse ( &Node.text, "RES_ID");
		if ( tagBuff.status == 0 )
		{
			priceRes->resID = tagBuff.text;
		}
		else
		{
			return atoi(E_PARSE_ERROR);
		}
		tagBuff = parse ( &Node.text, "TIME");
		if ( tagBuff.status == 0 )
		{
			priceRes->time = atoi((tagBuff.text).c_str());
		}
		else
		{
			return atoi(E_PARSE_ERROR);
		}
		tagBuff = parse ( &Node.text, "MIN_TTL");
		if ( tagBuff.status == 0 )
		{
			priceRes->minTTL = atoi((tagBuff.text).c_str());
		}
		else
		{
			return atoi(E_PARSE_ERROR);
		}
		tagBuff = parse ( &Node.text, "PRICE");
		if ( tagBuff.status == 0 )
		{
			priceRes->priceValue = atoi((tagBuff.text).c_str());
		}
		else
		{
			return atoi(E_PARSE_ERROR);
		}
	}
	return 0;
}

//TBI
int cert2hostname( string cert_subject , string *hostname)
{
	//cert_subject -> hostname
	// 0 on success
	// 1 on failure
	
	return 0;
}

int get_defaultPA(string *pa_url_string)
{
	*pa_url_string=LOCAL_PA; 
	return 0;
}


//cert_subject : certificate subject of the resource that has to be priced
//pa_url_string : url of the PA that we want to query, if pa_url_string is NULL
//the system will try to contact the PA of the resource or the default PA of the//caller system.
//time :  GMT timestamp of the price that we want.
//res_price : pointer to the structure that will contain the result.
int dgas_pa_client(string _acct_pa_id , price *res_price)
{
	string output_message; 
	string input_message;
	int returnCode = 0;
	if (_acct_pa_id == "") //user didn't give the _acct_pa_id directly
	{
		if ( _acct_pa_id == "" ) //_acct_pa_id not found in the GRIS
		{
			// the GRIS doesn't contain th PA URL
			// use the default
			return atoi(E_NO_PA);
		}
	}
	char delim = ':';
	//decompose remote PA url into server_name and port.
	vector<string> urlBuff;
	Split(delim, _acct_pa_id, &urlBuff);
	if ( urlBuff.size() != 3 )
		return atoi(E_PA_PARSE_ID);
	string paHostname = urlBuff[0];
	int paPort = atoi((urlBuff[1]).c_str());
	string paContact = urlBuff[2];
	//compose the xml message.
	price pa_query = {res_price->resID, res_price->time};
	string logBuff = "Requesting price for resource:";
	logBuff += pa_query.resID;
	logBuff += ", timestamp:";
	logBuff += int2string(pa_query.time);
	hlr_log( logBuff, &logStream );
	pa_xml_compose(pa_query, &output_message);
	//contact the PA server
	GSISocketClient *theClient = new GSISocketClient(paHostname,paPort);
	theClient-> ServerContact(paContact);
	if (!(theClient -> Open()))
	{
		returnCode =  atoi(E_NO_CONNECTION);
        }
	else
	{	
		if ( !(theClient->Send(output_message)) )
		{
			returnCode = atoi(E_SEND_MESSAGE);
		}
		if ( !(theClient->Receive(input_message)) )
		{
			returnCode = atoi(E_RECEIVE_MESSAGE);
		}
		theClient->Close();
		returnCode = pac_parse_xml(input_message, res_price);
	}
	delete theClient;
	return returnCode;
}//pa_client

void pa_xml_compose(price &price_query, string *xml)
{
	//initialize the message
	//the message will be of type PA_query
	*xml = "<HLR type=\"PA_query\">\n<HEAD>\n<VER>1.0</VER>\n</HEAD>\n<BODY>\n";
	*xml += "<PRICE_INFO>\n";
	*xml += "<RES_ID>\n";
	*xml += price_query.resID;
	*xml += "\n</RES_ID>\n";
	*xml += "<TIME>\n";
	*xml += int2string(price_query.time);
	*xml += "\n</TIME>\n";
	*xml += "</PRICE_INFO>\n";
	*xml += "</BODY>\n</HLR>\n";
	return;
}//pa_xml_compose

