// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: xmlHlrHelper.cpp,v 1.1.2.1.4.1 2010/10/19 09:11:05 aguarise Exp $
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
#include "xmlHlrHelper.h"
#include "glite/dgas/hlr-service/engines/atmResourceEngine.h"
#include "glite/dgas/hlr-service/engines/atmResourceEngine2.h"
#include "pingEngine.hpp"
#include "advancedQueryEngine2.h"
#include "urConcentratorEngine.h"
#include "getRecordEngine.h"


//using namespace glite::workload::dgas::engines;

extern statusInfo serverStatus;
extern errorInfo errorStatus;
extern bool is2ndLevelHlr;
extern bool useATMVersion2;
extern int authErrors;

int parse_xml( string &xmlInput, connInfo &connectionInfo, listenerStatus &lStatus, string *output)
{
	int returnCode = 0;
	attrType attributes;
	node Node = parse(&xmlInput, "HLR");
	attributes = Node.getAttributes();
	if ( Node.status !=0 )
	{
		 returnCode = Node.status;
	}
	else
	{
		if ( !is2ndLevelHlr )
		{
			if ( attributes["type"] == "ATM_request_toResource" )
			{
				serverStatus.ATMRequests++;
				int res = 0;
				if ( useATMVersion2 )
				{
					res = ATMResource::ATMResourceEngine2 ( Node.text, connectionInfo, output);
				}
				else
				{
					res = ATMResource::ATMResourceEngine ( Node.text, connectionInfo, output);
				}
				if ( res != 0 )
					errorStatus.ATMErrors++;
			}
		}
		else
		{
			if ( attributes["type"] == "urConcentrator" )
			{
				urConcentratorEngine(xmlInput, connectionInfo, output);
			}
		}
		if ( attributes["type"] == "sendRecord" )
		{
			 getRecordEngine( xmlInput, connectionInfo ,output);
		}
		if ( attributes["type"] == "advancedQuery" )
		{
			serverStatus.uiRequests++;
			advancedQueryEngine ( xmlInput, connectionInfo ,output);
		}
		if ( attributes["type"] == "ping_request" )
		{
			serverStatus.authErrors = authErrors;
			serverStatus.pingRequests++;
			ping_engine ( xmlInput, connectionInfo , serverStatus, errorStatus, lStatus ,output);
		}
		if ( attributes["type"] == "Error" )
		{
			output->append("<HLR type=\"hlr_error\"><HLR>");
		}
		
	}
	return returnCode;
}




