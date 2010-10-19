// DGAS (DataGrid Accounting System) 
// Server Daemon and protocol engines.
// 
// $Id: pingEngine.cpp,v 1.1.2.1.4.1 2010/10/19 09:11:04 aguarise Exp $
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
#include "pingEngine.hpp"
#include "glite/dgas/common/base/xmlUtil.h"
#include <fstream>

extern ofstream logStream;
extern string dgas_var_dir;

int pingType_zero ( string *output )
{
	*output += "<REPLY>\n0\n";
	*output += "</REPLY>\n";
	return 0;
}

int pingType_one ( statusInfo &status, errorInfo &errors, listenerStatus& lStatus, string *output )
{
	*output += "<STATUS_INFO>\n";
		*output += "<ENGINES>\n";
		*output += status.engines;
		*output += "\n</ENGINES>\n";
		if ( status.ATMRequests != 0 )
		{
			*output += "<ATM_REQUESTS>\n";
			*output += int2string(status.ATMRequests);
			*output += "\n</ATM_REQUESTS>\n";
		}
		if ( status.pingRequests != 0 )
		{
			*output += "<PING_REQUESTS>\n";
			*output += int2string(status.pingRequests);
			*output += "\n</PING_REQUESTS>\n";
		}
		if ( errors.ATMErrors != 0 )
		{
			*output += "<ATM_ERRORS>\n";
			*output += int2string(errors.ATMErrors);
			*output += "\n</ATM_ERRORS>\n";
		}
		if ( status.authErrors != 0 )
		{
			*output += "<AUTH_ERRORS>\n";
			*output += int2string(status.authErrors);
			*output += "\n</AUTH_ERRORS>\n";
		}
		*output += "<LISTENER_STATUS>\n";
		*output += "activeThreads:" + int2string(lStatus.activeThreads) + "\n";
		*output += "\n</LISTENER_STATUS>\n";
		*output += "</STATUS_INFO>\n";
		return 0;
}

int pingType_generic ( int pingType, string *output )
{
	string message;
	string textLine;
	string fileName = dgas_var_dir + "/ping_"+ int2string(pingType)+ ".out";
	ifstream f(fileName.c_str(), ios_base::in );
	if ( !f )
	{
		message = "Ping type not implemented on this server. Sorry.";
	}
	else
	{
		int i = 0;
		while ( getline (f, textLine, '\n') && ( i < 300 ) )
		{
			message += textLine + "\n";
			i++;
		}
		f.close();
	}
	*output += "<STATUS_INFO>\n";
	*output += "<serverMessage>";
	*output += message;
	*output += "</serverMessage>";
	*output += "</STATUS_INFO>\n";
	return 0;
}

void ping_engine( string &doc, connInfo &connectionInfo, statusInfo &status, errorInfo &errors, listenerStatus &lStatus, string *output )
{
	hlr_log( "Ping engine: Entering" , &logStream,4 );
	string logBuff = "Ping engine: Serving: " + connectionInfo.hostName +
		"(" + connectionInfo.contactString + ")";
	hlr_log( logBuff, &logStream,4 );
	int pingType = 0;
	node nodeBuff;
	while ( nodeBuff.status == 0 )
	{
		string tag = "PING";
		nodeBuff = parse( &doc, tag );
		if ( nodeBuff.status != 0)
			break;
		node tagBuff;
		tagBuff = parse ( &doc, "TYPE");
		if ( tagBuff.status == 0 )
		{
			pingType  = atoi((tagBuff.text).c_str());
		}
		nodeBuff.release();
	}

	*output = "<HLR type =\"ping_reply\">\n";
	*output += "<BODY>\n";
	
	switch ( pingType )
	{
		case 0: pingType_zero ( output ); break;
		case 1: pingType_one ( status , errors , lStatus , output ); break;
		case 2: pingType_generic ( pingType, output ); break;
		case 3: pingType_generic ( pingType, output ); break;
		case 4: pingType_generic ( pingType, output ); break;
		case 5: pingType_generic ( pingType, output ); break;
		case 6: pingType_generic ( pingType, output ); break;
		case 7: pingType_generic ( pingType, output ); break;
		case 8: pingType_generic ( pingType, output ); break;
		case 9: pingType_generic ( pingType, output ); break;
		case 10: pingType_generic ( pingType, output ); break;
		default : break;
	}
	*output +="</BODY>\n</HLR>\n";
	hlr_log( "Ping Engine: exit" , &logStream,4 );
	return;
}


