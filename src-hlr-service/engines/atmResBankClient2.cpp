// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: atmResBankClient2.cpp,v 1.1.2.1.4.1 2010/10/19 09:11:04 aguarise Exp $
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
//   This is different from other clients: it is meant to be used only
//   from within an HLR server to communicate the credit ticket to another
//   HLR, therefore it is not inserted in the clients API but in the server 
//   ones. 

// ---------------------------------------------------------------------------
//  Includes
// ---------------------------------------------------------------------------
#include "atmResBankClient2.h"
#include "serviceCommonUtl.h"
#include "glite/dgas/hlr-service/base/hlrTransaction.h"
#include "glite/dgas/hlr-service/base/hlrResource.h"
#include "glite/dgas/hlr-service/base/hlrTransIn.h"


extern ofstream logStream;

namespace dgasResBankClient
{

bool acctExists(string &a)
{
	bool result = false;
	hlrResource rBuff;
        rBuff.ceId=a;
        if ( rBuff.exists() )
        {
       		 result = true;
        }
        return result;
}

int bankClient2(hlrTransaction &t)
{
	int returnCode = 0;
	//string userVo;
	if ( t.exists() )
        {
                string logBuff = "ERROR: dgasResBankClient::dgas_bankClient: CREDIT_DUPL";
                hlr_log( logBuff, &logStream, 2 );
                return atoi(CREDIT_DUPL);
        }
	else
	{
		//check if the account exists
                if ( !acctExists(t.gridResource) )
                {
                        string logBuff = "ERROR: dgasResBankClient::dgas_bankClient: Could not find resource::" + t.gridResource;
                        hlr_log( logBuff, &logStream ,1);
                        return atoi(E_NO_USER);
                }
		//second line of defense for duplicated URs HERE!
		//parse transactionLog;
		bool processRecord = true;
		map<string,string> recordsMap;
		if ( cmnParseLog(t.logData, recordsMap) != 0)
		{
			string logBuff = "ERROR: dgas_bankClient: E_DEBIT_ERROR_A, can't parse logData";
			hlr_log( logBuff, &logStream ,1);
			returnCode = atoi(E_DEBIT_ERROR_A);
			processRecord = false;
		}
		//fill an ATM_resource_info instance with the info necessary
		//for the duplication check.
		//logEntries can now be passed to checkDuplicate
		bool possibleResubmission = true;
		string previousJobId;
		string uniqueChecksum;
		int duplResult = checkDuplicate(
			recordsMap,
			processRecord,
			possibleResubmission,
			previousJobId,
			uniqueChecksum);
		if ( duplResult != 0 )
		{
			possibleResubmission = false;
			returnCode = duplResult;
			//it seems the record is duplicated...
		}
		if ( possibleResubmission && t.exists() && processRecord )
		{
			hlr_log ("ATM Engine: Warning: record dgJobId already present, possible resubmission.", &logStream,3);
		}
		//processRecord is set according to result of checkDuplicate.
		if ( processRecord )
		{
			t.uniqueChecksum = uniqueChecksum;
			string logBuff = "Inserting:" + t.id + "," + t.uniqueChecksum;
			hlr_log( logBuff, &logStream ,7);
			t.accountingProcedure=recordsMap["accountingProcedure"];
			if ( t.process() != 0 )
        	        {
				string logBuff = "ERROR: dgas_bankClient: E_DEBIT_ERROR_B";
				hlr_log( logBuff, &logStream ,1);
				returnCode = atoi(E_DEBIT_ERROR_B);
			}
			else
			{
				//log transaction data
				transInLog transInLogBuff(t.id, t.logData);
                	        if ( transInLogBuff.put() != 0 )
	                        {
        	                      hlr_log("resBankClient: Error inserting info in transInLog table.",&logStream,3);
					hlrTransIn transInBuff;
                                       transInBuff.tid = t.tid;
                                       int res = transInBuff.del();
                                       if ( res != 0 )
                                       {
                                               hlr_log("resBankClient: Error deleting trans_in.",&logStream,3);
                                               returnCode = atoi(E_DEBIT_ERROR_C);          
                                       }
					else
                                        {
                                                hlr_log("resBankClient: deleted trans_in entry (rollback).",&logStream,5);
                                                returnCode = atoi(E_DEBIT_ERROR_D);
                                        }
                	        }
			}
		}
	}
	return returnCode;
}

};
