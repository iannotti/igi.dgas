// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: atmResBankClient.cpp,v 1.1.2.1.4.1 2010/10/19 09:11:04 aguarise Exp $
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
#include "atmResBankClient.h"
#include "serviceCommonUtl.h"
#include "glite/dgas/hlr-service/base/hlrTransaction.h"
#include "glite/dgas/hlr-service/base/hlrResource.h"
#include "glite/dgas/hlr-service/base/hlrTransIn.h"

extern ofstream logStream;

extern bool strictAccountCheck;
extern bool lazyAccountCheck;

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

int bankClient(hlrTransaction &t)
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
		if ( !lazyAccountCheck )
		{
		//check if the account exists
   	             if ( !acctExists(t.gridResource) )
        	        {
                	        string logBuff = "ERROR: dgasResBankClient::dgas_bankClient: Could not find resource::" + t.gridResource;
                   	     hlr_log( logBuff, &logStream ,1);
                        	return atoi(E_NO_USER);
                	}
		}
		//second line of defense for duplicated URs HERE!
		//parse transactionLog;
		bool processRecord = true;
		cmnLogRecords recordsBuff;
		if ( cmnParseLog(t.logData, recordsBuff) != 0)
		{
			string logBuff = "ERROR: dgas_bankClient: E_DEBIT_ERROR_A, can't parse logData";
			hlr_log( logBuff, &logStream ,1);
			returnCode = atoi(E_DEBIT_ERROR_A);
			processRecord = false;
		}
		//fill an ATM_resource_info instance with the info necessary
		//for the duplication check.
		ATM_job_record usageInfo;
		usageInfo.dgJobId = t.id;
		usageInfo.wall_time = recordsBuff.wallTime;
		usageInfo.cpu_time = recordsBuff.cpuTime;
		usageInfo.res_grid_id = recordsBuff.ceId;
		usageInfo.lrmsId = recordsBuff.lrmsId;
		usageInfo.jobName = recordsBuff.jobName;
		usageInfo.start = recordsBuff.start;
		usageInfo.end = recordsBuff.end;
		usageInfo.accountingProcedure = recordsBuff.accountingProcedure;
		//logEntries can now be passed to checkDuplicate
		bool possibleResubmission = true;
		string previousJobId;
		string uniqueChecksum;
		int duplResult = checkDuplicate(
			usageInfo,
			processRecord,
			possibleResubmission,
			previousJobId,
			uniqueChecksum);
		if ( duplResult != 0 )
		{
			//setting possibleResubmission is pleonastic
			//possibleResubmission = false;
			returnCode = duplResult;
			//it seems the record is duplicated...
		}
		/*t.exists() always false: pleonastic
		if ( possibleResubmission && t.exists() && processRecord )
		{
			hlr_log ("ATM Engine: Warning: record dgJobId already present, possible resubmission.", &logStream,3);
		}
		*/
		//processRecord is set according to result of checkDuplicate.
		if ( processRecord )
		{
			t.uniqueChecksum = uniqueChecksum;
			t.accountingProcedure=recordsBuff.accountingProcedure;
			if ( t.process() != 0 )
        	        {
				string logBuff = "ERROR: dgas_bankClient: E_DEBIT_ERROR_B";
				hlr_log( logBuff, &logStream ,1);
				returnCode = atoi(E_DEBIT_ERROR_B);
			}
			else
			{
				string logBuff = "Inserted trans_in with tid:" + int2string(t.tid);
				hlr_log( logBuff, &logStream ,8);
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
