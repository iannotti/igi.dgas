// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: engineCmnUtl.cpp,v 1.1.2.1.4.1 2010/10/19 09:11:04 aguarise Exp $
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
#include "serviceCommonUtl.h"
#include "glite/dgas/hlr-service/engines/engineCmnUtl.h"

extern ofstream logStream; 


int cmnParseLog(string logString, cmnLogRecords& records)
{
	vector<string> buffV;
	Split (',',logString, &buffV );
	vector<string>::const_iterator it = buffV.begin();
	map<string,string> logMap;
	while ( it != buffV.end() )
	{
		size_t pos = (*it).find_first_of("=");
		if ( pos != string::npos )
		{
			string param = (*it).substr(0,pos);
			string value = (*it).substr(pos+1);
			logMap.insert(
					map<string,string>::
					value_type (param,value)
					);
		}
		it++;
	}
	records.cpuTime = atoi(logMap["CPU_TIME"].c_str());
	records.wallTime = atoi(logMap["WALL_TIME"].c_str());
	records.ceId = logMap["CE_ID"];
	records.userVo = logMap["userVo"];
	records.localUserId = logMap["localUserId"];
	records.lrmsId = logMap["lrmsId"];
	records.jobName = logMap["jobName"];
	records.accountingProcedure = logMap["accountingProcedure"];
	records.start = logMap["start"];
	records.end = logMap["end"];
	records.userFqan = logMap["userFqan"];
	records.priceAuthority = logMap["priceAuthority"];
	records.economicAccounting = logMap["economicAccounting"];
	return 0;
}

int cmnParseLog(string logString, map<string,string>& logMap)
{
	vector<string> buffV;
	Split (',',logString, &buffV );
	vector<string>::const_iterator it = buffV.begin();
	while ( it != buffV.end() )
	{
		size_t pos = (*it).find_first_of("=");
		if ( pos != string::npos )
		{
			string param = (*it).substr(0,pos);
			string value = (*it).substr(pos+1);
			logMap.insert(
					map<string,string>::
					value_type (param,value)
					);
		}
		it++;
	}
	return 0;
}



void makeUniqueChecksum(ATM_job_record& usage_info, string& uniqueChecksum)
{
	string valuesBuffer = "";
	vector<string> ceIdBuff;
        Split (':', usage_info.res_grid_id, &ceIdBuff);
	if (ceIdBuff.size() > 0)
	{
		valuesBuffer  = ceIdBuff[0];//FIXME can't we finde something else?
	}
        valuesBuffer += usage_info.lrmsId;
        valuesBuffer += usage_info.start;
        valuesBuffer += int2string(usage_info.wall_time);
        valuesBuffer += int2string(usage_info.cpu_time);
	hlr_log ( valuesBuffer, &logStream,9);
        makeUniqueString (valuesBuffer,uniqueChecksum);
	hlr_log ( uniqueChecksum, &logStream,9);
	return;
}

void makeUniqueChecksum(map<string,string>& usageMap, string& uniqueChecksum)
{
	string valuesBuffer = "";
	vector<string> ceIdBuff;
        Split (':', usageMap["RES_GRID_ID"], &ceIdBuff);
	if (ceIdBuff.size() > 0)
	{
		valuesBuffer  = ceIdBuff[0];//FIXME can't we finde something else?
	}
        valuesBuffer += usageMap["LRMSID"];
        valuesBuffer += usageMap["start"];
        valuesBuffer += usageMap["WALL_TIME"];
        valuesBuffer += usageMap["CPU_TIME"];
	hlr_log ( valuesBuffer, &logStream,9);
        makeUniqueString (valuesBuffer,uniqueChecksum);
	hlr_log ( uniqueChecksum, &logStream,9);
	return;
}

int checkDuplicate(ATM_job_record& usage_info,bool& success,bool& possibleResubmission, string& previousJobId, string& uniqueChecksum)
{
	int code = 0;
	// check if this job was already accounted as outOfBand:
	makeUniqueChecksum(usage_info, uniqueChecksum);
	string logBuff ="checkDuplicate:dgJobId=" + usage_info.dgJobId + ",uniqueChecksum=" + uniqueChecksum + ",accountingProcedure=" + usage_info.accountingProcedure;
	hlr_log ( logBuff, &logStream,6);
	previousJobId = "";
	string previousAccountingProcedure;
	int res = getJobId( uniqueChecksum, 
			previousJobId,
			previousAccountingProcedure);
	  logBuff = "checkDuplicate:getJobId exit==" + int2string(res);
	  hlr_log ( logBuff, &logStream, 8);
	  if ( res == 0 )
	  {
		//found a record with the very same unique checksum
		logBuff ="checkDuplicate: record with dgJobId=" + usage_info.dgJobId + " already present with previousDgJobId=" + previousJobId + ",previousAccountingProcedure=" + previousAccountingProcedure;
		hlr_log ( logBuff, &logStream,4);
		// there is such a transaction, check wether the new one
		// is outOfBand or not:
		if ( usage_info.accountingProcedure == "" )
		{
			// record is duplicated and accountingProcedure not declared: IGNORING
		        logBuff ="checkDuplicate: New record hasn't accountingProcedure declared; ignoring!";
			hlr_log ( logBuff, &logStream,4);
			code = atoi(ATM_E_DUPLICATED_A);//70
			success = false;
			
		}
		else if (usage_info.accountingProcedure == "outOfBand")
		{
			// the new transaction is out of band, do NOT replace
		        logBuff ="checkDuplicate: New record has accountingProcedure=outOfBand; ignoring!";
			hlr_log ( logBuff, &logStream,4);
			code = atoi(ATM_E_DUPLICATED_A);//70
			success = false;
		}
		//else if ( (previousJobId == usage_info.dgJobId ) &&
			//	(previousAccountingProcedure != "outOfBand" ))
		else if ((previousAccountingProcedure != "outOfBand" ))
		{
		        logBuff ="checkDuplicate: Checking for jobId match";
			hlr_log ( logBuff, &logStream,5);
		        // here we test following cases:
			//dgJobId="https://host:port/hash" == previousJobId="https://host:port/hash"
			// as well as:
			//dgJobId="https://host:port/hash" == previousJobId="https://host:port/hash/d"
			//(duplicate over resubmission)
			size_t pos = previousJobId.find(usage_info.dgJobId);
			if ( pos != string::npos )
			{
		        	logBuff ="checkDuplicate: New record has the same dgJobId and uniqueChecksum; ignoring!";
				hlr_log ( logBuff, &logStream,4);
				code = atoi(ATM_E_DUPLICATED_B);//71
				success = false;
			}
		}
		else if (previousAccountingProcedure == "outOfBand")
		{
		        logBuff ="checkDuplicate: ... previous record is outOfBand; declaring previous transaction entry as obsolete!";
			hlr_log ( logBuff, &logStream,4);
			// and declaring old transaction entry obsolete:
			if (makeObsolete(previousJobId) != 0)
			{
			         logBuff ="checkDuplicate: ERROR while declaring previous transaction entry as obsolete!";
				 hlr_log ( logBuff, &logStream,1);
				 code = atoi(ATM_E_TRANS_A);
				 success = false;
			}
		}

	}
	return code;
}




int checkDuplicate(map<string,string>& usageMap,bool& success,bool& possibleResubmission, string& previousJobId, string& uniqueChecksum)
{
	int code = 0;
	// check if this job was already accounted as outOfBand:
	makeUniqueChecksum(usageMap, uniqueChecksum);
	string logBuff ="checkDuplicate:dgJobId=" + usageMap["DG_JOBID"] + ",uniqueChecksum=" + uniqueChecksum + ",accountingProcedure=" + usageMap["accountingProcedure"];
	hlr_log ( logBuff, &logStream,6);
	previousJobId = "";
	string previousAccountingProcedure;
	int res = getJobId( uniqueChecksum, 
			previousJobId,
			previousAccountingProcedure);
	  logBuff = "checkDuplicate:getJobId exit==" + int2string(res);
	  hlr_log ( logBuff, &logStream, 8);
	  if ( res == 0 )
	  {
		//found a record with the very same unique checksum
		logBuff ="checkDuplicate: record with dgJobId=" + usageMap["DG_JOBID"] + " already present with previousDgJobId=" + previousJobId + ",previousAccountingProcedure=" + previousAccountingProcedure;
		hlr_log ( logBuff, &logStream,4);
		// there is such a transaction, check wether the new one
		// is outOfBand or not:
		if ( (usageMap.count("accountingProcedure") == 0) || usageMap["accountingProcedure"] == "" )
		{
			// record is duplicated and accountingProcedure not declared: IGNORING
		        logBuff ="checkDuplicate: New record hasn't accountingProcedure declared; ignoring!";
			hlr_log ( logBuff, &logStream,4);
			code = atoi(ATM_E_DUPLICATED_A);//70
			success = false;
			
		}
		else if (usageMap["accountingProcedure"] == "outOfBand")
		{
			// the new transaction is out of band, do NOT replace
		        logBuff ="checkDuplicate: New record has accountingProcedure=outOfBand; ignoring!";
			hlr_log ( logBuff, &logStream,4);
			code = atoi(ATM_E_DUPLICATED_A);//70
			success = false;
		}
		//else if ( (previousJobId == usage_info.dgJobId ) &&
			//	(previousAccountingProcedure != "outOfBand" ))
		else if ((previousAccountingProcedure != "outOfBand" ))
		{
		        logBuff ="checkDuplicate: Checking for jobId match";
			hlr_log ( logBuff, &logStream,5);
		        // here we test following cases:
			//dgJobId="https://host:port/hash" == previousJobId="https://host:port/hash"
			// as well as:
			//dgJobId="https://host:port/hash" == previousJobId="https://host:port/hash/d"
			//(duplicate over resubmission)
			size_t pos = previousJobId.find(usageMap["DG_JOBID"]);
			if ( pos != string::npos )
			{
		        	logBuff ="checkDuplicate: New record has the same DG_JOBID and uniqueChecksum; ignoring!";
				hlr_log ( logBuff, &logStream,4);
				code = atoi(ATM_E_DUPLICATED_B);//71
				success = false;
			}
		}
		else if (previousAccountingProcedure == "outOfBand")
		{
		        logBuff ="checkDuplicate: ... previous record is outOfBand; declaring previous transaction entry as obsolete!";
			hlr_log ( logBuff, &logStream,4);
			// and declaring old transaction entry obsolete:
			if (makeObsolete(previousJobId) != 0)
			{
			         logBuff ="checkDuplicate: ERROR while declaring previous transaction entry as obsolete!";
				 hlr_log ( logBuff, &logStream,1);
				 code = atoi(ATM_E_TRANS_A);
				 success = false;
			}
		}

	}
	return code;
}
