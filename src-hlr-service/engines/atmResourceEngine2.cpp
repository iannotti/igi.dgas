// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: atmResourceEngine2.cpp,v 1.1.2.1.4.2 2010/12/13 10:18:36 aguarise Exp $
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
#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/stringSplit.h"

#include "glite/dgas/hlr-service/base/hlrTransaction.h"
#include "glite/dgas/hlr-service/base/qTransaction.h"
#include "glite/dgas/hlr-service/base/hlrResource.h"

#include "glite/dgas/hlr-service/engines/atmResourceEngine2.h"



extern ofstream logStream;
extern bool strictAccountCheck;

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_sql_dbname;

namespace ATMResource{

inline int urlSplit(char delim, string url_string , url_type *url_buff)
{
        size_t pos = 0;
        pos = url_string.find_first_of( delim, 0);
        url_buff->hostname=url_string.substr(0,pos);
        url_buff->port=atoi((url_string.substr(pos+1,url_string.size()).c_str()));
				                
        return 0;
}

int ATMResource_parse_xml (string &doc, map<string,string> &fieldsValues)
{
	string logBuff;
	node tagBuff;
	tagBuff = parse(&doc, "DG_JOBID");
        if ( tagBuff.status == 0 )
        {
        	fieldsValues["DG_JOBID"] = tagBuff.text;
		logBuff = "ATMengine2: found DG_JOBID=" + tagBuff.text;
		hlr_log(logBuff, &logStream, 7);
        }
	tagBuff = parse(&doc, "EDG_ECONOMIC_ACCOUNTING");
        if ( tagBuff.status == 0 )
        {
        	fieldsValues["EDG_ECONOMIC_ACCOUNTING"] = tagBuff.text;
		logBuff = "ATMengine2: found EDG_ECONOMIC_ACCOUNTING=" + tagBuff.text;
		hlr_log(logBuff, &logStream, 7);
        }
	tagBuff = parse(&doc, "SUBMISSION_TIME");
        if ( tagBuff.status == 0 )
        {
        	fieldsValues["SUBMISSION_TIME"] = tagBuff.text;
		logBuff = "ATMengine2: found SUBMISSION_TIME=" + tagBuff.text;
		hlr_log(logBuff, &logStream, 7);
        }
	tagBuff = parse(&doc, "RES_ACCT_PA_ID");
	if ( tagBuff.status == 0 )
	{
        	fieldsValues["RES_ACCT_PA_ID"] = tagBuff.text;
		logBuff = "ATMengine2: found RES_ACCT_PA_ID=" + tagBuff.text;
		hlr_log(logBuff, &logStream, 7);
	}
	tagBuff = parse(&doc, "RES_ACCT_BANK_ID");
	if ( tagBuff.status == 0 )
	{
        	fieldsValues["RES_ACCT_BANK_ID"] = tagBuff.text;
		logBuff = "ATMengine2: found RES_ACCT_BANK_ID=" + tagBuff.text;
		hlr_log(logBuff, &logStream, 7);
	}
	tagBuff = parse(&doc, "USER_CERT_SUBJECT");
	if ( tagBuff.status == 0 )
	{
        	fieldsValues["USER_CERT_SUBJECT"] = tagBuff.text;
		logBuff = "ATMengine2: found USER_CERT_SUBJECT=" + tagBuff.text;
		hlr_log(logBuff, &logStream, 7);
	}
        tagBuff = parse(&doc, "RES_GRID_ID");
	if ( tagBuff.status == 0 )
	{
        	fieldsValues["RES_GRID_ID"] = tagBuff.text;
		logBuff = "ATMengine2: found RES_GRID_ID=" + tagBuff.text;
		hlr_log(logBuff, &logStream, 7);
	}
	while ( 1 )
        {
                node nodeBuff = parse(&doc, "AdditionalUR");
                if ( nodeBuff.status != 0 )
                        break;
                bool goOn = true;
		while ( goOn )
		{
			node jobInfoNode;
			jobInfoNode = parse(&nodeBuff.text, "dgas:item");
			if ( jobInfoNode.status == 0 )
			{
				attrType attributes;
				attributes = jobInfoNode.getAttributes();
				map<string,string>::iterator it = attributes.begin();
				string key= (*it).first;
				string value = (*it).second;
				if ( key != "" )
				{
					logBuff = "ATMengine2: found " + key + "=" + value;
					fieldsValues.insert(pair<string,string>(key,value));
					hlr_log(logBuff, &logStream, 7);
				}
				jobInfoNode.release();
			}
			else
			{
				goOn =false;
			}
		}
		nodeBuff.release();
	}
	//now check that the record has enough information to be useful.
	if ( !fieldsValues.count("RES_GRID_ID") ) return 1;
	if ( !fieldsValues.count("DG_JOBID") ) return 1;
	return 0;
}//ATM_parse_xml (string &doc, ATM_job_data *job_data, ATM_usage_info *usage_info)

string composeLogData(map<string,string>  &fieldsValues)
{

	string urBuff;
	//string newVoSource;
	//string newVo = checkUserVo(usage_info.userVo, 
	//	usage_info.userFqan, 
	//	usage_info.localUserId, 
	//	newVoSource );//vo is determined at sensors level. It may be possible
	//to fix it a-posteriori on translateDb, but data coming from sensors should be
	//inserted as they are (a part workarounds for known bugs 
	//on underliyng components such as LRMS)		
	
	typedef map<string,string> fv;	
	fv::iterator it = fieldsValues.begin(),
		iter_end = fieldsValues.end();
	while ( it != iter_end )
	{
		urBuff += (*it).first + "=" + (*it).second;
		it++;
		if ( it != iter_end ) urBuff += ",";
	} 
	/*
	//workaround for PBS bug with walltime < 0 on exit_status= -4
	if ( usage_info.wall_time < 0 ) usage_info.wall_time = 0;
        urBuff = "WALL_TIME=" + int2string(usage_info.wall_time);
	if ( usage_info.cpu_time < 0 ) usage_info.cpu_time = 0;
        urBuff += ",CPU_TIME=" + int2string(usage_info.cpu_time);
        urBuff += ",CE_ID=" + usage_info.res_grid_id;
        urBuff += ",CE_PRICE_TIME=" + int2string(usage_info.time);
        urBuff += ",MEM=" + usage_info.mem;
        urBuff += ",VMEM=" + usage_info.vmem;
	urBuff += ",processors=" +usage_info.processors;
	urBuff += ",urCreation=" +usage_info.urCreation;
	if ( usage_info.lrmsId != "" )
		urBuff += ",lrmsId=" +usage_info.lrmsId;
	if ( usage_info.lrmsQueue != "" )
		urBuff += ",lrmsQueue=" +usage_info.lrmsQueue;
	if ( usage_info.localUserId != "" )
		urBuff += ",localUserId=" + usage_info.localUserId;
	if ( usage_info.jobName != "" )
		urBuff += ",jobName=" + usage_info.jobName;
	if ( usage_info.localGroup != "" )
		urBuff += ",localGroup=" + usage_info.localGroup;
	if ( usage_info.execHost != "" )
		urBuff += ",execHost=" + usage_info.execHost;
	if ( usage_info.userFqan != "" )
		urBuff += ",userFqan=" + usage_info.userFqan;
	if ( usage_info.start != "" )
		urBuff += ",start=" + usage_info.start;
	if ( usage_info.end != "" )
		urBuff += ",end=" + usage_info.end;
	if ( usage_info.ctime != "" )
		urBuff += ",ctime=" + usage_info.ctime;
	if ( usage_info.qtime != "" )
		urBuff += ",qtime=" + usage_info.qtime;
	if ( usage_info.etime != "" )
		urBuff += ",etime=" + usage_info.etime;
	if ( usage_info.iBench != "" )
		urBuff += ",iBench=" + usage_info.iBench;
	if ( usage_info.iBenchType != "" )
		urBuff += ",iBenchType=" + usage_info.iBenchType;
	if ( usage_info.fBench != "" )
		urBuff += ",fBench=" + usage_info.fBench;
	if ( usage_info.fBenchType != "" )
		urBuff += ",fBenchType=" + usage_info.fBenchType;
	if ( usage_info.tz != "" )
		urBuff += ",timeZone=" + usage_info.tz;
	if ( usage_info.economicAccountingFlag != "" )
		urBuff += ",economicAccounting=" + usage_info.economicAccountingFlag;
	if ( usage_info.res_acct_PA_id != "" )
		urBuff += ",priceAuthority=" + usage_info.res_acct_PA_id;
	if ( usage_info.accountingProcedure != "" )
		urBuff += ",accountingProcedure=" + usage_info.accountingProcedure;
	if ( usage_info.atmClientVersion != "" )
	        urBuff += ",atmClientVersion=" + usage_info.atmClientVersion;
	if ( usage_info.userVo != "" )
                urBuff += ",userVo=" + usage_info.userVo;
	if ( usage_info.voOrigin != "" )
		urBuff += ",voOrigin=" + usage_info.voOrigin;
	if ( usage_info.ceHostName != "" )
                urBuff += ",ceHostName=" + usage_info.ceHostName;
	if ( usage_info.siteName != "" )
                urBuff += ",SiteName=" + usage_info.siteName;
	if ( usage_info.execCe != "" )
		urBuff += ",execCe=" + usage_info.execCe;
	if ( usage_info.submitHost != "" )
		urBuff += ",submitHost=" + usage_info.submitHost;
	if ( usage_info.lrmsServer != "" )
		urBuff += ",lrmsServer=" + usage_info.lrmsServer;
	if ( usage_info.glueCEInfoTotalCPUs != "" )
		urBuff += ",glueCEInfoTotalCPUs=" + usage_info.glueCEInfoTotalCPUs;
*/
	urBuff += ",atmEngineVersion=";
	urBuff += ATM_RESOURCE_ENGINE_VERSION2;
	urBuff += VERSION;
	return urBuff;
}

int ATM_compose_xml(map<string,string> &fieldsValues ,string status_msg, string *output)
{
	*output = "<HLR type=\"ATM_answer\">\n";
	*output += "<BODY>\n";
	typedef map<string,string> fv;	
	fv::iterator it = fieldsValues.begin(),
		iter_end = fieldsValues.end();
	while ( it != iter_end )
	{
		*output += "<dgas:item ";
		*output += (*it).first + "=\"" + (*it).second;
		*output += "\"\\>\n";
		it++;
	} 
	*output += status_msg;
	*output += "</BODY>\n";
	*output += "</HLR>\n";
	return 0;
}//ATM_compose_xml()

//get the xml object from the daemon, parse the information 
int ATMResourceEngine2( string &input, connInfo &connectionInfo, string *output )
{
	hlr_log ("ATM to Resource Engine v2: Entering.", &logStream,4);
	hlrError e;
	bool success = true;
	int code = 0;
	//Get info concerning the job from the incoming request originating 
	//from the sensor
	string uniqueChecksum;
	string accountingProcedure;
	//ATM_job_record usage_info;
	map<string,string> fieldsValues;
	if ( ATMResource_parse_xml(input, fieldsValues) != 0 )
	{
		//something went wrong parsing the input DGASML
		 hlr_log ("ATM Engine: Error parsing the XML, reporting error.", &logStream,1);
		 code = atoi(E_PARSE_ERROR);
		 success = false;
	} 
	else
	{
		hlrResource r;
		success = r.exists("resource",connectionInfo.contactString);
		if ( !success )
		{
			hlr_log ("ATM Engine: Operation not allowed, certificate DN isn't asociated to a valid resource in DB", &logStream,2);
			code = atoi(E_STRICT_AUTH);
		}
		else
		{
			if ( strictAccountCheck )
			{
				hlr_log ("ATM Engine: strictAccountCheck", &logStream,4);
				hlrResource rBuff;
        		        rBuff.ceId=fieldsValues["RES_GRID_ID"];
   			       	success = rBuff.exists();
				if ( !success )
				{
					code = atoi(E_STRICT_AUTH);
				}
			}
		}
	}
	hlrTransaction t;
	string urBuff;
	bool possibleResubmission = true;
	if ( success )
	{
		 hlr_log ("ATMEngine: Processing record", &logStream,6);
		 time_t currTime;
		 time (&currTime);
		 if ( !fieldsValues.count("end") )
		 {
			//if job end time is not available use current time!
			fieldsValues["end"] = int2string(currTime);
		 }
	  	string logBuff ="ATMEngine: gridJobId: " + fieldsValues["DG_JOBID"];
		hlr_log ( logBuff, &logStream,4);
		//check for duplicated transaction
		string previousJobId;
		urBuff = composeLogData(fieldsValues);
		code = checkDuplicate(fieldsValues,
			success,
			possibleResubmission, 
			previousJobId,
			uniqueChecksum);
		if ( code != 0 ) possibleResubmission = false;//duplicated, so not resubmission.
		t.id = fieldsValues["DG_JOBID"];
		if ( previousJobId != "" )
		{
			urBuff += ",dg_jobid_subst=" + previousJobId;
		} 
	}
	//Resubmission check below
	if ( possibleResubmission && t.exists() && success )
	{
		 hlr_log ("ATMEngine: Warning: record DG_JOBID already present, possible resubmission.", &logStream,3);
		if ( t.get() != 0 )
		{
			hlr_log ("ATMEngine: Error retrieving record", &logStream,1);
			code = atoi(ATM_E_DUPLICATED_C);//72
			success = false;
		}
		else
		{	
			db hlrDb ( hlr_sql_server,
				hlr_sql_user,
				hlr_sql_password,
				hlr_sql_dbname
				);
			if ( hlrDb.errNo == 0 )
			{
				string dgJobIdBuff = fieldsValues["DG_JOBID"];
				size_t pos = dgJobIdBuff.find("_");
				if ( pos != string::npos )
				{
					//there's an unwanted wildchar inside the dgJobId. quote it
					while ( pos != string::npos )
					{
						dgJobIdBuff.insert(pos,"\\");
						pos = dgJobIdBuff.find("_",pos+2);
					}
				}

				string queryString = "SELECT * FROM trans_in WHERE dgJobId LIKE '";
				queryString += dgJobIdBuff + "%' ";
				hlr_log (queryString, &logStream,9);
				dbResult result = hlrDb.query(queryString);
				if ( hlrDb.errNo == 0)
				{
					if ( result.numRows() != 0 )
					{
						//real resub
						hlr_log ("ATM Engine: got accounting request for resubmission", &logStream,4);
						string logBuff = "ATM Engine:"+ t.id + "," + fieldsValues["DG_JOBID"] + "/";
						logBuff += t.logData + "," + urBuff;
						hlr_log (logBuff, &logStream,4);
						logBuff = "ATM Engine: got keys:"+ int2string(result.numRows());
						hlr_log (logBuff, &logStream,5);
						fieldsValues["DG_JOBID"] = fieldsValues["DG_JOBID"] + "/" + int2string(result.numRows());
						success = true;
					}
				}
				else
				{
					hlr_log ("ATM Engine: Error in query!", &logStream,3);
					success = false;
					code = atoi(INFO_NOT_FOUND);
				}
			}
			else
			{
				success = false;
				hlr_log ("ATM Engine: Error opening DB!", &logStream,1);
				code = atoi(E_NO_DB);
			}
		}
	}
	//end of resubmission check.
	if ( success )
	{
		fieldsValues["uniqueChecksum"] = uniqueChecksum;
		string logBuff ="Inserting: " +
			fieldsValues["DG_JOBID"] + "," + 
			fieldsValues["USER_CERT_SUBJECT"]+ "," +
			fieldsValues["RES_GRID_ID"] + "," +
			fieldsValues["RES_ACCT_BANK_ID"]+","+
			fieldsValues["uniqueChecksum"]+","+
			fieldsValues["accountingProcedure"];
			hlr_log (logBuff, &logStream,6);
		 //put the transactuion in the queue;
		hlr_log ( urBuff, &logStream,8);
		qTransaction qt( fieldsValues["DG_JOBID"],
				fieldsValues["USER_CERT_SUBJECT"],
				fieldsValues["RES_GRID_ID"],
				fieldsValues["RES_ACCT_BANK_ID"],
				int2string(time(NULL)),
				urBuff,
				0,
				time(NULL),
				fieldsValues["uniqueChecksum"],
				fieldsValues["accountingProcedure"]
			       );
		int qtRes = qt.put();
		if ( qtRes != 0 )
		{
			success = false;
			if ( qtRes == atoi(E_NO_DB) )
			{
				code = atoi(ATM_E_TRANS_B);
			}
			else
			{
				code = atoi(ATM_E_TRANS_C);
				//error inseting the entry.
			}
		}
	}
	string message;
        if (success)
        {
		message += "<dgas:info status=\"ok\"\\>\n";
        }
	else
	{
		message += "<dgas:info status=\"failed\"\\>\n";
		message += "<errMsg>";
        	message += e.error[int2string(code)];
        	message += "</errMsg>";
	}
	message += "<CODE>\n";
	message += int2string(code);
	message += "\n</CODE>\n";
        if ( ATM_compose_xml(fieldsValues, message, output) != 0 )
        {
	         hlr_log ( "ATM_engine: Error composing the XML answer!",&logStream,3);
        }
	hlr_log ("ATM Engine: Exiting.", &logStream,4);	
	return code;
} //ATMEngine( string doc, connInfo &connectionInfo, string *output )


}; // namespace
