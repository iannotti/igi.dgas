// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: atmResourceEngine.cpp,v 1.1.2.1.4.7 2012/06/21 08:48:59 aguarise Exp $
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
#include "glite/dgas/hlr-service/engines/atmResourceEngine.h"

#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/hlr-service/base/hlrTransaction.h"
#include "glite/dgas/hlr-service/base/qTransaction.h"
#include "glite/dgas/hlr-service/base/hlrResource.h"

extern ofstream logStream;
extern bool strictAccountCheck;
extern bool lazyAccountCheck;

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

bool authorize(string& contactString)
{
	roles rolesBuff(contactString, "recordSource" );
	return rolesBuff.exists();
}

int ATMResource_parse_xml (string &doc, ATM_job_record *usage_info)
{
	node nodeBuff;
	while ( nodeBuff.status == 0 )
	{
		string tag = "JOB_PAYMENT";
		nodeBuff = parse(&doc, tag);
		if ( nodeBuff.status != 0 )
			break;
		node tagBuff;
		tagBuff = parse(&doc, "EDG_ECONOMIC_ACCOUNTING");
		if ( tagBuff.status == 0 )
		{
			usage_info->economicAccountingFlag = tagBuff.text;
		}
		tagBuff = parse(&doc, "DG_JOBID");
		if ( tagBuff.status == 0 )
		{
			usage_info->dgJobId = tagBuff.text;
		}
		tagBuff = parse(&doc, "SUBMISSION_TIME");
		if ( tagBuff.status == 0 )
		{
			usage_info->time = atoi((tagBuff.text).c_str());
		}
		tagBuff = parse(&doc, "RES_ACCT_PA_ID");
		if ( tagBuff.status == 0 )
		{
			usage_info->res_acct_PA_id = tagBuff.text;
		}
		tagBuff = parse(&doc, "RES_ACCT_BANK_ID");
		if ( tagBuff.status == 0 )
		{
			usage_info->res_acct_bank_id = tagBuff.text;
		}
		tagBuff = parse(&doc, "USER_CERT_SUBJECT");
		if ( tagBuff.status == 0 )
		{
			usage_info->user_CertSubject = tagBuff.text;
		}
		tagBuff = parse(&doc, "RES_GRID_ID");
		if ( tagBuff.status == 0 )
		{
			usage_info->res_grid_id = tagBuff.text;
		}
		usage_info->forceResourceHlrOnly = "yes";
		tagBuff = parse(&doc, "JOB_INFO");
		if ( tagBuff.status == 0 )
		{
			node jobInfoNode;  
			jobInfoNode = parse(&tagBuff.text, "CPU_TIME" );
			if ( jobInfoNode.status == 0)
			{
				usage_info->cpu_time = atoi((jobInfoNode.text).c_str());
			}
			jobInfoNode = parse(&tagBuff.text, "WALL_TIME" );
			if ( jobInfoNode.status == 0)
			{
				usage_info->wall_time = atoi((jobInfoNode.text).c_str());
			}
			jobInfoNode = parse(&tagBuff.text, "MEM" );
			if ( jobInfoNode.status == 0)
			{
				usage_info->mem = jobInfoNode.text;
			}
			jobInfoNode = parse(&tagBuff.text, "VMEM" );
			if ( jobInfoNode.status == 0)
			{
				usage_info->vmem = jobInfoNode.text;
			}
		}
		nodeBuff.release();
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
			string logBuff = "";
			if ( jobInfoNode.status == 0 )
			{
				attrType attributes;
				attributes = jobInfoNode.getAttributes();
				string buffer = "";
				buffer =
						parseAttribute ("USER", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found USER=" + buffer;
					usage_info->localUserId = buffer;	
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("PROCESSORS", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found PROCESSORS=" + buffer;
					usage_info->processors = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("URCREATION", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found URCREATION=" + buffer;
					usage_info->urCreation = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("jobName", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found jobName=" + buffer;
					usage_info->jobName = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("group", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found group=" + buffer;
					usage_info->localGroup = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("execHost", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found execHost=" + buffer;
					usage_info->execHost = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("SiteName", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found SiteName=" + buffer;
					usage_info->siteName = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("LRMSID", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found lrmsId=" + buffer;
					usage_info->lrmsId = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("start", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found start=" + buffer;
					usage_info->start = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("end", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found end=" + buffer;
					usage_info->end = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("ctime", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found ctime=" + buffer;
					usage_info->ctime = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("qtime", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found qtime=" + buffer;
					usage_info->qtime = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("etime", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found etime=" + buffer;
					usage_info->etime = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("fqan", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found fqan=" + buffer;
					usage_info->userFqan = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("si2k", attributes);
				if ( buffer != "")//backwardCompatibility
				{
					logBuff = "ATMengine: found si2k=" + buffer;
					usage_info->iBench = buffer;
					usage_info->iBenchType = "si2k";
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("sf2k", attributes);
				if ( buffer != "")//backwardCompatibility
				{
					logBuff = "ATMengine: found sf2k=" + buffer;
					usage_info->fBench = buffer;
					usage_info->fBenchType = "sf2k";
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("iBench", attributes);
				if ( buffer != "")//backwardCompatibility
				{
					logBuff = "ATMengine: found iBench=" + buffer;
					usage_info->iBench = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("fBench", attributes);
				if ( buffer != "")//backwardCompatibility
				{
					logBuff = "ATMengine: found fBench=" + buffer;
					usage_info->fBench = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("iBenchType", attributes);
				if ( buffer != "")//backwardCompatibility
				{
					logBuff = "ATMengine: found iBenchType=" + buffer;
					usage_info->iBenchType = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("fBenchType", attributes);
				if ( buffer != "")//backwardCompatibility
				{
					logBuff = "ATMengine: found fBenchType=" + buffer;
					usage_info->fBenchType = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("tz", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found tz=" + buffer;
					usage_info->tz = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("accountingProcedure", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found accountingProcedure=" + buffer;
					usage_info->accountingProcedure = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("atmClientVersion", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found atmClientVersion=" + buffer;
					usage_info->atmClientVersion = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("userVo", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found userVo=" + buffer;
					usage_info->userVo = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("ceHostName", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found ceHostName=" + buffer;
					usage_info->ceHostName = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer = 
						parseAttribute ("ceCertificateSubject", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found ceCertificateSubject=" + buffer;
					usage_info->ceCertificateSubject = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer =
						parseAttribute ("execCe", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found execCe=" + buffer;
					usage_info->execCe = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer =
						parseAttribute ("submitHost", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found submitHost=" + buffer;
					usage_info->submitHost = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer =
						parseAttribute ("lrmsServer", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found lrmsServer=" + buffer;
					usage_info->lrmsServer = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer =
						parseAttribute ("voOrigin", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found voOrigin=" + buffer;
					usage_info->voOrigin = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				buffer =
						parseAttribute ("GlueCEInfoTotalCPUs", attributes);
				if ( buffer != "")
				{
					logBuff = "ATMengine: found GlueCEInfoTotalCPUs=" + buffer;
					usage_info->glueCEInfoTotalCPUs = buffer;
					hlr_log(logBuff, &logStream, 7);
				}
				jobInfoNode.release();
			}
			else
			{
				goOn = false;
			}
		}
		nodeBuff.release();
	}
	return 0;

}//ATM_parse_xml (string &doc, ATM_job_data *job_data, ATM_usage_info *usage_info)

string composeLogData(ATM_job_record  &usage_info)
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
	if ( usage_info.ceCertificateSubject != "" )
		urBuff += ",ceCertificateSubject=" + usage_info.ceCertificateSubject;
	urBuff += ",atmEngineVersion=";
	urBuff += ATM_RESOURCE_ENGINE_VERSION;
	urBuff += VERSION;
	return urBuff;
}

int ATM_compose_xml(ATM_job_record &usage_info ,string status_msg, string *output)
{
	*output = "<HLR type=\"ATM_answer\">\n";
	*output += "<BODY>\n";
	*output += "<dgas:item dgJobId=\"" + usage_info.dgJobId + "\"\\>\n";
	*output += "<dgas:item submissionTime=\"" + int2string(usage_info.time) + "\"\\>\n";
	*output += "<dgas:item lrmsId=\"" + usage_info.lrmsId + "\"\\>\n";
	*output += "<dgas:item resAcctPAId=\"" + usage_info.res_acct_PA_id + "\"\\>\n";
	*output += "<dgas:item userGridId=\"" + usage_info.user_CertSubject + "\"\\>\n";
	*output += "<dgas:item resourceGridId=\"" + usage_info.res_grid_id + "\"\\>\n";
	*output += "<dgas:item accountingProcedure=\"" + usage_info.accountingProcedure + "\"\\>\n";
	*output += "<dgas:item uniqueChecksum=\"" + usage_info.uniqueChecksum + "\"\\>\n";
	*output += status_msg;
	*output += "</BODY>\n";
	*output += "</HLR>\n";
	return 0;
}//ATM_compose_xml()

//get the xml object from the daemon, parse the information 
int ATMResourceEngine( string &input, connInfo &connectionInfo, string *output )
{
	hlr_log ("ATM to Resource Engine: Entering.", &logStream,4);
	hlrError e;
	bool success = true;
	int code = 0;
	//Get info concerning the job from the incoming request originating 
	//from the sensor
	string uniqueChecksum;
	string accountingProcedure;
	ATM_job_record usage_info;
	if ( ATMResource_parse_xml(input, &usage_info) != 0 )
	{
		//something went wrong parsing the input DGASML
		hlr_log ("ATM Engine: Error parsing the XML, reporting error.", &logStream,1);
		code = atoi(E_PARSE_ERROR);
		success = false;
	} 
	else
	{
		hlr_log(usage_info.ceCertificateSubject,&logStream,9);
		hlr_log(usage_info.res_grid_id,&logStream,9);
		hlr_log(usage_info.execCe,&logStream,9);
		hlr_log(usage_info.dgJobId,&logStream,9);
		hlrResource r;
		if ( connectionInfo.contactString == "")
		{
			//this is important for AMQ based records
			string logBuff = "using ceCertificateSubject from UR in connInfo" + usage_info.ceCertificateSubject;
			hlr_log (logBuff,&logStream,4);
			connectionInfo.contactString = usage_info.ceCertificateSubject;
		}
		if ( !lazyAccountCheck )
		{
			success = r.exists("resource",connectionInfo.contactString);
			if ( !success )
			{
				hlr_log ("ATM Engine: Operation not allowed, certificate DN isn't associated to a valid resource in DB", &logStream,2);
				code = atoi(E_STRICT_AUTH);
			}
			else
			{
				if ( strictAccountCheck )
				{
					hlr_log ("ATM Engine: strictAccountCheck", &logStream,4);
					hlrResource rBuff;
					rBuff.ceId=usage_info.res_grid_id;
					success = rBuff.exists();
					if ( !success )
					{
						code = atoi(E_STRICT_AUTH);
					}
				}
			}
		}
		else
		{
			//DN check here
			if ( !authorize(connectionInfo.contactString) )
			{
				if (!authorize(connectionInfo.hostName) )
				{
					hlr_log ("ATM Engine: Operation not allowed, certificate DN or hostname not authorized as record source.", &logStream,2);
					success = false;
					code = atoi(ATM_E_AUTH);
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
		usage_info.time = currTime;
		if ( usage_info.end == "" )
		{
			//if job end time is not available use current time!
			usage_info.end = int2string(currTime);
		}
		string logBuff ="ATMEngine: gridJobId: " + usage_info.dgJobId + " submitted by " + usage_info.user_CertSubject;
		hlr_log ( logBuff, &logStream,4);
		//check for duplicated transaction
		string previousJobId;
		urBuff = composeLogData(usage_info);
		code = checkDuplicate(usage_info,
				success,
				possibleResubmission,
				previousJobId,
				uniqueChecksum);
		if ( code != 0 ) possibleResubmission = false;//duplicated, so not resubmission.
		t.id = usage_info.dgJobId;
		if ( previousJobId != "" )
		{
			urBuff += ",dg_jobid_subst=" + previousJobId;
		} 
		//Resubmission check below
		if ( possibleResubmission && t.exists() && success )//success still necessary
		{
			hlr_log ("ATMEngine: Warning: record dgJobId already present, resubmission.", &logStream,3);
			//if ( t.get() != 0 )
			//{
			//	hlr_log ("ATMEngine: Error retrieving record", &logStream,1);
			//	code = atoi(ATM_E_DUPLICATED_C);//72
			//	success = false;
			//}
			//else
			//{	
			db hlrDb ( hlr_sql_server,
					hlr_sql_user,
					hlr_sql_password,
					hlr_sql_dbname
			);
			if ( hlrDb.errNo == 0 )
			{
				string dgJobIdBuff = usage_info.dgJobId;
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
						string logBuff = "ATM Engine:"+ t.id + "," + usage_info.dgJobId + "/";
						logBuff += t.logData + "," + urBuff;
						hlr_log (logBuff, &logStream,4);
						logBuff = "ATM Engine: got keys:"+ int2string(result.numRows());
						hlr_log (logBuff, &logStream,5);
						usage_info.dgJobId = usage_info.dgJobId + "/" + int2string(result.numRows());
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
			//}
		}
		//end of resubmission check.
	}
	if ( success )
	{
		usage_info.uniqueChecksum = uniqueChecksum;
		string logBuff ="Inserting: " +
				usage_info.dgJobId + "," +
				usage_info.user_CertSubject + "," +
				usage_info.res_grid_id + "," +
				usage_info.res_acct_bank_id+","+
				usage_info.uniqueChecksum+","+
				usage_info.accountingProcedure;
		hlr_log (logBuff, &logStream,6);
		//put the transactuion in the queue;
		qTransaction qt( usage_info.dgJobId,
				usage_info.user_CertSubject,
				usage_info.res_grid_id,
				usage_info.res_acct_bank_id,
				int2string(time(NULL)),
				urBuff,
				0,
				time(NULL),
				usage_info.uniqueChecksum,
				usage_info.accountingProcedure
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
	if ( ATM_compose_xml(usage_info, message, output) != 0 )
	{
		hlr_log ( "ATM_engine: Error composing the XML answer!",&logStream,3);
	}
	hlr_log ("ATM Engine: Exiting.", &logStream,4);	
	return code;
} //ATMEngine( string doc, connInfo &connectionInfo, string *output )


}; // namespace
