// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: advancedQueryEngine2.cpp,v 1.1.2.1.4.11 2011/06/21 14:45:34 aguarise Exp $
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
#include "advancedQueryEngine2.h"
#include <sstream>
#include <iomanip>

extern ofstream logStream;
extern string  maxItemsPerQuery;
extern string authQueryTables;
extern bool authUserSqlQueries; 

inline int urlSplit(char delim, string url_string , url_type *url_buff)
{
	size_t pos = 0;
	pos = url_string.find_first_of( delim, 0);
	url_buff->hostname=url_string.substr(0,pos);
	url_buff->port=atoi((url_string.substr(pos+1,url_string.size()).c_str()));

	return 0;
}

#ifdef WITH_VOMS
string getVomsAuthQuery (string &authRole, inputData& iD, connInfo& c)
{
	string buff = "";
	//normalUser ACL -- Default!
	if (authRole == "normalUser" )
	{
		if ( iD.queryTypeBuffer == "resourceAggregate" )
		{
			//normalUser can see aggregates for her VO Jobs
			//and for its own jobs. Not for other specific
			//users (even if from the same VO)
			//DENY LIST:
			if ( (iD.groupBy != "" && iD.userCertBuffer != c.contactString) )
			{
				//restrict queries to this user only.
				iD.userCertBuffer = c.contactString;
			}
			//ALLOW LIST:
			if ( iD.userCertBuffer == "" ||
					iD.userCertBuffer == c.contactString ||
					iD.voIDBuffer == c.voname )
			{
				buff = " AND gridUser LIKE \"%";
				buff += c.contactString + "%\"";
				return buff;
			}
			//default unauthorised request
			return "AUTHERROR";
		}
		//Can't authorise other query types.
		return "AUTHERROR";
	}
	if ( authRole == "groupManager" )
	{
		//group managers should be able to see every informationi
		//for records with the same VO and group as the submitter.
		//current implementatio takes just the firs FQAN
		buff = " AND userVo LIKE \"";
		buff += c.voname + "\"";
		vector<vomsAC>::iterator it = (c.vomsData).begin();
		string groupBuff = (*it).group; //take first group only
		buff += " AND userFqan LIKE \"%";
		buff += groupBuff;
		buff += "/%\"";
		return buff;
	}
	if ( authRole == "voManager" )
	{
		//vomanager should be able to see every information
		//for records with the same VO as the submitter,
		buff = " AND userVo LIKE \"";
		buff += c.voname + "\""; 
		return buff;
	}
	if ( authRole == "siteManager" )
	{
		//siteManager should be able to see every information
		//concerning the site.
		//not implemented yet.
	}
	return "AUTHERROR";
}
#endif

void xml2struct(inputData &out, string &xml)
{
	node tagBuff;
	string logBuff = "";

	tagBuff = parse(&xml, "jobId");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.jobId = tagBuff.text;
	}
	tagBuff = parse(&xml, "startTid");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.startTid = tagBuff.text;
	}

	tagBuff = parse(&xml, "USERCERTBUFFER");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.userCertBuffer = tagBuff.text;
	}


	tagBuff = parse(&xml, "RESOURCEIDBUFFER");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.resourceIDBuffer = tagBuff.text;
	}


	tagBuff = parse(&xml, "TIMEBUFFER");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.timeBuffer = tagBuff.text;
	}


	tagBuff = parse(&xml, "GROUPIDBUFFER");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.groupIDBuffer = tagBuff.text;
	}


	tagBuff = parse(&xml, "VOIDBUFFER");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.voIDBuffer = tagBuff.text;
	}

	tagBuff = parse(&xml, "VOMSROLE");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.fqanBuffer = tagBuff.text;
	}

	tagBuff = parse(&xml, "FREQUENCYBUFFER");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.frequencyBuffer = tagBuff.text;
	}

	tagBuff = parse(&xml, "AGGREGATESTRINGBUFFER");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.aggregateStringBuff = tagBuff.text;
	}
	tagBuff = parse(&xml, "TIMEINDEXBUFF");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.timeIndexBuff = tagBuff.text;
	}


	tagBuff = parse(&xml, "DEBUG");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.debug =  atoi(tagBuff.text.c_str());
	}


	tagBuff = parse(&xml, "AGGREGATEFLAG");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.aggregateFlag = atoi(tagBuff.text.c_str());
	}

	tagBuff = parse(&xml, "groupBy");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.groupBy = tagBuff.text;
	}

	tagBuff = parse(&xml, "siteName");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.siteName = tagBuff.text;
	}

	tagBuff = parse(&xml, "urOrigin");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.urOrigin = tagBuff.text;
	}

	tagBuff = parse(&xml, "authoriseAs");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.substDn = tagBuff.text;
	}

	tagBuff = parse(&xml, "orderBy");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.orderBy = tagBuff.text;
	}

	tagBuff = parse(&xml, "LISTFLAG");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.listFlag = atoi(tagBuff.text.c_str());
	}


	tagBuff = parse(&xml, "QUERYTYPEBUFFER");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.queryTypeBuffer = tagBuff.text;
	}


	tagBuff = parse(&xml, "ITSHEADING");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.itsHeading = atoi(tagBuff.text.c_str());
	}

	tagBuff = parse(&xml, "itsFieldList");
	if(tagBuff.status == 0)
	{
		logBuff = "queryENGINE, got: \"" + tagBuff.text + "\"";
		hlr_log(logBuff, &logStream, 8);
		out.itsFieldList = tagBuff.text;
	}

	return;
}
int advancedQueryEngine_parse_xml (string &doc, inputData &input)
{
	node nodeBuff;
	while ( nodeBuff.status == 0 )
	{
		string tag = "inputData";
		nodeBuff = parse(&doc, tag);
		if ( nodeBuff.status != 0 )
			break;
		xml2struct(input, doc);
		nodeBuff.release();
	}
	return 0;

}//advancedQueryEngine_parse_xml


int advancedQueryEngine_compose_xml(vector<string> &queryResult ,string *output, int status, inputData& d)
{
	*output = "<HLR type=\"advancedQuery_answer\">\n";
	*output += "<BODY>\n";
	*output += "<queryResult>";
	vector<string>::iterator it = queryResult.begin();
	vector<string>::iterator it_end = queryResult.end();
	while ( it != it_end )
	{	
		*output += "\n<lineString>";
		*output += *it;
		*output += "</lineString>\n";
		it++;
	}
	*output += "</queryResult>\n";
	*output += "<lastTid>\n";
	*output += d.lastTid;
	*output += "\n</lastTid>\n";
	*output += "<STATUS>";
	*output += int2string(status);
	*output += "</STATUS>";
	*output += "</BODY>\n";
	*output += "</HLR>\n";
	return 0;
}//advancedQueryEngine_compose_xml


bool isHlrAdmin (connInfo& connectionInfo)
{
	//check if the request comes from a user mapped as hlrAdmin
	hlrAdmin a;
	a.acl = connectionInfo.contactString;
	if ( a.get() == 0 )
	{
		return true;
	}
	else
	{
		return false;
	}
}//request_is_authorized (ATM_job_data &job_data)

bool isVoAdmin (connInfo& connectionInfo, string &requestedVo)
{
	//if user is mapped as voAdmion for the vo requested in the conn
	//he will not be flagged as normal user but there will be 
	//a coinstraint to the requested vo.
	if ( connectionInfo.contactString == "voAdmin" )
	{
		return true;
	}
	hlrVoAcl v;
	v.voId = requestedVo;
	v.acl = connectionInfo.contactString;
	return v.exists(); 
}

bool isSiteAdmin (connInfo& connectionInfo, string &requestedSite)
{
	//if user is mapped as voAdmion for the vo requested in the conn
	//he will not be flagged as normal user but there will be 
	//a coinstraint to the requested SiteName.
	if ( connectionInfo.contactString == "siteAdmin" )
	{
		return true;
	}
	return false;
}

bool isllHlr (connInfo& connectionInfo )
{
	roles r(connectionInfo.contactString, "lowLevelHlr" );	
	return r.exists();
}

string timeStamp2Broken(inputData &iD, string ts)
{
	struct tm * brokenTime =NULL;
	string timeIndexBuff ="";
	time_t timeVal = atoi(ts.c_str());
	brokenTime = localtime(&timeVal);
	timeIndexBuff += int2string(
			brokenTime->tm_year+1900) + "-";
	if ( brokenTime->tm_mon >= 9 )
	{
		timeIndexBuff += int2string(
				brokenTime->tm_mon+1) + "-";
	}
	else
	{
		timeIndexBuff += "0" + int2string(
				brokenTime->tm_mon+1) + "-";
	}
	if ( brokenTime->tm_mday > 9)
	{
		timeIndexBuff += int2string(
				brokenTime->tm_mday) + " ";
	}
	else
	{
		timeIndexBuff += "0" + int2string(
				brokenTime->tm_mday) + " ";
	}
	if ( brokenTime->tm_hour > 9 )
	{
		timeIndexBuff += int2string(
				brokenTime->tm_hour) + ":";
	}
	else
	{
		timeIndexBuff += "0" + int2string(
				brokenTime->tm_hour) + ":";
	}
	if ( brokenTime->tm_min > 9 )
	{
		timeIndexBuff += int2string(
				brokenTime->tm_min) + ":";
	}
	else
	{
		timeIndexBuff += "0" + int2string(
				brokenTime->tm_min) + ":";
	}
	if (  brokenTime->tm_sec > 9)
	{
		timeIndexBuff += int2string(
				brokenTime->tm_sec);
	}
	else
	{
		timeIndexBuff += "0" + int2string(
				brokenTime->tm_sec);
	}
	return timeIndexBuff;
}

int createTmpTimesTable(inputData& iD, int seed, string &tableName)
{
	pid_t myself = getpid();
	tableName = "tmpTimesTable_" + int2string(myself) +"_" + int2string(seed);
	string queryString = "";
	queryString = "CREATE TABLE " + tableName;
	queryString += "( start datetime, ";
	queryString += " end datetime, ";
	queryString += " primary key (start,end)) TYPE=HEAP";
	if ( iD.debug )
	{
		string logBuff = "tmpTimeTable:" + queryString;
		hlr_log(logBuff, &logStream,8);
	}
	hlrGenericQuery makeTable(queryString);
	return makeTable.query();
}

int dropTmpTimesTable(int seed, string& tableName)
{
	string queryString = "";
	queryString += "DROP TABLE " + tableName;
	hlrGenericQuery dropTable(queryString);
	return dropTable.query();
}

int populateTmpTimesTable(inputData& iD,string start, string end, int seed)
{
	pid_t myself = getpid();
	string tableName = "tmpTimesTable_" + int2string(myself) +"_" + int2string(seed);
	string queryString = "";
	queryString += "REPLACE INTO " + tableName;
	queryString += " VALUES ('";
	queryString += start +"','";
	queryString += end +"')";
	if ( iD.debug )
	{
		hlr_log(queryString,&logStream,8);
	}
	hlrGenericQuery populateTable(queryString);
	return populateTable.query();
}

int produceTimeQueries(inputData& iD, string& q, string& tableName,int seed, vector<string>& startDateVector)
{
	int secondsPerHour = 3600;
	int timeFactor = secondsPerHour;
	string quoteBuff = "'";
	string f = iD.frequencyBuffer;
	string t = iD.timeBuffer;
	if ( f == "day" )
	{
		timeFactor = secondsPerHour*24;
	}
	if ( f == "hour" )
	{
		timeFactor = secondsPerHour;
	}
	if ( f == "week" )
	{
		timeFactor = secondsPerHour*24*7;
	}
	if ( f== "month" )
	{
		timeFactor = secondsPerHour*24*30;
	}
	string query = "select MIN(UNIX_TIMESTAMP(date)),MAX(UNIX_TIMESTAMP(date)) from jobTransSummary WHERE 1 ";
	string queryAppendString = "";
	string separator = "_";
	if (t == "*" || t == "")//match all
	{
		queryAppendString = "";
	}
	if (t == "today" )
	{
		t =  "CURDATE()_";
		quoteBuff = "";
	}
	if (t == "yesterday" )
	{
		t = "(CURDATE()-1)_CURDATE()";
		quoteBuff = "";
	}
	if (t == "thisWeek" )
	{
		t = "(CURDATE()-WEEKDAY(CURDATE()))_(CURDATE()+1)";
		quoteBuff = "";
	}
	if (t == "lastWeek" )
	{
		t =  "(CURDATE()-WEEKDAY(CURDATE())-7)_(CURDATE()-WEEKDAY(CURDATE())-1)";
		quoteBuff = "";
	}
	if (t == "thisMonth" )
	{
		t =  "(CURDATE()-DAYOFMONTH(CURDATE())+1)_(CURDATE()+1)";
		quoteBuff = "";
	}
	if (t == "lastMonth")
	{
		t =  "SUBDATE((CURDATE()-DAYOFMONTH(CURDATE())+1),INTERVAL 1 MONTH)_(CURDATE()-DAYOFMONTH(CURDATE()))";
		quoteBuff = "";
	}
	string::size_type pos = t.find_first_of(separator);
	if ( pos != string::npos )
	{
		if ( pos == 0 )//time interval
		{
			// type: -timestamp
			queryAppendString = t.substr(pos+1);
			queryAppendString = "AND jobTransSummary.date <="+quoteBuff + queryAppendString + quoteBuff;
		}
		else
		{
			if ( pos == t.length()-1 )
			{
				// type: timestamp-
				queryAppendString = t.substr(0,pos);
				queryAppendString = "AND jobTransSummary.date >"+quoteBuff + queryAppendString + quoteBuff;
			}
			else
			{
				//type timeA - time B
				queryAppendString = t.substr(0,pos);
				queryAppendString = "AND jobTransSummary.date >"+quoteBuff + queryAppendString + quoteBuff;
				queryAppendString += " AND jobTransSummary.date <=" +quoteBuff + t.substr(pos+1)+quoteBuff;
			}
		}
	}
	query = query + queryAppendString;
	if (iD.debug)
	{
		string logBuff = "produceTimeQueries:" +query;
		hlr_log(logBuff, &logStream,8);
	}
	hlrGenericQuery genericQuery(query);
	int res = genericQuery.query();
	if ( res != 0 )
	{
		if (iD.debug)
		{
			hlr_log("Error retrieving list of tr_stamp",&logStream,8);
		}
	}
	else
	{
		string startString = ((genericQuery.queryResult).front())[0];
		startString = startString.substr(0,8);
		startString = startString+ "00";
		int start =
				atoi(startString.c_str());
		string endString = ((genericQuery.queryResult).front())[1];
		endString = endString.substr(0,8);
		endString = endString+ "00";
		int end =
				atoi(endString.c_str());
		if (iD.debug)
		{
			string logBuff = "start:" + int2string(start) + ",end:" + int2string(end);
			hlr_log(logBuff, &logStream,8);
			logBuff = "startString:" + startString + ",endString:" + endString;
			hlr_log(logBuff, &logStream,8);
		}
		int time = start;
		string dateBuff ="";
		while (time < end )
		{
			dateBuff = timeStamp2Broken(iD, int2string(time));
			populateTmpTimesTable(iD, dateBuff,timeStamp2Broken(iD,int2string(time+timeFactor)),seed);
			if ( iD.debug )
			{
				hlr_log ("Error in query", &logStream,1);
			}
			startDateVector.push_back(dateBuff);
			time = time+timeFactor;
		}
		q = " AND jobTransSummary.date>"+tableName+".start AND jobTransSummary.date <= "+tableName+".end GROUP BY "+tableName+".start";
	}
	return 0;

}

string parseTime (inputData &iD, string& t)
{

	string queryBuff = "";
	string quoteBuff = "'";
	if (t == "*" || t == "")//match all
	{
		queryBuff = "";
	}
	if (t == "today" )
	{
		t =  "CURDATE()_";
		quoteBuff = "";
	}
	if (t == "yesterday" )
	{
		t = "(CURDATE()-1)_CURDATE()";
		quoteBuff = "";
	}
	if (t == "thisWeek" )
	{
		t =  "(CURDATE()-WEEKDAY(CURDATE()))_(CURDATE()+1)";
		quoteBuff = "";
	}
	if (t == "lastWeek" )
	{
		t =  "(CURDATE()-WEEKDAY(CURDATE())-7)_(CURDATE()-WEEKDAY(CURDATE())-1)";
		quoteBuff = "";
	}
	if (t == "thisMonth" )
	{
		t =  "(CURDATE()-DAYOFMONTH(CURDATE())+1)_(CURDATE()+1)";
		quoteBuff = "";
	}
	if (t == "lastMonth" )
	{
		t =  "SUBDATE((CURDATE()-DAYOFMONTH(CURDATE())+1),INTERVAL 1 MONTH)_(CURDATE()-DAYOFMONTH(CURDATE()))";
		quoteBuff = "";
	}
	string separator = "_";
	string::size_type pos = t.find_first_of(separator);
	if ( pos != string::npos )//time interval
	{
		if ( pos == 0 )
		{
			// type: -timestamp
			queryBuff = t.substr(pos+1);
			queryBuff = "AND jobTransSummary.date <=" +quoteBuff + queryBuff + quoteBuff;
		}
		else
		{
			if ( pos == t.length()-1 )
			{
				// type: timestamp-
				queryBuff = t.substr(0,pos);
				queryBuff = "AND jobTransSummary.date >" + quoteBuff + queryBuff +quoteBuff;
			}
			else
			{
				//type timeA - time B
				queryBuff = t.substr(0,pos);
				queryBuff = "AND jobTransSummary.date >"+ quoteBuff + queryBuff;
				queryBuff += quoteBuff + " AND jobTransSummary.date <="+quoteBuff + t.substr(pos+1)+quoteBuff;
			}
		}
	}
	string logBuff = "parseTime:" + queryBuff;
	hlr_log (logBuff, &logStream,8);
	return queryBuff;
}

#ifdef MERGE
int getAvailableTables(connInfo& connectionInfo, vector<string>& tables)
{
	database hlrDb ( hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	string logBuff;
	mergeTables mt(hlrDb, mergeTablesDefinitions, false);
	int res = mt.getDef();
	if ( res != 0 )
	{
		logBuff = "Error reading tables definitions. Just jobTransSummary will be available, error:" + int2string(res);
		hlr_log (logBuff,&logStream,3);
		return res;
	}
	vector<string>::iterator it = (mt.definitions).begin();
	while ( it != (mt.definitions).end() )
	{
		vector<string> defBuffV;
		Split(':',*it,&defBuffV);
		if ( defBuffV.size() < 2 )
		{
			logBuff = "Bad merge definition: " + *it;
			hlr_log(logBuff, &logStream, 7);	
		}
		else
		{
			tables.push_back(defBuffV[0]);		
		}
		it++;
	}
	return 0;
}
#endif

inline string stripWhite ( string &input )
{
	size_t startPos = input.find_first_not_of(" \n\0");
	if ( startPos == string::npos )
		return "";
	size_t endPos = input.find_last_not_of(" \n\0");
	if ( endPos == string::npos )
		return "";
	return input.substr( startPos, endPos - startPos +1 );
}

int getSySDefTables(vector<string>& tables)
{
	string queryString = "show tables like \"sysDef%\"";
	hlrGenericQuery genericQuery(queryString);
	int res = genericQuery.query();
	if ( res == 0 )
	{
		vector<resultRow>::const_iterator it = (genericQuery.queryResult).begin();
		while ( it != (genericQuery.queryResult).end() )
		{
			string logBuff = "Adding " + (*it)[0] + " to tables.";	
			hlr_log(logBuff, &logStream,8); 
			tables.push_back((*it)[0]);
			it++;
		}
	}	
	return 0;
}

int getAuthorizedTables(vector<string>& tables)
{
	vector<string> buffer;
	Split(';',authQueryTables, &buffer );
	vector<string>::iterator it = buffer.begin();
	vector<string>::iterator it_end = buffer.end();
	while ( it != it_end )
	{
		tables.push_back(stripWhite(*it));
		it++;
	}
	//FIXME complete getAuthorizedTables
	//This method retrieves the list of "user@db.table:password" that can be queried.
	return 0;
}

string composeQuery(inputData &iD, string& time_interval, string &tableName, connInfo &connectionInfo)
{
	if ( iD.aggregateStringBuff == "")
	{
		iD.aggregateStringBuff = "COUNT(dgJobId),SUM(cpuTime)/60,SUM(wallTime)/60,SUM(pmem)/1024,SUM(vmem)/1024,SUM(AMOUNT)/1000";
	}
	string urOriginBuff = iD.urOrigin;
	bool voAdminQuery = false;
	bool llHlr = false;
	bool hlrAdminQuery = isHlrAdmin(connectionInfo);
	string logStringBuff;
	string groupByQuery ="";
	string certBuff = iD.resourceIDBuffer;
#ifdef WITH_VOMS
	string authRole = "";
	string vomsAuthQueryString = "";
#endif
	string authRoleLogBuff = "normalUser";
	//AUTHZ checks here
	if ( !hlrAdminQuery )
	{
		voAdminQuery = isVoAdmin(connectionInfo, iD.voIDBuffer);
		if ( voAdminQuery )
		{
			logStringBuff = "voAdmin Query, privileges granted for vo:";
			logStringBuff += iD.voIDBuffer;
			authRoleLogBuff = "voAdmin";
		}
		else
		{
			llHlr = isllHlr(connectionInfo);
			if ( llHlr )
			{
				logStringBuff =  "Query from registered llHlr, privileges granted for records coming from:";              
				logStringBuff += connectionInfo.hostName;
				urOriginBuff = connectionInfo.hostName;
				authRoleLogBuff = "lowLevelHlr";
			}
			else
			{
#ifdef WITH_VOMS
				//static mappings have higher priority on
				//voms mapping. Voms mapping here
				//Now get hlrRole mapping from voms AC role.
				authRole = hlrRoleGet(connectionInfo);
				authRoleLogBuff = authRole;

#endif
			}

		}
	}
	else
	{
		logStringBuff = "hlrAdmin Query, all privileges granted!";
		authRoleLogBuff = "hlrAdmin";
	}
	//end of AUTHZ check
	hlr_log (logStringBuff,&logStream,4);
	logStringBuff = "Connection certificate mapped to hlrRole:" + authRoleLogBuff;
	hlr_log (logStringBuff,&logStream,4);
	string maxItemsBuff = maxItemsPerQuery;
	if ( maxItemsBuff != "" )
	{
		if ( maxItemsBuff.find_first_not_of("0123456789") != string::npos )
		{
			//not just a number. try searching for authRoleLogBuff:number string and use number as the limit value, otherwise search for "default:number" and use this as limit
			size_t startPos = maxItemsBuff.find(authRoleLogBuff);
			if ( startPos != string::npos )
			{
				//authRoleLogBuff string found
				startPos = maxItemsBuff.find(":(",startPos) + 2;
				size_t endPos = maxItemsBuff.find_first_of(")", startPos);
				if ( endPos != string::npos )
				{
					maxItemsBuff = maxItemsBuff.substr(startPos, endPos-startPos);
				}
			}
		}
		if ( maxItemsBuff.find_first_not_of("0123456789") != string::npos )
		{
			hlr_log ("Configuration error for maxItemsPerQuery variable, please check, using default value", &logStream, 3);
			maxItemsBuff = "2500";
		}
		string Buff = "maxItemsPerQuery =" + maxItemsBuff;
		hlr_log (Buff, &logStream,7);
	}
	string limit;
	string queryBuff = "";
	string aggregateStringBuff = iD.aggregateStringBuff;
	string orderByQuery;
	if ( iD.frequencyBuffer != "" )
	{
		aggregateStringBuff = tableName + ".start,"+tableName +".end," + aggregateStringBuff;
	}
	else
	{
		aggregateStringBuff = "MIN(date),MAX(date),"+aggregateStringBuff;
	}
	if ( iD.groupBy != "" )
	{
		aggregateStringBuff = iD.groupBy + "," + aggregateStringBuff;
		groupByQuery += " GROUP BY " + iD.groupBy;
	}
	queryBuff = "SELECT "+ aggregateStringBuff+ " FROM ";
	if ( iD.frequencyBuffer != "" )
	{
		queryBuff += tableName +",";
	}
	if ( iD.orderBy != "" )
	{
		string::size_type pos = (iD.orderBy).find(" DESC");
		if ( pos != string::npos )
		{
			iD.orderBy = (iD.orderBy).substr(0, (iD.orderBy).size()-5);
			orderByQuery = " ORDER BY " + iD.orderBy + " DESC ";
		}
		else
		{
			orderByQuery = " ORDER BY " + iD.orderBy + " ";
		}
	}
	queryBuff += "jobTransSummary ";
	queryBuff +=" WHERE 1 ";
	if ( certBuff != "*" &&
			certBuff != "" )
	{
		string::size_type pos = certBuff.find("NOT ");
		if ( pos != string::npos )
		{
			queryBuff += " AND gridResource NOT LIKE BINARY \"";
			queryBuff += certBuff.substr(pos+4)+= "\"";
		}
		else
		{
			queryBuff += " AND gridResource LIKE BINARY \""
					+certBuff+"\"";
		}
	}
	if ( iD.fqanBuffer != "" )
	{
		queryBuff += " AND userFqan LIKE \"\%"
				+ iD.fqanBuffer+ "\%\"";
	}
	if ( iD.voIDBuffer != "" )
	{
		queryBuff += " AND userVo LIKE \""
				+ iD.voIDBuffer+ "\"";
	}
	if ( iD.groupIDBuffer != "" )
	{
		queryBuff += " AND hlrGroup LIKE \""
				+ iD.groupIDBuffer + "\"";
	}
	if ( iD.siteName != "" )
	{
		queryBuff += " AND siteName LIKE \""
				+ iD.siteName + "\"";
	}
	if ( urOriginBuff != "" )
	{
		queryBuff += " AND urSourceServer LIKE \""
				+ urOriginBuff + "\"";
	}
	if ( iD.jobId != "" )
	{
		queryBuff += " AND dgJobId LIKE BINARY \"";
		queryBuff += iD.jobId += "\"";
	}
	if ( iD.userCertBuffer != "" )
	{
		string::size_type pos = (iD.userCertBuffer).find("NOT ");
		if ( pos != string::npos )
		{
			queryBuff += " AND gridUser NOT LIKE \"";
			queryBuff += (iD.userCertBuffer).substr(pos+4)+= "\"";
		}
		else
		{
			queryBuff += " AND gridUser LIKE \"";
			queryBuff += iD.userCertBuffer += "\"";
		}
	}
	if ( iD.startTid != "" )
	{
		queryBuff += " AND id < " + iD.startTid;
	}
	//AUTHZ enforcement
	bool normalUser=true;
	if ( hlrAdminQuery || voAdminQuery || llHlr )//if not hlrAdmin check voAdmin of the record.
	{
		//no constraints
		normalUser = false;
	}
#ifdef WITH_VOMS
	if ( authRole != "" )
	{
		vomsAuthQueryString = getVomsAuthQuery (
				authRole,
				iD,
				connectionInfo
		);
		normalUser=false;//set to false since normaluser enforcement
		//is done by getVomsAuthQuery  and doesn't need
		//to be done hereafter.
		if ( vomsAuthQueryString != "AUTHERROR" )
		{
			queryBuff += vomsAuthQueryString;
		}
		else
		{
			return vomsAuthQueryString;
		}
		if ( authRole == "normalUser" )
		{
			limit = " LIMIT " + maxItemsBuff;
		}
	}
#endif
	if ( normalUser )
	{
		//last line of defense, if no other checks are true,
		//assume is normal user with minimum set of priviledges.
		queryBuff += " AND gridUser LIKE \"%";
		queryBuff += connectionInfo.contactString + "%\"";
		limit = " LIMIT " + maxItemsBuff;
	}
	//end AUTHZ enforcement
	queryBuff += time_interval;
	queryBuff += groupByQuery;
	queryBuff += orderByQuery;
	queryBuff += limit;
	string logBuff;
	logBuff = "composeQuery:" + queryBuff;
	hlr_log (logBuff, &logStream,9);
	return queryBuff;
}

int parseQueryType(inputData& iD)
{
	if ( iD.queryTypeBuffer == "sql" || iD.queryTypeBuffer == "sqlCsv" )
	{
		return 0;
	}
	if ( iD.queryTypeBuffer == "resourceAggregate" )
	{
		return 0;
	}
	if ( iD.queryTypeBuffer == "fieldList" )
	{
		return 0;
	}
	if ( iD.queryTypeBuffer == "showTables" )
	{
		return 0;
	}
	return 1;
}

void getOutput (inputData &iD, vector<string>& startDateVector, vector<string>& outputVector, hlrGenericQuery& q)
{
	string userCertBuffer = iD.userCertBuffer;
	string resourceIDBuffer = iD.resourceIDBuffer;
	string timeBuffer = iD.timeBuffer;
	string groupIDBuffer = iD.groupIDBuffer;
	string voIDBuffer = iD.voIDBuffer;
	string frequencyBuffer = iD.frequencyBuffer; 
	string timeIndexBuff = iD.timeIndexBuff;
	string queryTypeBuffer = iD.queryTypeBuffer;
	bool debug = iD.debug;
	bool itsHeading = iD.itsHeading;
	string lastIdBuff;

	int res = 0;
	if ((q.queryResult).size() == 0 ) res = -1;

	if ( queryTypeBuffer == "resourceAggregate" )
	{
		string aggregateStringBuff = "MIN(date),MAX(date)," + iD.aggregateStringBuff;
		if ( iD.groupBy != "" )
		{
			aggregateStringBuff = iD.groupBy + "," + aggregateStringBuff;
		}
		vector<int> queryFieldsSize;
		vector<string> queryItems;
		itsHeading = true;
		hlr_log("Aggregate query",&logStream,7);
		Split(',',aggregateStringBuff, &queryItems );
		bool emptyRowsPresent =false;
		if ( (q.queryResult).size() < startDateVector.size() && (frequencyBuffer != "") )
		{
			emptyRowsPresent = true;
		}
		if ( (q.queryResult).size() == 0 )
		{
			hlr_log("result size == 0",&logStream,4);
			return;
		}
		if ( itsHeading )
		{
			ostringstream os;
			if (debug)
			{
				hlr_log("Heading...",&logStream,8);
			}
			vector<string>::const_iterator heading = queryItems.begin();
			int i =0;
			size_t headingBuff = 0;//
			size_t buff = 0;//
			while ( heading != queryItems.end() )
			{
				if (debug )
				{
					string logBuff = "inserting:" + *heading;
					hlr_log(logBuff,&logStream,8);
				}
				vector<resultRow>::const_iterator it = (q.queryResult).begin();
				headingBuff = (*heading).size();
				while ( it != (q.queryResult).end() )
				{
					buff = ( headingBuff > ((*it)[i]).size()) ? headingBuff : ((*it)[i]).size();
					headingBuff = buff;
					it++;
				}
				if ( i == 0 )
				{
					os << right << setw(headingBuff) << *heading << left << "|";
				}
				else
				{
					os << setw(headingBuff) << *heading << "|";
				}
				queryFieldsSize.push_back(headingBuff);
				heading++;
				i++;
			}
			os.flush();
			outputVector.push_back(os.str());
			itsHeading = false;
		}
		vector<resultRow>::const_iterator it = (q.queryResult).begin();
		if ( emptyRowsPresent )
		{
			string logBuff;
			hlr_log("Empty rows...",&logStream,8);
			vector<string>::const_iterator date=startDateVector.begin();
			//this loop is used only if the query retrieves
			//less rows than it should
			while ( it != (q.queryResult).end() && date != startDateVector.end())
			{
				ostringstream os;
				if ( *date == (*it)[0] )
				{
					for ( size_t i = 0; i < (*it).size(); i++ )
					{
						if ( i == 0 )	
						{
							os << left << setw(queryFieldsSize[i]) << (*it)[i] << right << "|";
						}
						else
						{
							os << setw(queryFieldsSize[i]) << (*it)[i] << "|";
						}
					}
					it++;
					date ++;
					os.flush();
					outputVector.push_back(os.str());
					if ( debug )
					{
						logBuff = os.str();
					}
				}
				else
				{
					os << setw(queryFieldsSize[0]) << *date << "|";
					os << setw(queryFieldsSize[1]) << *(++date) << "|";
					for ( size_t i = 2; i < (*it).size(); i++ )
					{
						os << setw(queryFieldsSize[i]) << "0" << "|";
					}
					os.flush();
					outputVector.push_back(os.str());
					if ( debug )
					{
						logBuff = os.str();
					}
				}
				hlr_log(logBuff,&logStream,6);
			}
			hlr_log("...done.",&logStream,8);
		}
		else
		{
			hlr_log("No Empty rows...",&logStream,8);
			while ( it != (q.queryResult).end() )
			{
				ostringstream os;
				if ((*it).size() != queryFieldsSize.size() )
				{
					hlr_log("Mismatch in vectors size",&logStream,0);
					string errBuff = "Engine internal error.";
					outputVector.push_back(errBuff);
					return;
				}
				for ( size_t i = 0; i < (*it).size(); i++ )
				{
					if ( i == 0 )	
					{
						os << left << setw(queryFieldsSize[i]) << (*it)[i] << right <<"|";
					}
					else
					{
						os << setw(queryFieldsSize[i]) << (*it)[i] << "|";
					}
				}
				it++;
				os.flush();
				outputVector.push_back(os.str());
			}
			hlr_log("...done.",&logStream,8);
		}

	}
	return;
}

string composeError(string message, int status)
{
	string output;
	output = "<HLR type=\"advancedQuery_answer\">\n";
	output += "<BODY>\n";
	output += "<Error>";
	output += message;
	output += "</Error>";
	output += "<STATUS>";
	output += int2string(status);
	output += "</STATUS>";
	output += "</BODY>\n";
	output += "</HLR>\n";
	return output;
}

//get the xml object from the daemon, parse the information 
int advancedQueryEngine( string &inputXML, connInfo &connectionInfo, string *output )
{
	hlr_log ("advancedQueryEngine: Entering.", &logStream,5);
	int exitCode = 0;
	inputData inputStruct;
	if ( advancedQueryEngine_parse_xml (inputXML, inputStruct) != 0)
	{
		hlr_log ("advancedQueryEngine: ERROR parsing XML.", &logStream,1);
		exitCode =1;
	}
	if ( inputStruct.substDn != "" )
	{
		if ( isHlrAdmin(connectionInfo) )
		{
			string logBuff = "Request received with certSubject mapped as hlrAdmin";
			hlr_log(logBuff,&logStream,3);
			connectionInfo.contactString = inputStruct.substDn;
			logBuff = "requested to be authorised as:";
			logBuff += connectionInfo.contactString;
			hlr_log(logBuff,&logStream,3);
		}
		else
		{
			*output = composeError("You ar not authorised to issue an authoriseAs request. Your certificate is not mapped as hlr admin, sorry.",1);
			return 4;
		}
	}
	if ( 
			inputStruct.queryTypeBuffer != "resourceAggregate" &&
			inputStruct.queryTypeBuffer != "sql" &&
			inputStruct.queryTypeBuffer != "sqlCsv" &&
			inputStruct.queryTypeBuffer != "fieldList" &&
			inputStruct.queryTypeBuffer != "showTables" )
	{
		*output = composeError("Unknown Query Type",1);
		return 1;
	}
	vector<string> outputVector;
	if ( parseQueryType(inputStruct) != 0 )
	{
		hlr_log ("advancedQueryEngine: Error composing the query",
				&logStream,1);
		*output = composeError("HLR Internal error:Error composing the query",2);
		return 2;
	}
	else	
	{
		string logBuff = "advancedQueryEngine: Query Type identified:";
		logBuff += inputStruct.queryTypeBuffer;
		hlr_log (logBuff,
				&logStream,6);
	}
#ifdef MERGE
	if ( inputStruct.queryTypeBuffer == "showTables" )
	{
		string logBuff;
		vector<string> tables;
		int res = getAvailableTables(connectionInfo,tables);
		if ( res != 0 )
		{
			logBuff = "Error retrieving table list:" + int2string(res);
			tables.push_back("jobTransSummary");
			hlr_log(logBuff,&logStream,6);
		}
		res = getAuthorizedTables(tables);
		if ( res != 0 )
		{
			logBuff = "Error retrieving table list:" + int2string(res);
			tables.push_back("jobTransSummary");
			hlr_log(logBuff,&logStream,6);
		}
		hlr_log("Formatting query output...", &logStream,8);
		*output = "<HLR type=\"advancedQuery_answer\">\n";
		*output += "<BODY>\n";
		*output += "<queryResult>";
		vector<string>::iterator it = tables.begin();
		while ( it != tables.end() )
		{
			*output += "\n<lineString>";
			*output += *it;
			*output += "</lineString>\n";
			it++;
		}
		*output += "</queryResult>\n";
		*output += "<STATUS>0</STATUS>";
		*output += "</BODY>\n";
		*output += "</HLR>\n";
		hlr_log("...done.", &logStream,8);
		return 0;
	}
#endif
	if ( inputStruct.queryTypeBuffer == "fieldList"  )
	{
		if ( inputStruct.itsFieldList != "1" )
		{
			*output = composeError("Command not supported, client too old.",3);
			return 3;
		}
		//returns help info here.
		string queryString = "DESCRIBE jobTransSummary";
		hlr_log("Performing query...", &logStream,8);
		hlrGenericQuery genericQuery(queryString);
		int res = genericQuery.query();
		hlr_log("... query performed", &logStream,8);
		if ( res != 0 )
		{
			hlr_log ("advancedQueryEngine.Error in query.",
					&logStream,3);
			*output = composeError("Info not Found",3);
			return 3;
		}
		else
		{
			hlr_log("Formatting query output...", &logStream,8);
			*output = "<HLR type=\"advancedQuery_answer\">\n";
			*output += "<BODY>\n";
			*output += "<queryResult>";
			vector<resultRow>::const_iterator it = (genericQuery.queryResult).begin();
			while ( it != (genericQuery.queryResult).end() )
			{
				*output += "\n<lineString>";
				*output += (*it)[0] + " - " + (*it)[1] + " - " +(*it)[3];
				*output += "</lineString>\n";
				it++;
			}
			*output += "</queryResult>\n";
			*output += "<STATUS>0</STATUS>";
			*output += "</BODY>\n";
			*output += "</HLR>\n";
			hlr_log("...done.", &logStream,8);
		}
		return 0;
	}
	if ( (inputStruct.queryTypeBuffer == "sql") || 
			(inputStruct.queryTypeBuffer == "sqlCsv") )
	{
		bool authz = false;
		string appendQuery = "";
		string authRoleLogBuff = "";
		if (!isHlrAdmin(connectionInfo) )
		{
			if (isVoAdmin(connectionInfo,inputStruct.voIDBuffer))
			{
				authz = true;
				authRoleLogBuff = "voAdmin";
				appendQuery = " userVo=\"" + inputStruct.voIDBuffer + "\" ";
			}
			else if(isSiteAdmin(connectionInfo,inputStruct.siteName))
			{
				authz = true;
				authRoleLogBuff = "siteAdmin";
				appendQuery = " siteName=\"" + inputStruct.siteName + "\" ";
			}
			else if ( authUserSqlQueries )
			{

				//FIXME insert a configuration parameter to
				//allow the administrator to disable normal 
				//user queries.
				authz = true;
				authRoleLogBuff = "normalUser";
				appendQuery = " gridUser=\"" + connectionInfo.contactString + "\" ";
			}
		}
		else
		{
			authz = true;
			authRoleLogBuff = "hlrAdmin";
		}
		if ( !authz )
		{
			*output = composeError("User not authorized to this query type!",3);
			return 3;
		}
		string queryString = inputStruct.aggregateStringBuff;
		string queryStringBuffer = queryString;
		for (size_t j=0; j<queryStringBuffer.length(); ++j)
		{
			queryStringBuffer[j]=toupper(queryStringBuffer[j]);
		}
		string::size_type pos = 0;
		pos = queryStringBuffer.find("SELECT");
		if ( (pos != 0) || ( pos == string::npos ) )
		{	
			pos = queryStringBuffer.find("EXPLAIN");
			if ( (pos != 0) || ( pos == string::npos ) )
			{
				*output = composeError("Command not allowed: queries MUST begin with SELECT or EXPLAIN ",3);
				return 3;
			}
		}
		//manage LIMIT clause
		string maxItemsBuff = maxItemsPerQuery;
		if ( maxItemsBuff != "" )
		{
			string Buff = "maxItemsPerQuery =" + maxItemsBuff;
			Buff += ", searching for:" + authRoleLogBuff;
			hlr_log (Buff, &logStream,8);
			if ( maxItemsBuff.find_first_not_of("0123456789") != string::npos )
			{
				//not just a number. try searching for authRoleLogBuff:number string and use number as the limit value, otherwise search for "default:number" and use this as limit
				size_t startPos = maxItemsBuff.find(authRoleLogBuff);
				if ( startPos != string::npos )
				{
					//authRoleLogBuff string found
					startPos = maxItemsBuff.find(":(",startPos) + 2;
					size_t endPos = maxItemsBuff.find_first_of(")", startPos);
					if ( endPos != string::npos )
					{
						maxItemsBuff = maxItemsBuff.substr(startPos, endPos-startPos);
					}
				}
			}
			if ( maxItemsBuff.find_first_not_of("0123456789") != string::npos )
			{
				hlr_log ("Configuration error for maxItemsPerQuery variable, please check, using default value", &logStream, 3);
				maxItemsBuff = "2500";
			}
			Buff = "maxItemsPerQuery =" + maxItemsBuff;
			hlr_log (Buff, &logStream,7);
		}
		string currentLimit = maxItemsBuff;
		pos = queryStringBuffer.find("LIMIT");
		if ( pos != string::npos )
		{
			//user specified his own limit value, if it is lower 
			//than maxItemsPerQuery use the user's limit.
			string::size_type limitStart = queryStringBuffer.find_first_of("0123456789",pos);
			string::size_type limitEnd = queryStringBuffer.find_first_not_of("0123456789",limitStart);
			if ( limitStart != string::npos ) 
			{
				string limitBuff = queryStringBuffer.substr(limitStart,limitEnd-limitStart);
				if (atoi(limitBuff.c_str()) < atoi(maxItemsBuff.c_str()))
					currentLimit = limitBuff;

				queryString.erase(pos);
			}
			else	
			{
				*output = composeError("Syntax error",3);
				return 3;
			}	
		}
		//manage ORDER clause
		string orderBuff;
		pos = queryStringBuffer.rfind("ORDER BY");
		if ( pos == string::npos )
			pos = queryStringBuffer.rfind("order by");
		if ( pos != string::npos )
		{
			orderBuff = queryString.substr(pos);
			queryString.erase(pos);
			string logBuff = "ORDER BY:" + orderBuff;
			hlr_log(logBuff, &logStream,6);
		}
		//manage GROUP clause
		string groupBuff;
		pos = queryStringBuffer.rfind("GROUP BY");
		if ( pos == string::npos )
			pos = queryStringBuffer.rfind("group by");
		if ( pos != string::npos )
		{
			groupBuff = queryString.substr(pos);
			queryString.erase(pos);
			string logBuff = "GROUP BY:" + groupBuff;
			hlr_log(logBuff, &logStream,6);
		}
		//find FROM statement
		string tablesBuffer;
		//manage WHERE clause
		pos = queryString.rfind("WHERE ");
		if ( pos == string::npos )
			pos = queryString.rfind("where ");
		if ( pos != string::npos )
		{	
			size_t fromPos = queryString.rfind("FROM ",pos);
			if ( fromPos == string::npos )
				fromPos = queryString.rfind("from ",pos);
			tablesBuffer = queryString.substr(fromPos+5,pos-fromPos-6);
			if ( appendQuery != "" )
				appendQuery = " AND " + appendQuery;
		}
		else
		{
			size_t fromPos = queryString.rfind("FROM ");
			if ( fromPos == string::npos )
				fromPos = queryString.rfind("from ");
			tablesBuffer = queryString.substr(fromPos+5);
			if ( appendQuery != "" )
				appendQuery = " WHERE " + appendQuery;
		}
		string logBuff = "tables:-" + tablesBuffer + "-";
		hlr_log(logBuff, &logStream,8);
		string tableList;
		vector<string> tables;
#ifdef MERGE
		getAvailableTables(connectionInfo,tables);
#endif
		getSySDefTables(tables);
		getAuthorizedTables(tables);
		if ( tables.size() != 0 )
		{
			vector<string>::iterator it = tables.begin();
			vector<string>::iterator it_end = tables.end();
			while ( it != it_end )
			{
				tableList += *it + " ; ";
				it++;
			}
		}
		if ( tableList.find("jobTransSummary") == string::npos )
		{
			tableList += " jobTransSummary ;";
		}
		if ( tableList.find("voStorageRecords") == string::npos )
		{
			tableList += " voStorageRecords ;";
		}
		hlr_log (tableList,&logStream,8);
		vector<string> queryTables;
		Split(',',tablesBuffer,&queryTables);
		vector<string>::iterator it = queryTables.begin();
		while ( it != queryTables.end() )
		{	
			string buff = stripWhite(*it);
			if ( tableList.find(buff) == string::npos )
			{
				string logBuff = "Queries on table: "+ buff + " are not allowed!. use -Q showTables to get available tables.";
				*output = composeError(logBuff,3);
				return 3;
			}
			it++;
		}
		//recompose query
		queryString += appendQuery;
		queryString += groupBuff;
		queryString += orderBuff;
		queryString += " LIMIT " + currentLimit; 
		hlr_log("Performing query...", &logStream,8);
		hlr_log(queryString, &logStream,6);
		hlrGenericQuery genericQuery(queryString);
		int res = genericQuery.query();
		hlr_log("... query performed", &logStream,8);
		if ( res != 0 )
		{
			hlr_log ("advancedQueryEngine.Error in query.",
					&logStream,3);
			string error = "Info not found, DB error:" + genericQuery.errMsg;
			*output = composeError(error,3);
			return 3;
		}
		else
		{
			hlr_log("Formatting query output...", &logStream,8);
			*output = "<HLR type=\"advancedQuery_answer\">\n";
			*output += "<BODY>\n";
			*output += "<queryResult>";
			vector<size_t> fieldsSize;
			vector<resultRow>::const_iterator lenght_it = (genericQuery.queryResult).begin();
			vector<resultRow>::const_iterator lenght_it_end = (genericQuery.queryResult).end();
			size_t buff;
			while (lenght_it != lenght_it_end)
			{
				for (size_t i=0; i<(*lenght_it).size(); i++)
				{
					if ( lenght_it == (genericQuery.queryResult).begin())
					{
						fieldsSize.push_back(0);
					}
					buff = ((*lenght_it)[i]).size();
					if ( fieldsSize[i] < buff ) fieldsSize[i] = buff;
				}
				lenght_it++;
			}
			size_t lineSize = 0;
			vector<size_t>::iterator fieldsSizeIt = fieldsSize.begin();
			vector<size_t>::iterator fieldsSizeIt_end = fieldsSize.end();
			while ( fieldsSizeIt != fieldsSizeIt_end )
			{
				lineSize+=*fieldsSizeIt;
				fieldsSizeIt++;
			}
			stringstream buf2;
			bool csv = false;
			if ( (inputStruct.queryTypeBuffer == "sql") )
			{
				vector<string>::const_iterator h_it= (genericQuery.fieldNames).begin();
				vector<string>::const_iterator h_it_end= (genericQuery.fieldNames).end();
				stringstream buf;
				buf2.fill('-');
				int i =0;
				while ( h_it != h_it_end )
				{
					if ( fieldsSize[i] < (*h_it).size() ) fieldsSize[i] = (*h_it).size();
					buf << left << setw(fieldsSize[i]) << *h_it << right << "|";
					buf2 << left << setw(fieldsSize[i]+1) << right << "+";
					h_it++;
					i++;
				}
				*output += "\n<lineString>+";
				*output += buf2.str();
				*output += "</lineString>\n";
				*output += "\n<lineString>|";
				*output += buf.str();
				*output += "</lineString>\n";
				*output += "\n<lineString>+";
				*output += buf2.str();
				*output += "</lineString>\n";
			}
			else
			{
				csv = true;
			}
			output->reserve( ((genericQuery.queryResult).size())*lineSize );
			vector<resultRow>::const_iterator it = (genericQuery.queryResult).begin();
			vector<resultRow>::const_iterator it_end = (genericQuery.queryResult).end();
			while ( it != it_end )
			{
				*output += "\n<lineString>|";
				vector<string>::const_iterator l_it=(*it).begin();
				vector<string>::const_iterator l_it_end=(*it).end();
				stringstream buf;
				int i =0;
				while ( l_it != l_it_end )
				{
					if ( csv )
					{
						buf << *l_it << "|";
					}
					else
					{
						buf << left << setw(fieldsSize[i]) << *l_it << right << "|";
					}
					l_it++;
					i++;
				}
				*output += buf.str();
				*output += "</lineString>\n";
				it++;
			}
			if ( !csv )
			{
				*output += "\n<lineString>+";
				*output += buf2.str();
				*output += "</lineString>\n";
			}
			*output += "</queryResult>\n";
			*output += "<STATUS>0</STATUS>";
			*output += "</BODY>\n";
			*output += "</HLR>\n";
			hlr_log("...done.", &logStream,8);
		}
		return 0;
	}
	//returns help info here.
	time_t seed = time(NULL);
	string tableName;
	vector<string> startDateVector;
	if ( inputStruct.frequencyBuffer == "" )
	{
		hlr_log ("advancedQueryEngine:Frequency is not defined.", 
				&logStream,6);
		string timeQuery = parseTime( inputStruct, inputStruct.timeBuffer);
		string queryString = composeQuery(  inputStruct, timeQuery,tableName, connectionInfo );
		if ( queryString == "AUTHERROR" )
		{
			hlr_log ("advancedQueryEngine.Query not authorised.",
					&logStream,2);

			*output = composeError("Authorisation Error!",3);
			return 3;
		}
		hlr_log("Performing query...", &logStream,8);
		hlrGenericQuery genericQuery(queryString);
		int res = genericQuery.query();
		hlr_log("... query performed", &logStream,8);
		if ( res != 0 )
		{
			hlr_log ("advancedQueryEngine.Error in query.",
					&logStream,3);

			*output = composeError("Info not Found",3);
			return 3;
		}
		else
		{
			hlr_log("Formatting query output...", &logStream,8);
			getOutput(inputStruct, startDateVector, outputVector, genericQuery);
			hlr_log("...done.", &logStream,8);
			string logBuffer = "advancedQueryEngine: Got rows:";
			logBuffer += int2string(outputVector.size());
			hlr_log (logBuffer,&logStream,6);
			if ( res >= 0 )
			{	
				exitCode = 0;
			}
		}
	}
	else
	{
		string logBuff = "advancedQueryEngine:frequency=";
		logBuff += inputStruct.frequencyBuffer;
		hlr_log (logBuff, &logStream,6);
		string timeQueryBuff = "";
		string queryStringBuff = "";
		inputStruct.itsHeading = true;
		int res = createTmpTimesTable(inputStruct,seed,tableName);
		if ( res != 0 )
		{
			hlr_log("Error creating tmpTimes table!",&logStream,0);
		}
		res = produceTimeQueries(inputStruct, timeQueryBuff,tableName,seed, startDateVector);
		if ( res != 0 )
		{
			hlr_log("Error creating queries!",&logStream,1);
		}
		queryStringBuff = composeQuery(inputStruct,timeQueryBuff,tableName,connectionInfo);
		hlrGenericQuery genericQuery(queryStringBuff);
		genericQuery.query();
		getOutput(inputStruct, startDateVector, outputVector, genericQuery);
		dropTmpTimesTable(seed,tableName);

	}
	if ( advancedQueryEngine_compose_xml(outputVector, output, exitCode , inputStruct) != 0 )
	{
#ifdef DEBUG
		cerr << "advancedQueryengine: Error composing the XML answer!" <<endl;
#endif
		exitCode = 3;
	}
	hlr_log ("advancedQueryEngine: Exiting.", &logStream,4);	
	return exitCode;
}







