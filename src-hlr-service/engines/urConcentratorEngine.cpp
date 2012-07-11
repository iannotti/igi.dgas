#include "urConcentratorEngine.h"

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_sql_dbname;
extern bool deleteOnReset;


int urConcentrator::run()
{
	int res = 0;
	time_t time0 = time(NULL);
	string logBuff = "Entering urConcentrator engine";
	hlr_log(logBuff,&logStream,4);
	logBuff = "urSourceServer:" + c->hostName;
	hlr_log(logBuff,&logStream,4); 
	logBuff = "urSourceServerDN:" + c->contactString;
	hlr_log(logBuff,&logStream,4); 
	if ( !authorize() )
	{
		logBuff = "Could not authorize request!";
		hlr_log(logBuff,&logStream,3);
		errorComposeXml(E_URCONCENTRATOR_AUTH);
		return E_URCONCENTRATOR_AUTH;
	}
	string requestType;
	vector<jobTransSummary> r;
	r.reserve(atoi(recordsPerConnection.c_str()));
	res = xmlParser(requestType, r);
	if ( res != 0 )
	{
		errorComposeXml(res);
		return res;
	}
	if ( requestType == "infoRequest" )
	{
		logBuff = "infoRequest:" + *input;
		hlr_log(logBuff,&logStream,6);
		//inforequestSubEngine here
		res = infoRequestSubEngine();
		return res;
	}
	if ( requestType == "insertRecords" )
	{
		//insertrecords subengine here
		res = insertRequestSubEngine(r);
		time_t time1 = time(NULL);
		time_t deltaT = time1-time0;
		float effProcessedPerSecond = ( (float)(r.size())/((float)deltaT) );
		logBuff = c->contactString + ":Message containing " + int2string(r.size()) + " records processed in " + int2string(deltaT);
		logBuff += " sec: " + int2string(effProcessedPerSecond) + " rec/sec";
		hlr_log(logBuff,&logStream,6);
		return res;
	}
	if ( requestType == "resetRecords" )
	{
		logBuff = "resetRequest:" + *input;
		hlr_log(logBuff,&logStream,6);
		//resetrecords subengine here
		res = resetRequestSubEngine();
		return res;
	}
	res = E_URCONCENTRATOR_WRONG_REQUEST;
	errorComposeXml(res);
	return res;
}


int urConcentrator::infoRequestSubEngine()
{
	int res = 0;
	string logBuff = "Entering infoRequestSubEngine";
	hlr_log(logBuff,&logStream,7);
	//get entry from index table.
	urConcentratorIndex indexEntry;
	res = getIndex(indexEntry);
	if ( res == 0 )
	{
		res = infoRequestComposeXml(indexEntry);
	}
	else
	{
		errorComposeXml(res);
	}
	logBuff = "infoRequestSubEngine exit.";
	hlr_log(logBuff,&logStream,7);
	return res;
}

bool urConcentrator::authorize()
{
	roles rolesBuff(c->contactString, "lowLevelHlr" );
	return rolesBuff.exists();
}

int urConcentrator::insertRequestSubEngine(vector<jobTransSummary>& r)
{
	time_t time0 = time(NULL);
	int actualNumberOfRecords = r.size();
	string logBuff = "Entering insertRequestSubEngine()";
	hlr_log(logBuff,&logStream,7);
	urConcentratorIndex currentIndex;
	currentIndex.urSourceServer = c->hostName; 
	currentIndex.urSourceServerDN = c->contactString;
	logBuff = "urSourceServer:" + currentIndex.urSourceServer;
	hlr_log(logBuff,&logStream,4); 
	logBuff = "urSourceServerDN:" + currentIndex.urSourceServerDN;
	hlr_log(logBuff,&logStream,4);
#ifdef MERGE
	//check if databse is locked due to merge tables update.
	database hlrDb ( hlr_sql_server, 
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	if ( hlrDb.locked() )
	{
		logBuff = "Error inserting queries in the database, the database is temporary locked.";
		hlr_log(logBuff,&logStream,4);
		errorComposeXml(E_URCONCENTRATOR_INSERT_RECORDS);
		return E_URCONCENTRATOR_INSERT_RECORDS;
	} 
#endif
	if ( getIndex(currentIndex) != 0 )
	{
		logBuff = "Error inserting queries in the database, can't find index.";
		hlr_log(logBuff,&logStream,4);
		errorComposeXml(E_URCONCENTRATOR_INSERT_RECORDS);
		return E_URCONCENTRATOR_INSERT_RECORDS;
	}
	if ( useBulkInsert )
	{
		if ( bulkInsertRecords(r) != 0 )
		{
			logBuff = "Error inserting queries in the database";
			hlr_log(logBuff,&logStream,4);
			errorComposeXml(E_URCONCENTRATOR_INSERT_RECORDS);
			return E_URCONCENTRATOR_INSERT_RECORDS;
		}
		else
		{
			currentIndex.remoteRecordId = lastInsertedId;
			currentIndex.recordDate = lastInsertedRecordDate;
			currentIndex.uniqueChecksum = lastInsertedUniqueChecksum;
			updateIndex(currentIndex);
		}
	}
	else
	{
		if ( insertRecords(r) != 0 )
		{
			logBuff = "Error inserting queries in the database";
			hlr_log(logBuff,&logStream,4);
			errorComposeXml(E_URCONCENTRATOR_INSERT_RECORDS);
			return E_URCONCENTRATOR_INSERT_RECORDS;
		}
		else
		{
			currentIndex.remoteRecordId = lastInsertedId;
			currentIndex.recordDate = lastInsertedRecordDate;
			currentIndex.uniqueChecksum = lastInsertedUniqueChecksum;
			updateIndex(currentIndex);
		}
	}
	time_t time1 = time(NULL);
	time_t deltaT = time1 - time0;
	float recordsPerSecond = ((float) actualNumberOfRecords) /  ((float) deltaT);
	logBuff = "Inserted " + int2string(insertedRecords) + " of " + int2string(actualNumberOfRecords) + " records in " + int2string(deltaT) + " sec: ";
	logBuff += int2string(recordsPerSecond) + " rec/sec";
	hlr_log(logBuff,&logStream,6);
	insertRequestComposeXml();
	return 0;
}

int urConcentrator::removeServerRecords()
{
	string queryString = "DELETE FROM urConcentratorIndex WHERE urSourceServer = \'" + c->hostName + "\'";
	hlrGenericQuery delIndex(queryString);
	int res = delIndex.query();
	if ( ( res != 0 ) && ( res != 1) ) 
	{
		string logBuff = "Error deleting server entry from DB with query:" + queryString;
		hlr_log(logBuff,&logStream,2);
		return E_URCONCENTRATOR_DELETE_RECORDS;

	}
	if ( deleteOnReset )
	{
		queryString = "DELETE FROM jobTransSummary WHERE urSourceServer = \'" + c->hostName + "\'";
		hlrGenericQuery del(queryString);
		res = del.query();
		if ( ( res != 0 ) && ( res != 1 ) )
		{
			string logBuff = "Error expunging records from DB with query:" + queryString;
			hlr_log(logBuff,&logStream,2);
			return E_URCONCENTRATOR_DELETE_RECORDS;
		}
	} 
	return 0;
}

int urConcentrator::resetRequestComposeXml()
{
	*output = "<HLR type=\"urConcentrator_answer\">\n";
	*output += "<BODY>\n";
	*output += tagAdd( "STATUS", "0" );
	*output += "</BODY>\n</HLR>\n";
	return 0;
}

int urConcentrator::resetRequestSubEngine()
{
	//reset all the records with urSourceServer == c->hostName;
	string logBuff;
	logBuff = "urConcentrator::resetRequestSubEngine";
	hlr_log(logBuff,&logStream,2);
	if ( authorize() )
	{
		int res = removeServerRecords();
		if ( res != 0 )
		{
			logBuff = "Error removing entries from DB!";
			hlr_log(logBuff,&logStream,2);
			errorComposeXml(res);
			return res;
		}
		else
		{
			logBuff = "Entries removed from DB.";
			hlr_log(logBuff,&logStream,5);
			resetRequestComposeXml();
			return 0;
		}
	}
	else
	{
		logBuff = "Could not authorize request!";
		hlr_log(logBuff,&logStream,2);
		errorComposeXml(E_URCONCENTRATOR_AUTH);
		return E_URCONCENTRATOR_AUTH;
	}
}

int urConcentrator::xmlParser( string& requestType,
		vector<jobTransSummary>& r)
{
	string logBuff;
	node nodeBuff;
	while ( nodeBuff.status == 0 )
	{
		node nodeBuff = parse(input, "requestType");
		if ( nodeBuff.status == 0 )
		{
			requestType = nodeBuff.text;
			logBuff = "Found requestType:" + requestType;
			hlr_log(logBuff,&logStream,4);
			nodeBuff.release();
		}
		else
		{
			break;
		}
		nodeBuff = parse(input, "jobTransSummary");
		if ( nodeBuff.status == 0)
		{
			//there are records attached
			node urNode;
			bool haveRecords = true;
			while ( haveRecords )
			{
				urNode = parse (&nodeBuff.text, "record" );
				if ( urNode.status == 0 )
				{
					jobTransSummary urBuff;
					if ( urBuff.dgJobId == "" ) urBuff.dgJobId = parseAndReleaseS (urNode, "dgJobId" );
					if ( urBuff.date == "" ) urBuff.date = parseAndReleaseS (urNode, "date" );
					if ( urBuff.userFqan == "") urBuff.userFqan = parseAndReleaseS (urNode, "userFqan" );
					if ( urBuff.userVo == "") urBuff.userVo = parseAndReleaseS (urNode, "userVo" );
					if ( urBuff.cpuTime == "") urBuff.cpuTime  = parseAndReleaseS (urNode, "cpuTime" );
					if ( urBuff.wallTime == "") urBuff.wallTime  = parseAndReleaseS (urNode, "wallTime" );
					if ( urBuff.pmem == "") urBuff.pmem = parseAndReleaseS (urNode, "pmem" );
					if ( urBuff.vmem == "") urBuff.vmem  = parseAndReleaseS (urNode, "vmem" );
					if ( urBuff.amount == "") urBuff.amount  = parseAndReleaseS (urNode, "amount" );
					if ( urBuff.start == "") urBuff.start  = parseAndReleaseS (urNode, "start" );
					if ( urBuff.end == "") urBuff.end = parseAndReleaseS (urNode, "end" );
					if ( urBuff.thisGridId == "") urBuff.thisGridId = parseAndReleaseS (urNode, "thisGridId" );//backward Compatibility
					if ( urBuff.thisGridId == "" )
					{
						urBuff.thisGridId  = parseAndReleaseS (urNode, "gridResource" );
					}
					if ( urBuff.remoteGridId == "") urBuff.remoteGridId = parseAndReleaseS (urNode, "remoteGridId" );//backward Compatibility
					if ( urBuff.remoteGridId == "" )
					{
						urBuff.remoteGridId = parseAndReleaseS (urNode, "gridUser" );
					}
					if ( urBuff.transType == "") urBuff.transType  = parseAndReleaseS (urNode, "transType" );
					if ( urBuff.iBench == "") urBuff.iBench  = parseAndReleaseS (urNode, "si2k" );
					if ( urBuff.iBench != "" )//backward compatibility
					{
						urBuff.iBenchType = "si2k";
					}
					else
					{
						urBuff.iBench = parseAndReleaseS (urNode, "iBench" );
						urBuff.iBenchType = parseAndReleaseS (urNode, "iBenchType" );
					}
					if ( urBuff.fBench == "") urBuff.fBench = parseAndReleaseS (urNode, "sf2k" );
					if ( urBuff.fBench != "" )//backward compatibility
					{
						urBuff.fBenchType = "sf2k";
					}
					else
					{
						urBuff.fBench = parseAndReleaseS (urNode, "fBench" );
						urBuff.fBenchType = parseAndReleaseS (urNode, "fBenchType" );
					}
					if ( urBuff.acl == "") urBuff.acl = parseAndReleaseS (urNode, "acl" );
					if ( urBuff.id == "") urBuff.id = parseAndReleaseS (urNode, "id" );
					if ( urBuff.lrmsId == "") urBuff.lrmsId = parseAndReleaseS (urNode, "lrmsId" );
					if ( urBuff.localUserId == "") urBuff.localUserId = parseAndReleaseS (urNode, "localUserId" );
					if ( urBuff.hlrGroup == "") urBuff.hlrGroup  = parseAndReleaseS (urNode, "hlrGroup" );
					if ( urBuff.localGroup == "") urBuff.localGroup = parseAndReleaseS (urNode, "localGroup" );
					if ( urBuff.endDate == "") urBuff.endDate = parseAndReleaseS (urNode, "endDate" );
					if ( urBuff.siteName == "") urBuff.siteName = parseAndReleaseS (urNode, "siteName" );
					if ( urBuff.urSourceServer == "") urBuff.urSourceServer = parseAndReleaseS (urNode, "urSourceServer" );
					if ( urBuff.hlrTid == "") urBuff.hlrTid  = parseAndReleaseS (urNode, "hlrTid" );
					if ( urBuff.accountingProcedure == "") urBuff.accountingProcedure = parseAndReleaseS (urNode, "accountingProcedure" );
					if ( urBuff.voOrigin == "") urBuff.voOrigin = parseAndReleaseS (urNode, "voOrigin" );
					if ( urBuff.glueCEInfoTotalCPUs == "") urBuff.glueCEInfoTotalCPUs = parseAndReleaseS (urNode, "glueCEInfoTotalCPUs" );
					if ( urBuff.executingNodes == "") urBuff.executingNodes = parseAndReleaseS (urNode, "executingNodes" );
					if ( urBuff.numNodes == "") urBuff.numNodes = parseAndReleaseS (urNode, "numNodes" );
					if ( urBuff.uniqueChecksum == "") urBuff.uniqueChecksum = parseAndReleaseS (urNode, "uniqueChecksum" );

					//go on with records here...
					logBuff = "<--" + urBuff.dgJobId;
					hlr_log(logBuff,&logStream,7);
					r.push_back(urBuff);
					urNode.release();
				}
				else
				{
					haveRecords = false;
				}
			}
			nodeBuff.release();
		}
	}
	logBuff = "Inserted n. ";
	logBuff += int2string(r.size()); 
	logBuff += " records.";
	hlr_log(logBuff,&logStream,5);
	logBuff = "Exiting parser.";
	hlr_log(logBuff,&logStream,7);
	return 0;
}

int urConcentrator::errorComposeXml(int errorCode)
{

	*output = "<HLR type=\"urConcentrator_answer\">\n";	
	*output += "<BODY>\n";
	*output +="<STATUS>";
	*output += int2string(errorCode);
	*output +="</STATUS>\n";
	*output += "<errMsg>";
	*output += "Server encountered an error! (Sorry will specify better in future releases!, You'll need to look for the exit status.)";
	*output += "</errMsg>";
	*output += "</BODY>\n</HLR>\n";
	return 0;
}

int urConcentrator::getIndex(urConcentratorIndex& indexEntry)
{
	//get index entry for server calling this service (info
	//taken from connInfo connection context)
	int res = 0;
	string logBuff = "Entering getIndex()";	
	hlr_log(logBuff,&logStream,4);
	string queryString = "SELECT * FROM urConcentratorIndex WHERE ";
	queryString += "urSourceServer='";
	queryString += c->hostName;
	queryString += "'";
	hlrGenericQuery getIndex(queryString);
	res = getIndex.query();
	if ( res != 0 )
	{
		//error in query
		if ( res == 1 )//NO_RECORD hlrGenericQuery.cpp !!
		{
			indexEntry.remoteRecordId = "";
			indexEntry.recordDate = "";
			indexEntry.recordInsertDate ="";
			return 0;
		}
		else
		{
			return E_URCONCENTRATOR_INDEX_DB_ERROR;
		}
	}
	else
	{

		indexEntry.urSourceServer = ((getIndex.queryResult).front())[0];
		indexEntry.urSourceServerDN = ((getIndex.queryResult).front())[1];
		indexEntry.remoteRecordId = ((getIndex.queryResult).front())[2];
		indexEntry.recordDate = ((getIndex.queryResult).front())[3];
		indexEntry.recordInsertDate = ((getIndex.queryResult).front())[4];
		indexEntry.uniqueChecksum = ((getIndex.queryResult).front())[5];
		return 0;
	}


}

int urConcentrator::insertRecords(vector<jobTransSummary>& r)
{
	insertedRecords =0;
	//IMPORTANT should exit with !=0 just in case no records have
	//been inserted at all.
	string logBuff = "Entering insertRecords";
	hlr_log(logBuff,&logStream,7);
	lastInsertedId = "-1";
	db hlrDb (hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	vector<jobTransSummary>::iterator it = r.begin();
	vector<jobTransSummary>::iterator r_end = r.end();
	while ( it != r_end )
	{
		if ( insertRecord(hlrDb,*it) != 0 )
		{
			//error encountered. check if some record was
			//inserted.
			if ( lastInsertedId == "-1")
			{
				logBuff = "Could not insert records.";
				hlr_log(logBuff,&logStream,1);
				return E_URCONCENTRATOR_INSERT_RECORDS;
			}
		}
		else
		{
			//update lastInsertedId with the id of the just
			//inserted record.
			lastInsertedId = (*it).id;
			lastInsertedRecordDate = (*it).date;
			lastInsertedUniqueChecksum = (*it).uniqueChecksum;
			insertedRecords++;
		}
		it++;
	}
	return 0;
}

int urConcentrator::bulkInsertRecords(vector<jobTransSummary>& r)
{
	insertedRecords = 0;
	//FIXME go on with bulk inserts.
	//IMPORTANT should exit with !=0 just in case no records have
	//been inserted at all.
	string logBuff = "Entering bulkInsertRecords";
	hlr_log(logBuff,&logStream,7);

	lastInsertedId = "-1";
	string lastInsertedIdBuff;
	string lastInsertedRecordDateBuff;
	string lastInsertedUniqueChecksumBuff;
	db hlrDb (hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	vector<jobTransSummary>::iterator it = r.begin();
	vector<jobTransSummary>::iterator r_end = r.end();
	insertValuesBuffer.reserve(recordsPerBulkInsert);
	while ( it != r_end )
	{
		insertValuesBuffer.clear();
		logBuff = "Records to be inserted this bulk step:" + int2string(recordsPerBulkInsert);
		hlr_log(logBuff,&logStream,8);
		for (int i=0; (i < recordsPerBulkInsert) && (it != r.end()); i++ )
		{
			bulkInsertRecord(hlrDb,*it);
			lastInsertedIdBuff = (*it).id;
			lastInsertedRecordDateBuff = (*it).date;
			lastInsertedUniqueChecksumBuff = (*it).uniqueChecksum;
			it++;
		}
		logBuff = "Records received for this bulk step:" + int2string(insertValuesBuffer.size());
		hlr_log(logBuff,&logStream,8);
		string valuesString;
		valuesString.reserve(4096*(insertValuesBuffer.size()));
		vector<string>::iterator valuesIt = insertValuesBuffer.begin();
		vector<string>::iterator valuesIt_begin = insertValuesBuffer.begin();
		vector<string>::iterator valuesIt_end = insertValuesBuffer.end();
		while ( valuesIt !=  valuesIt_end )
		{
			if ( valuesIt != valuesIt_begin )
			{
				valuesString += ",";
			}
			valuesString += *valuesIt;
			valuesIt++;
		}
		string insertQueryString = "INSERT IGNORE INTO jobTransSummary VALUES "+ valuesString;
		logBuff = "QUERY:" + insertQueryString;
		hlr_log(logBuff,&logStream,8);
		dbResult result = hlrDb.query(insertQueryString);
		if ( hlrDb.errNo == 0 )
		{
			long long affectedRowsBuff = hlrDb.getAffectedRows();
			logBuff = "Records inserted this bulk step:" + int2string(affectedRowsBuff);
			hlr_log(logBuff,&logStream,6);
			insertedRecords += affectedRowsBuff;
			logBuff = "Total records inserted from message so far :" + int2string(insertedRecords);
			hlr_log(logBuff,&logStream,5);
			logBuff = "Total records received in current message:" + int2string(r.size());
			hlr_log(logBuff,&logStream,5);
			lastInsertedId = lastInsertedIdBuff;
			lastInsertedRecordDate = lastInsertedRecordDateBuff;
			lastInsertedUniqueChecksum = lastInsertedUniqueChecksumBuff;
		}
		else
		{
			logBuff = "WARNING Could not insert records: ErrMsg:" + hlrDb.errMsg;
			hlr_log(logBuff,&logStream,3);
		}
	}

	//TODO go on from here. compose query and execute it. Then check for errors and update
	//to last correctly inserted checksum for this sourceServer.
	//error encountered. check if some record was
	//inserted.
	if ( insertedRecords == 0 )
	{
		logBuff = "Could not insert records.";
		hlr_log(logBuff,&logStream,1);
		return E_URCONCENTRATOR_INSERT_RECORDS;
	}
	return 0;
}

/*
int urConcentrator::checkExistsOutOfBand(jobTransSummary& r, string& recordId)
{
	int res = 0;
	string logBuff;
	string queryString = "SELECT id,dgJobId,lrmsId,start,urSourceServer FROM jobTransSummary WHERE ";
	queryString += "lrmsId = " + r.lrmsId;
	queryString += "AND start = "
}
 */

/*
urConcentrator::checkResubmission(jobTransSummary& r)
{
	//it is possible that a resubmission lands on a different CE (and thus
	//on a different HLR) than the original job. Thus two HLRs would have
	//two distinct job with the same dgJobId. This occourrence needs to be checked.
	//If in the Database a record with the same dgJobId already exists, if the
	//lrmsId are different it must be a resubmission, otherwis it is a duplication.
	//overhead should be low since dgJobId is a DB Key.
}
 */

int urConcentrator::insertRecord(db& hlrDb, jobTransSummary& r)
{
	int res = 0;
	string logBuff;
	//string recordId;//taken from id field in jobTransSummary.
	/*
	if ( checkExistsOutOfBand(r, recordId) )
	{
		if ( removeRecord(recordId) != 0 )
		{
			logBuff = "Could not remove old  outOfband.";
		}
	}
	 */
	string tableName = "jobTransSummary";
#ifdef MERGE
	if ( useMergeTables )
	{

		database hlrDb ( hlr_sql_server, 
				hlr_sql_user,
				hlr_sql_password,
				hlr_sql_dbname);
		//build records_XXXXYY from date
		string year,month;
		year = (r.date).substr(0,4);
		month = (r.date).substr(5,2);
		string buff = "records_"+year+month;
		table recordsBuff(hlrDb,buff);
		//see if records_XXXXYY exists
		if ( recordsBuff.exists() )
		{
			tableName = buff;
		}	
		else
		{
			//otherwise see if oldRcords exists
			buff = "oldRecords";
			table oldRecordsBuff(hlrDb,buff);
			if ( oldRecordsBuff.exists() )
			{
				tableName = buff;
			}
		}
		//otherwise use jobTransSummary
		logBuff = "Writing records with date " + r.date;
		logBuff += " into table " + tableName;
		hlr_log(logBuff,&logStream,9);
	} 
#endif
	//check if uniqueChecksum is already present, otherwise calculate it now.
	if ( r.uniqueChecksum == "" )
	{
		ATM_job_record usageInfo;
		usageInfo.res_grid_id = r.thisGridId;
		usageInfo.lrmsId = r.lrmsId;
		usageInfo.start = r.start;
		usageInfo.wall_time = atoi((r.wallTime).c_str());
		usageInfo.cpu_time = atoi((r.cpuTime).c_str());
		makeUniqueChecksum(usageInfo,r.uniqueChecksum);
		logBuff = "uniqueChecksum: dgJobId:" + r.dgJobId;
		logBuff += ",uniqueChecksum:" + r.uniqueChecksum;
		hlr_log(logBuff,&logStream,9);
	}
	string queryString = "INSERT INTO " + tableName + " VALUES ('";
	queryString += r.dgJobId + "','";
	queryString += r.date + "','";
	queryString += r.thisGridId + "','";
	queryString += r.remoteGridId + "','";
	queryString += r.userFqan + "','";
	queryString += r.userVo + "',";
	queryString += r.cpuTime + ",";
	queryString += r.wallTime + ",";
	queryString += r.pmem + ",";
	queryString += r.vmem + ",";
	queryString += r.amount + ",";
	queryString += r.start + ",";
	queryString += r.end + ",'";
	queryString += r.iBench + "','";
	queryString += r.iBenchType + "','";
	queryString += r.fBench + "','";
	queryString += r.fBenchType + "','";
	queryString += r.acl + "',";
	queryString += "0,'";
	queryString += r.lrmsId + "','";
	queryString += r.localUserId + "','";
	queryString += r.hlrGroup + "','";
	queryString += r.localGroup + "','";
	queryString += r.endDate + "','";
	queryString += r.siteName + "','";
	queryString += c->hostName + "','";
	queryString += r.hlrTid + "','";
	queryString += r.accountingProcedure + "','";
	queryString += r.voOrigin + "','";
	queryString += r.glueCEInfoTotalCPUs + "','";
	queryString += r.executingNodes+ "',";
	queryString += r.numNodes+ ",'";
	queryString += r.uniqueChecksum + "')";
	dbResult result = hlrDb.query(queryString);
	if ( hlrDb.errNo == 0 )
	{
		if ( result.numRows() == 0 )
		{
			res = 1;
		}
		res = 0;
	}
	else
	{
		if ( hlrDb.errNo != 0 )
		{
			res = hlrDb.errNo;
		}
		else
		{
			res = -1;
		}
	}
	if ( (res != 0) && ( res != 1 ) )
	{
		if ( isDuplicateEntry(r.dgJobId, c->hostName, r.transType ) )//duplicate entry
		{
			logBuff = "Dupl:" + r.dgJobId + ":" + r.transType + ",MysqlErr:" + int2string(res);
			hlr_log(logBuff,&logStream,4);
			hlr_log("going on anyway...",&logStream,9);
			return 0;	
		}
		logBuff = "Error in query:" + queryString + ":" + int2string(res);
		hlr_log(logBuff,&logStream,1);
		return E_URCONCENTRATOR_INSERT_RECORDS;
	}
	else
	{
		return 0;	
	}
}

int urConcentrator::bulkInsertRecord(db& hlrDb, jobTransSummary& r)
{
	int res = 0;
	string logBuff;
	//string recordId;//taken from id field in jobTransSummary.
	/*
	if ( checkExistsOutOfBand(r, recordId) )
	{
		if ( removeRecord(recordId) != 0 )
		{
			logBuff = "Could not remove old  outOfband.";
		}
	}
	 */
	string tableName = "jobTransSummary";
#ifdef MERGE
	if ( useMergeTables )
	{

		database hlrDb ( hlr_sql_server,
				hlr_sql_user,
				hlr_sql_password,
				hlr_sql_dbname);
		//build records_XXXXYY from date
		string year,month;
		year = (r.date).substr(0,4);
		month = (r.date).substr(5,2);
		string buff = "records_"+year+month;
		table recordsBuff(hlrDb,buff);
		//see if records_XXXXYY exists
		if ( recordsBuff.exists() )
		{
			tableName = buff;
		}
		else
		{
			//otherwise see if oldRcords exists
			buff = "oldRecords";
			table oldRecordsBuff(hlrDb,buff);
			if ( oldRecordsBuff.exists() )
			{
				tableName = buff;
			}
		}
		//otherwise use jobTransSummary
		logBuff = "Writing records with date " + r.date;
		logBuff += " into table " + tableName;
		hlr_log(logBuff,&logStream,9);
	}
#endif
	//check if uniqueChecksum is already present, otherwise calculate it now.
	if ( r.uniqueChecksum == "" )
	{
		ATM_job_record usageInfo;
		usageInfo.res_grid_id = r.thisGridId;
		usageInfo.lrmsId = r.lrmsId;
		usageInfo.start = r.start;
		usageInfo.wall_time = atoi((r.wallTime).c_str());
		usageInfo.cpu_time = atoi((r.cpuTime).c_str());
		makeUniqueChecksum(usageInfo,r.uniqueChecksum);
		logBuff = "uniqueChecksum: dgJobId:" + r.dgJobId;
		logBuff += ",uniqueChecksum:" + r.uniqueChecksum;
		hlr_log(logBuff,&logStream,9);
	}
	string queryString = "('";
	queryString += r.dgJobId + "','";
	queryString += r.date + "','";
	queryString += r.thisGridId + "','";
	queryString += r.remoteGridId + "','";
	queryString += r.userFqan + "','";
	queryString += r.userVo + "',";
	queryString += r.cpuTime + ",";
	queryString += r.wallTime + ",";
	queryString += r.pmem + ",";
	queryString += r.vmem + ",";
	queryString += r.amount + ",";
	queryString += r.start + ",";
	queryString += r.end + ",'";
	queryString += r.iBench + "','";
	queryString += r.iBenchType + "','";
	queryString += r.fBench + "','";
	queryString += r.fBenchType + "','";
	queryString += r.acl + "',";
	queryString += "0,'";
	queryString += r.lrmsId + "','";
	queryString += r.localUserId + "','";
	queryString += r.hlrGroup + "','";
	queryString += r.localGroup + "','";
	queryString += r.endDate + "','";
	queryString += r.siteName + "','";
	queryString += c->hostName + "','";
	queryString += r.hlrTid + "','";
	queryString += r.accountingProcedure + "','";
	queryString += r.voOrigin + "','";
	queryString += r.glueCEInfoTotalCPUs + "','";
	queryString += r.executingNodes + "',";
	queryString += r.numNodes + ",'";
	queryString += r.uniqueChecksum + "')";
	insertValuesBuffer.push_back(queryString);
	return 0;
}

bool urConcentrator::isDuplicateEntry(string& dgJobId, string& hostName, string& transType )
{
	string queryString = "SELECT dgJobId FROM jobTransSummary WHERE";
	queryString += " dgJobId=\"" + dgJobId + "\"";
	hlrGenericQuery check(queryString);
	check.query();
	if ( check.Rows() >=1 )
	{
		return true;
	}
	else
	{
		return false;
	}
}

int urConcentrator::updateIndex(urConcentratorIndex& indexEntry)
{
	int res;
	string logBuff = "Entering updateIndex(";
	logBuff += indexEntry.urSourceServer + ",";
	logBuff += indexEntry.urSourceServerDN + ",";
	logBuff += indexEntry.remoteRecordId + ",";
	logBuff += indexEntry.recordDate + ")";
	hlr_log(logBuff,&logStream,4);
	string queryString = "REPLACE INTO  urConcentratorIndex VALUES ('";
	queryString += indexEntry.urSourceServer + "','";
	queryString += indexEntry.urSourceServerDN + "','";
	queryString += indexEntry.remoteRecordId + "','";
	queryString += indexEntry.recordDate + "',";
	queryString += "NOW(),'";
	queryString += indexEntry.uniqueChecksum + "')";
	hlrGenericQuery updateIndex(queryString);	
	res = updateIndex.query();
	if ( ( res != 0 ) && ( res != 1 ) )
	{
		logBuff = "Error in query:" + queryString + " exited with:";
		logBuff += int2string(res);
		hlr_log(logBuff,&logStream,1);

	}
	return res;
}

int urConcentrator::infoRequestComposeXml(urConcentratorIndex& indexEntry)
{
	*output = "<HLR type=\"urConcentrator_answer\">\n";
	*output += "<BODY>\n";
	*output += tagAdd( "acceptRecordsStartDate", acceptRecordsStartDate );
	*output += tagAdd( "acceptRecordsEndDate", acceptRecordsEndDate );
	*output += tagAdd( "recordsPerConnection", recordsPerConnection );
	*output += tagAdd( "urSourceServer", indexEntry.urSourceServer );
	*output += tagAdd( "urSourceServerDN", indexEntry.urSourceServerDN );
	*output += tagAdd( "remoteRecordId", indexEntry.remoteRecordId );
	*output += tagAdd( "recordDate", indexEntry.recordDate );
	*output += tagAdd( "recordInsertDate", indexEntry.recordInsertDate );
	*output += tagAdd( "uniqueChecksum", indexEntry.uniqueChecksum );
	*output += tagAdd( "serverVersion", VERSION );
	*output += tagAdd( "STATUS", "0" );
	*output += "</BODY>\n</HLR>\n";
	return 0;
}

int urConcentrator::insertRequestComposeXml()
{
	*output = "<HLR type=\"urConcentrator_answer\">\n";
	*output += "<BODY>\n";
	*output += tagAdd( "lastInsertedId", lastInsertedId );
	*output += tagAdd( "lastInsertedUniqueChecksum", lastInsertedUniqueChecksum );
	*output += tagAdd( "STATUS", "0" );
	*output += "</BODY>\n</HLR>\n";
	return 0;
}


int urConcentratorEngine( string &input, connInfo &connectionInfo, string *output )
{
	string logBuff = "urSourceServer:" + connectionInfo.hostName;
	hlr_log(logBuff,&logStream,4); 
	logBuff = "urSourceServerDN:" + connectionInfo.contactString;
	hlr_log(logBuff,&logStream,4); 
	urConcentrator u(&input, &connectionInfo, output);
	return u.run();
}
