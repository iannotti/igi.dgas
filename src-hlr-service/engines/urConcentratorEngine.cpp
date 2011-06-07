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
		logBuff = "Message containing " + int2string(r.size()) + " records processed in " + int2string(deltaT);
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
			while ( urNode.status == 0 )
			{
				jobTransSummary urBuff;
				urNode = parse (&nodeBuff.text, "record" );
				if ( urNode.status == 0 )
				{
					node fieldNode;	
					fieldNode = parseAndRelease (urNode, "dgJobId" );
					if ( fieldNode.status == 0 )
					{
						urBuff.dgJobId = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "date" );
					if ( fieldNode.status == 0 )
					{
						urBuff.date = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "transType" );
					if ( fieldNode.status == 0 )
					{
						urBuff.transType = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "thisGridId" );//backward Compatibility
					if ( fieldNode.status == 0 )
					{
						urBuff.thisGridId = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "gridResource" );
					if ( fieldNode.status == 0 )
					{
						urBuff.thisGridId = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "remoteGridId" );//backward Compatibility
					if ( fieldNode.status == 0 )
					{
						urBuff.remoteGridId = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "gridUser" );
					if ( fieldNode.status == 0 )
					{
						urBuff.remoteGridId = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "userFqan" );
					if ( fieldNode.status == 0 )
					{
						urBuff.userFqan = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "userVo" );
					if ( fieldNode.status == 0 )
					{
						urBuff.userVo = fieldNode.text;
					}
					//					fieldNode = parse (&urNode.text, "remoteHlr" );
					//					if ( fieldNode.status == 0 )
					//					{
					//						urBuff.remoteHlr = fieldNode.text;
					//					}
					fieldNode = parse (&urNode.text, "cpuTime" );
					if ( fieldNode.status == 0 )
					{
						urBuff.cpuTime = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "wallTime" );
					if ( fieldNode.status == 0 )
					{
						urBuff.wallTime = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "pmem" );
					if ( fieldNode.status == 0 )
					{
						urBuff.pmem = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "vmem" );
					if ( fieldNode.status == 0 )
					{
						urBuff.vmem = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "amount" );
					if ( fieldNode.status == 0 )
					{
						urBuff.amount = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "start" );
					if ( fieldNode.status == 0 )
					{
						urBuff.start = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "end" );
					if ( fieldNode.status == 0 )
					{
						urBuff.end = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "iBench" );
					if ( fieldNode.status == 0 )
					{
						urBuff.iBench = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "fBench" );
					if ( fieldNode.status == 0 )
					{
						urBuff.fBench = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "iBenchType" );
					if ( fieldNode.status == 0 )
					{
						urBuff.iBenchType = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "fBenchType" );
					if ( fieldNode.status == 0 )
					{
						urBuff.fBenchType = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "si2k" );
					if ( fieldNode.status == 0 )//backward compatibility
					{
						urBuff.iBench = fieldNode.text;
						urBuff.iBenchType = "si2k";
					}
					fieldNode = parse (&urNode.text, "sf2k" );
					if ( fieldNode.status == 0 )//backward compatibility
					{
						urBuff.fBench = fieldNode.text;
						urBuff.fBenchType = "sf2k";
					}
					fieldNode = parse (&urNode.text, "acl" );
					if ( fieldNode.status == 0 )
					{
						urBuff.acl = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "id" );
					if ( fieldNode.status == 0 )
					{
						urBuff.id = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "lrmsId" );
					if ( fieldNode.status == 0 )
					{
						urBuff.lrmsId = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "localUserId" );
					if ( fieldNode.status == 0 )
					{
						urBuff.localUserId = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "hlrGroup" );
					if ( fieldNode.status == 0 )
					{
						urBuff.hlrGroup = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "localGroup" );
					if ( fieldNode.status == 0 )
					{
						urBuff.localGroup = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "endDate" );
					if ( fieldNode.status == 0 )
					{
						urBuff.endDate = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "siteName" );
					if ( fieldNode.status == 0 )
					{
						urBuff.siteName = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "urSourceServer" );
					if ( fieldNode.status == 0 )
					{
						urBuff.urSourceServer = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "hlrTid" );
					if ( fieldNode.status == 0 )
					{
						urBuff.hlrTid = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "accountingProcedure" );
					if ( fieldNode.status == 0 )
					{
						urBuff.accountingProcedure = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "voOrigin" );
					if ( fieldNode.status == 0 )
					{
						urBuff.voOrigin = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "glueCEInfoTotalCPUs" );
					if ( fieldNode.status == 0 )
					{
						urBuff.glueCEInfoTotalCPUs = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "executingNodes" );
					if ( fieldNode.status == 0 )
					{
						urBuff.executingNodes = fieldNode.text;
					}
					fieldNode = parse (&urNode.text, "uniqueChecksum" );
					if ( fieldNode.status == 0 )
					{
						urBuff.uniqueChecksum = fieldNode.text;
					}
					//go on with records here...
					logBuff = "<--" + urBuff.dgJobId;
					hlr_log(logBuff,&logStream,7);
					r.push_back(urBuff);
					urNode.release();
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
	*output += "Server encountered an error! (Sorry will specify better in future releases!, You'll net to look for the exit status.)";
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
	while ( it != r.end() )
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
	while ( it != r.end() )
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
		vector<string>::iterator valuesIt = insertValuesBuffer.begin();
		while ( valuesIt != insertValuesBuffer.end() )
		{
			if ( valuesIt != insertValuesBuffer.begin() )
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
	if ( checkVo )
	{
		string buff = "";
		r.userVo = checkUserVo(r.userVo,
				r.userFqan,
				r.localUserId,
				buff);
		if ( buff != "" )
		{
			hlr_log (buff, &logStream,4);
		}

	}
	string recordId;//taken from id field in jobTransSummary.
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
	queryString += r.executingNodes+ "','";
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
	if ( checkVo )
	{
		string buff = "";
		r.userVo = checkUserVo(r.userVo,
				r.userFqan,
				r.localUserId,
				buff);
		if ( buff != "" )
		{
			hlr_log (buff, &logStream,4);
		}

	}
	string recordId;//taken from id field in jobTransSummary.
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
	queryString += r.executingNodes+ "','";
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
