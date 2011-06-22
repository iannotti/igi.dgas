#include "dbWaitress.h"

int database::lock()
{
	std::fstream cfStream(lockFile.c_str(), ios::out);
	if ( !cfStream )
	{
		hlr_log("Could not lock database.", &logStream, 8);
		return 1;
	}
	else
	{
		hlr_log("Database locked.", &logStream, 8);
		cfStream.close();
		return 0;
	}
}

int database::unlock()
{
	hlr_log("Un-lock database.", &logStream, 8);
	return unlink ( lockFile.c_str());
}

bool database::locked()
{
	ifstream cfStream (lockFile.c_str(), ios::in);
	if ( !cfStream )
	{
		hlr_log("Database is not locked.", &logStream, 8);
		return false;
	}	
	else
	{
		hlr_log("Database is locked.", &logStream, 8);
		cfStream.close();
		return true;
	}
}




int JTS::create()
{
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	dbResult result = hlrDb.query(tableDef);
	std::string logBuff = "Creating table with definition: " + tableDef;
	hlr_log(logBuff, &logStream, 7);
	return hlrDb.errNo;	
}

int mergeTables::getDef()
{
	return readDefFile();
}

int mergeTables::readDefFile()
{
	std::ifstream inFile;
	inFile.open(defFile.c_str(), fstream::in);
	//make sure jobTransSummary is ALWAYS present (to ensure backward compatibility)
	std::string jobTransSummary = "jobTransSummary:{ALL}";
	definitions.push_back(jobTransSummary);
	if ( !inFile )
	{
		string logBuff = "Error opening definition file:" + defFile;
		hlr_log(logBuff, &logStream, 3);
		logBuff += "Just jobTransSummary merge definition will be present";
		hlr_log(logBuff, &logStream, 3);
		return 0;
	}
	std::string textLine;
	std::string logBuff = "Reading merge definitions from:" + defFile;
	hlr_log(logBuff, &logStream, 6);
	while ( getline (inFile, textLine, '\n') )
	{
		if (textLine[0] == '#')
		{
			continue;
		}		
		hlr_log(textLine, &logStream, 6);
		definitions.push_back(textLine);
	}
	inFile.close();
	return 0;
}

int mergeTables::produceSqlUnionDefinition(std::string& def, std::string& unionDef)
{
	std::string unionBuff = "";
	std::string::size_type pos = def.find("{");
	if ( pos != std::string::npos )
	{
		string::size_type pos1=0;
		pos1 = def.find("}");
		if ( pos1 != std::string::npos )
		{
			std::string definition = def.substr(pos+1,(pos1-pos)-1);
			std::string logBuff = "found Definition==" + definition;
			hlr_log (logBuff,&logStream,8);
			if ( definition == "ALL" )
			{
				//oldRecords + all records_* tables ordered by date;
				db hlrDb ( DB.sqlServer,
						DB.sqlUser,
						DB.sqlPassword,
						DB.sqlDbName );
				std::string findOldRecords = "SHOW TABLES LIKE \"oldRecords\"";
				hlr_log(findOldRecords, &logStream, 9);
				dbResult result0 = hlrDb.query(findOldRecords);
				if ( hlrDb.errNo == 0 )
				{
					if ( result0.numRows() == 1 )
					{
						//add oldRecords as the first
						//table in the merge.
						unionBuff += "`oldRecords`";
					}
				}
				else
				{	
					return E_DBW_DBERR;
				}
				std::string findTables = "SHOW TABLES LIKE \"records_%\"";
				hlr_log(findTables, &logStream, 9);
				dbResult result1 = hlrDb.query(findTables);
				if ( hlrDb.errNo == 0 )
				{
					if ( result1.numRows() > 0 )
					{
						int numRows = result1.numRows();
						//found records_ tables;
						//append to unionBuff;
						for (int i=0; i<numRows; i++)
						{
							if ( result1.getItem(i,0) != "" )
							{
								unionBuff += ",";
								unionBuff += "`" + result1.getItem(i,0) + "`";
							}
						}
					}
				}
				else
				{
					return E_DBW_DBERR;
				}

			}
			if ( definition.at(0) == '-' ) 
			{
				pos = definition.find_first_of("0123456789");
				pos1 = definition.find_last_of("0123456789");
				string months = definition.substr(pos,pos1-pos+1);
				int m = atoi(months.c_str());
				//here add the {-M} def handler 
				db hlrDb ( DB.sqlServer,
						DB.sqlUser,
						DB.sqlPassword,
						DB.sqlDbName );
				std::string findTables = "SHOW TABLES LIKE \"records_%\"";
				hlr_log(findTables, &logStream, 9);
				dbResult result = hlrDb.query(findTables);
				if ( hlrDb.errNo == 0 )
				{
					if ( result.numRows() > 0 )
					{
						int numRows = result.numRows();
						//found records_ tables;
						//append last n Months to unionBuff;
						for (int i=numRows-m; i<numRows; i++)
						{
							if ( result.getItem(i,0) != "" )
							{
								unionBuff += "`" + result.getItem(i,0) + "`";
								if ( i != numRows-1 )
								{
									unionBuff += ",";
								}
							}
						}
					}
				}
				else
				{
					return E_DBW_DBERR;
				}

			}
		}
		else
		{
			return E_DBW_BADDEF;
		}
	}
	else
	{
		return E_DBW_BADDEF;
	}
	if ( unionBuff != "" )
	{	
		//compose union definition
		unionDef = "ENGINE=MRG_MyISAM UNION=(" + unionBuff + ") ";
		unionDef += "INSERT_METHOD=LAST";
		return 0;
	}
	else
	{
		return E_DBW_BADDEF;
	}
}

int mergeTables::createMergeTable(std::string& mergeTableName, std::string& unionDef)
{
	hlr_log("createMergeTable", &logStream, 9);
	hlr_log(mergeTableName, &logStream, 9);
	hlr_log(unionDef, &logStream, 9);
	//FIXME if tableName exists
	//retrieve union definition from existing table and check against
	//new union definition. If they match. do nothing, if they don't drop 
	//the existing one and create the new table. 
	table currentTable( DB, mergeTableName );
	if ( currentTable.exists() )
	{
		hlr_log("Table wxists. Comparing with new UNION definition.",&logStream,8);
		std::string newUnionDef;
		std::string existingUnionDef;
		db hlrDb ( DB.sqlServer,
				DB.sqlUser,
				DB.sqlPassword,
				DB.sqlDbName );
		std::string showCreate = "SHOW CREATE TABLE ";
		showCreate += mergeTableName;
		dbResult result = hlrDb.query(showCreate);
		if ( hlrDb.errNo == 0 )
		{
			size_t pos = (result.getItem(0,1)).rfind("UNION=");
			size_t posE = (result.getItem(0,1)).find_first_of(")",pos);
			if ( (pos != string::npos) && (posE != string::npos) ) 
			{
				existingUnionDef = (result.getItem(0,1)).substr(pos,posE-pos+1);
			}
			else
			{
				hlr_log("Current table is not a merge table.",&logStream,8);
				return E_DBW_GETUDEF;	
			}
		}
		else
		{
			return E_DBW_DBERR;
		}
		size_t pos = unionDef.rfind("UNION=");
		size_t posE = unionDef.find_first_of(")",pos);
		if ( (pos != string::npos) && (posE != string::npos) ) 
		{
			newUnionDef = unionDef.substr(pos,posE-pos+1);
		}
		else
		{
			hlr_log("Error within new UNION definition.",&logStream,8);
			return E_DBW_GETUDEF;	
		}
		std::string logBuff = "Checking " + newUnionDef + " against " + existingUnionDef;
		hlr_log(logBuff,&logStream,7);
		if ( newUnionDef == existingUnionDef )
		{
			//the union definition did not change so do nothing.
			return 0;
		}
		else
		{
			//drop current table definition.
			std::string dropTable = "DROP TABLE ";
			dropTable += mergeTableName;
			dbResult result1 = hlrDb.query(dropTable);
			if ( hlrDb.errNo != 0 )
			{
				return E_DBW_DBERR;
			}
		}

	}
	JTS _jts(DB, mergeTableName, is2ndLevelHlr ,unionDef);
	return _jts.create();
}

int mergeTables::dropMergeTable(std::string& mergeTableName)
{
	//check that this IS a MERGE table first!
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	std::string showCreate = "show create table ";
	showCreate += mergeTableName;
	dbResult result = hlrDb.query(showCreate);
	if ( hlrDb.errNo == 0 )
	{
		size_t pos = (result.getItem(0,1)).rfind("ENGINE=MRG_MyISAM");
		if ( pos != string::npos )
		{
			//Ok this IS a MERGE table go on dropping.
			std::string dropTable = "DROP TABLE ";
			dropTable += mergeTableName;
			dbResult result1 = hlrDb.query(dropTable);
			if ( hlrDb.errNo != 0 )
			{
				hlr_log("error: could not drop merge table.",&logStream,3);
				return E_DBW_DBERR;
			}
			else
			{
				return 0;
			}
		}
		else
		{
			hlr_log("warning: this is not a merge table.",&logStream,3);
			return 0;
		}
	}
	else
	{
		hlr_log("Error retrieving table schema.",&logStream,3);
		return E_DBW_DBERR;
	}
}

int recordsTables::dropAll()
{
	hlr_log("Dropping all MyISAM records tables.",&logStream,7);
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	std::string findTables = "SHOW TABLES LIKE \"records_%\"";
	hlr_log(findTables, &logStream, 9);
	dbResult result = hlrDb.query(findTables);
	int numRows = result.numRows();
	for ( int i = 0; i < numRows; i++ )
	{
		//iterate over records_tables and drop them all.
		table t(DB,result.getItem(i,0));
		t.drop();
	}
	table oldRecords(DB,"oldRecords");
	oldRecords.drop();
	return 0;
}

int mergeTables::create()
{
	hlr_log("Creating merge tables.", &logStream, 9);
	int res = 0;
	ofstream mtf;
	mtf.open(mergeTablesFile.c_str(), fstream::out );
	if ( !mtf )
	{

		hlr_log("Error opening merge tables file.", &logStream, 3);
		return E_DBW_OPENFILE;
	}
	hlr_log("Reading file definition.", &logStream, 9);
	res = readDefFile();
	if ( res != 0 )
	{
		mtf.close();
		return res;
	}	
	if ( reset )
	{
		hlr_log("User requested a reset of the merge tables.", &logStream, 4);
		res = drop();
		if ( res == E_DBW_DROPMTABLE )
		{
			//there was an error dropping the merge tables;
			hlr_log("Error dropping merge tables.", &logStream, 3);
			return res;
		}
	}
	std::vector<std::string>::iterator it = definitions.begin();
	hlr_log("Iterating over tables def.", &logStream, 9);
	while ( it != definitions.end() )
	{
		hlr_log(*it, &logStream, 9);
		vector<string> defBuffV;
		Split(':',*it,&defBuffV);
		if ( defBuffV.size() < 2 )
		{
			hlr_log("Bad def.", &logStream, 9);
			mtf.close();
			return E_DBW_BADDEF;
		}
		string tableName = defBuffV[0];
		string tableDef = defBuffV[1];
		string unionDef;
		res = produceSqlUnionDefinition(tableDef, unionDef);
		if ( res == 0 )
		{
			res = createMergeTable(tableName, unionDef);
			if ( res == 0 )
			{
				hlr_log(tableName, &logStream, 9);
				mtf << tableName << endl;
			}
			else
			{
				std::string logBuff = "Error definition for merge table:" + tableName + " Correct this please!";
				hlr_log (logBuff,&logStream,2);
				mtf.close();
				return res;
			}
		}
		else
		{
			mtf.close();
			return res;
			//there where errors, log this. 
		}
		it++;
	}
	mtf.close();
	return res;
}

int mergeTables::exec()
{
	int res = 0;
	std::string yearMonthBuff;
	//lock DB
	if ( DB.lock() != 0 )
	{
		return E_DBW_DBLOCKED;
	}
	//if needed restore MyISAM jobTransSummary.
	res = restoreMyISAMJTS();
	if ( res != 0 )
	{
		string logBuff = "Error restoring jobTransSummary to MyISAM:" + int2string(res);
		hlr_log(logBuff,&logStream,2);
		return E_DBW_RESTORE;
	}
	//get current Year and Month
	res = getYearMonth(DB,yearMonthBuff);
	if ( res != 0 )
	{
		return E_DBW_GETDATE;
	}
	table oldRecords(DB,"oldRecords");
	if ( !oldRecords.exists() )
	{
		table jobTransSummary(DB,"jobTransSummary");
		if ( !jobTransSummary.exists() )
		{
			//jobTransSummary doesn't exists.
			//Since both JTS and oldRecords are not present,
			//this has to be a fresh new DB.
			//create jobTransSummary then go on.
			JTS jobTransSummaryDef(DB,"jobTransSummary", is2ndLevelHlr);
			if ( jobTransSummaryDef.create() != 0 )
			{
				return E_DBW_CREATE;
			}
		}
		//rename jobTransSummary to oldRecords
		res = jobTransSummary.rename("oldRecords");
		if ( res != 0 )
		{
			return res;
		}
		/*
		res = rTables.createPastMonths();
		if ( res != 0 )
		{
			//there was an error creating the records tables
			//for the past months.
			return res;
		}
		 */
	}
	//check if records table for current month exists.
	table currMonthRecords(DB,"records_" + yearMonthBuff );
	if ( !currMonthRecords.exists() )
	{
		recordsTables rTables(DB,is2ndLevelHlr,months);
		res = rTables.createCurrentMonth();
		if ( res != 0 )
		{
			//there was an error creating the records table
			//for the current month.
			hlr_log ("Error creating records table for current month.",&logStream,3);
			return res;
		}
	}	
	hlr_log ("Entering mergeTables::create().",&logStream,8);
	res = create();//mergeTables::create();
	if ( res != 0 )
	{
		hlr_log ("Error in mergeTables::create().",&logStream,8);
		return res;
	}
	return DB.unlock();
}

int mergeTables::restoreMyISAMJTS()
{
	if ( reset )
	{
		hlr_log ("Restoring jobTransSummary.",&logStream,6);
		int res = 0;
		//check that this IS a MERGE table first!
		db hlrDb ( DB.sqlServer,
				DB.sqlUser,
				DB.sqlPassword,
				DB.sqlDbName );
		std::string showCreate = "show create table jobTransSummary";
		dbResult result = hlrDb.query(showCreate);
		if ( hlrDb.errNo == 0 )
		{
			size_t pos = (result.getItem(0,1)).rfind("ENGINE=MRG_MyISAM");
			if ( pos == string::npos )
			{
				hlr_log ("Already MyISAM, nothing to do.",&logStream,6);
				return 0;
			}
		}
		//ok, we can go on. write JTS contents intoa file.
		table jobTransSummary(DB,"jobTransSummary");
		int writtenLines,readLines;
		string whereClause;
		res = jobTransSummary.write(whereClause,writtenLines);
		if ( res != 0 )
		{
			return res;
		}
		res = jobTransSummary.drop();
		if ( res != 0 )
		{
			return res;
		}
		res = drop();
		if ( res != 0 )
		{
			return res;
		}
		res = dropAll();
		if ( res != 0 )
		{
			return res;
		}
		JTS jobTransSummarySchema(DB,"jobTransSummary",is2ndLevelHlr);
		res = jobTransSummarySchema.create();
		if ( res != 0 )
		{
			return res;
		}
		res = jobTransSummary.read(whereClause,readLines);
		if ( res != 0 )
		{
			return res;
		}
		if ( readLines != writtenLines )
		{
			return E_DBW_LINESMISMATCH;
		}
		if ( jobTransSummary.unlinkBuffer() != 0 )
		{
			return E_DBW_UNLINK;
		}
	}
	hlr_log ("jobTransSummary restored to MyISAM.",&logStream,6);
	return 0;
}

int mergeTables::drop()
{
	fstream mtf;
	mtf.open(mergeTablesFile.c_str(), fstream::in | fstream::out );
	if ( !mtf )
	{
		return E_DBW_OPENFILE;
	}
	std::string textLine;
	while ( getline (mtf, textLine, '\n') )
	{
		if ( dropMergeTable(textLine) != 0 )
		{
			std::string logBuff = "Warning: error dropping ";
			logBuff += textLine;
			hlr_log(logBuff,&logStream,3);
		}
	}
	mtf.close();
	return 0;
}

int table::rename(std::string newLabel)
{
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	std::string queryString = "RENAME TABLE " + tableName + " TO ";
	queryString += newLabel;
	hlr_log(queryString, &logStream, 9);
	dbResult result = hlrDb.query(queryString);
	return hlrDb.errNo; 
}

bool table::exists()
{

	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	std::string queryString = "SHOW tables LIKE '" + tableName + "'";
	hlr_log(queryString, &logStream, 9);
	dbResult result = hlrDb.query(queryString);
	if ( hlrDb.errNo == 0 )
	{
		if ( result.numRows() != 0 )
		{
			return true;
		}
	}
	return false;	
}

int table::drop()
{
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	std::string logBuff = "Dropping table: " + tableName;
	hlr_log(logBuff,&logStream,7);
	std::string dropTable = "DROP TABLE " + tableName;
	dbResult result1 = hlrDb.query(dropTable);
	if ( hlrDb.errNo != 0 )
	{
		hlr_log("Warning: Error dropping table",&logStream,3);
		return E_DBW_DBERR;
	}
	else
	{
		return 0;
	}
}

int table::write(std::string& whereClause,int& writtenLines, std::string fileBuff)
{
	int currentNumLines = 0;
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	std::string queryCountString = "SELECT count(*) FROM " + tableName;
	if ( whereClause != "" ) queryCountString += " WHERE " + whereClause;
	hlr_log(queryCountString, &logStream, 9);
	dbResult result = hlrDb.query(queryCountString);
	if ( hlrDb.errNo != 0 )
	{
		return E_DBW_SELECT;	
	}
	else
	{
		if ( result.numRows() != 0 )
		{
			currentNumLines = atoi((result.getItem(0,0)).c_str());
		}
	}
	std::string queryString = "SELECT * FROM " + tableName;
	if ( whereClause != "" ) queryString += " WHERE " + whereClause;
	queryString += " INTO OUTFILE '" + fileBuff + "'";
	queryString += " FIELDS TERMINATED BY \";|\" ENCLOSED BY '\"'";
	hlr_log(queryString, &logStream, 9);
	dbResult writeResult = hlrDb.query(queryString);
	if ( hlrDb.errNo != 0 )
	{
		return E_DBW_WRITE;	
	}
	else
	{
		writtenLines = 0;
		std::ifstream inFile(fileBuff.c_str());
		if ( !inFile )
		{
			return E_DBW_OPENFILE;
		}
		std::string textLine;
		while ( getline (inFile, textLine, '\n') )
		{
			writtenLines++;
		}
		inFile.close();
		if ( writtenLines != currentNumLines )
		{
			return E_DBW_FILEMISMATCH;
		}
	}
	return 0;
}

int table::read(std::string& writeWhereClause,int& readLines ,std::string fileBuff)
{
	int currentNumLines = 0;
	std::ifstream inFile(fileBuff.c_str());
	if ( !inFile )
	{
		return E_DBW_OPENFILE;
	}
	std::string textLine;
	while ( getline (inFile, textLine, '\n') )
	{
		currentNumLines++;
	}
	inFile.close();
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	std::string queryString = "LOAD DATA INFILE '" + fileBuff + "' ";
	queryString += "INTO TABLE " + tableName;
	queryString += " FIELDS TERMINATED BY \";|\" ENCLOSED BY '\"'";
	hlr_log(queryString, &logStream, 9);
	dbResult writeResult = hlrDb.query(queryString);
	if ( hlrDb.errNo != 0 )
	{
		return E_DBW_READ;	
	}
	else
	{
		std::string queryCountString = "SELECT count(*) FROM " + tableName;
		if ( writeWhereClause != "" ) queryCountString += " WHERE " + writeWhereClause;
		hlr_log(queryCountString, &logStream, 9);
		dbResult result = hlrDb.query(queryCountString);
		if ( hlrDb.errNo != 0 )
		{
			return E_DBW_SELECT;	
		}
		else
		{
			if ( result.numRows() != 0 )
			{
				readLines = atoi((result.getItem(0,0)).c_str());
			}
		}

	}
	if ( readLines != currentNumLines )
	{
		return E_DBW_FILEMISMATCH;
	}
	return 0;
}

int table::unlinkBuffer(std::string fileBuff)
{
	return unlink ( fileBuff.c_str() );
}

int table::removeRecords(std::string& whereClause)
{
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	std::string queryString = "DELETE FROM " + tableName;
	queryString += " WHERE " + whereClause;
	hlr_log(queryString, &logStream, 9);
	dbResult ymResult = hlrDb.query(queryString);
	if ( hlrDb.errNo != 0 )
	{
		return E_DBW_DELETE;
	}
	return 0;
}

int table::addIndex( string index, bool primary)
{
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	string queryString;
	if ( primary )
	{
		queryString = "CREATE UNIQUE INDEX " + index +" on " + tableName + " ("+index+ ")";
	}
	else
	{
		queryString = "CREATE INDEX " + index +" on " + tableName + " ("+index+ ")";
	}
	hlr_log(queryString,&logStream,9);
	hlrDb.query(queryString);
	int res = hlrDb.errNo;
	string logBuff = "Exited with:" + int2string(res);
	hlr_log(logBuff,&logStream,9);
	return res;
}

int table::dropIndex(string index)
{
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	string queryString;
	if ( index == "PRIMARY" )
	{
		queryString = "ALTER TABLE " + tableName + " DROP PRIMRY KEY";
	}
	else
	{
		queryString = "ALTER TABLE " + tableName + " DROP INDEX "+index;
	}
	hlr_log(queryString,&logStream,9);
	hlrDb.query(queryString);
	int res = hlrDb.errNo;
	string logBuff = "Exited with:" + int2string(res);
	hlr_log(logBuff,&logStream,9);
	return res;
}

bool table::checkIndex(string index)
{
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	string queryString = "DESCRIBE " + DB.sqlDbName + "." + tableName;
	hlr_log(queryString, &logStream, 9);
	dbResult result = hlrDb.query(queryString);
	if ( hlrDb.errNo == 0 )
	{	
		int numRows = result.numRows();
		for ( int i = 0; i < numRows; i++ )
		{
			if ( result.getItem(i,0) == index )
			{
				if ( result.getItem(i,3) != "" ) return true;
			}
		}	
	}
	return false;
}

int table::disableKeys()
{
	string logBuff = tableName + ": DISABLE KEYS";
	hlr_log(logBuff,&logStream,9);
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	string queryString = "ALTER TABLE " + tableName + " DISABLE KEYS";	
	hlr_log(queryString,&logStream,9);
	hlrDb.query(queryString);
	int res = hlrDb.errNo;
	logBuff = "Exited with:" + int2string(res);
	hlr_log(logBuff,&logStream,9);
	return res;
}

int table::enableKeys()
{
	string logBuff = tableName + ": ENABLE KEYS";
	hlr_log(logBuff,&logStream,9);
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	string queryString = "ALTER TABLE " + tableName + " ENABLE KEYS";	
	hlr_log(queryString,&logStream,9);
	hlrDb.query(queryString);
	int res = hlrDb.errNo;
	logBuff = "Exited with:" + int2string(res);
	hlr_log(logBuff,&logStream,9);
	return res;
}



int table::checkAgainst(string &fieldList)
{
	string queryString = "DESCRIBE " + tableName;
	string logBuff = "DB:" + DB.sqlDbName + ":" + queryString;
	hlr_log(logBuff,&logStream,9);
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	dbResult queryResult = hlrDb.query(queryString);
	if ( hlrDb.errNo == 0 )
	{
		string checkBuffer;
		int numRows = queryResult.numRows();
		for ( int i = 0; i < numRows; i++ )
		{
			checkBuffer += queryResult.getItem(i,0);
		}
		size_t x = fieldList.find(";");
		while ( x < string::npos )
		{
			fieldList.erase(x,1);
			x = fieldList.find(";");
		}
		logBuff = "Checking: " + checkBuffer;
		hlr_log(logBuff,&logStream,9);
		logBuff = "against : " + fieldList;
		hlr_log(logBuff,&logStream,9);
		if ( checkBuffer == fieldList )
		{
			hlr_log("Result: match.",&logStream,9);
			return true;
		}
	}
	hlr_log("Result: don't match.",&logStream,9);
	return false;
}

int getYearMonth(database &DB,std::string& yearMonth, int i)
{
	std::string yearMonthBuff;
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	std::string queryString;
	if ( i != 0 )
	{
		queryString = "SELECT EXTRACT( YEAR_MONTH FROM DATE_SUB( CURDATE(), INTERVAL " + int2string(i) + " MONTH ))";
	}
	else
	{
		queryString = "SELECT EXTRACT( YEAR_MONTH FROM CURDATE())";
	}
	hlr_log(queryString, &logStream, 9);
	dbResult ymResult = hlrDb.query(queryString);
	if ( hlrDb.errNo != 0 )
	{
		return E_DBW_SELECT;
	}
	else
	{
		yearMonth = ymResult.getItem(0,0);
		hlr_log(yearMonth, &logStream, 9);
	}
	return 0;
}

int recordsTables::addIndex(std::string field,std::string indexName)
{
	std::vector<std::string> tables;
	tables.push_back("oldRecords");
	std::string showTables = "SHOW tables LIKE 'records_%'";
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	hlr_log(showTables, &logStream, 9);
	dbResult stResult = hlrDb.query(showTables);
	if ( hlrDb.errNo != 0 )
	{
		return E_DBW_SELECT;
	}
	else
	{
		int numRows = stResult.numRows();
		for ( int i = 0; i< numRows; i++ )
		{
			tables.push_back(stResult.getItem(i,0));
		}
	}
	std::vector<std::string>::iterator it = tables.begin();
	while ( it != tables.end() )
	{
		std::string queryString = "CREATE INDEX " + indexName + " ON " + *it + " (" + field + ")";
		dbResult queryResult = hlrDb.query(queryString);
		it++;
	} 
	return 0;
}

int recordsTables::createPastMonths()
{
	int res = 0;
	std::string yearMonthBuff,currYear,currMonth;
	for ( int i = 1; i <= monthsNumber; i++ )
	{
		std::string logBuff = "pastMonth:" + int2string(i);
		hlr_log(logBuff,&logStream,6);
		res = getYearMonth(DB,yearMonthBuff,i);
		if ( res != 0 )
		{
			return E_DBW_GETDATE;
		}
		if ( yearMonthBuff != "" )
		{
			currYear = yearMonthBuff.substr(0,4);
			currMonth = yearMonthBuff.substr(4);
		}
		std::string tableName = "records_" + currYear + currMonth;
		JTS currentMonth(DB,tableName, is2ndLevelHlr);
		res = currentMonth.create();
		if ( res != 0 )
		{
			return res;
		}
		table currentMonthTable(DB,tableName);
		table oldRecordsTable(DB,"oldRecords");
		std::string whereClause = " date >= \"" + currYear + "-";
		whereClause += currMonth + "-01\"";
		int writtenLines;
		int readLines;
		res = oldRecordsTable.write(whereClause,writtenLines);
		if ( res != 0 )
		{
			return res;
		}
		res = currentMonthTable.read(whereClause,readLines);
		if ( res != 0 )
		{
			return res;
		}
		if ( writtenLines == readLines )
		{
			res = oldRecordsTable.removeRecords(whereClause);
			//expunge records from oldRecords
			if ( res != 0 )
			{
				return E_DBW_REMOVE_RECORDS;
			}
			//unlink Buffer.
			if ( oldRecordsTable.unlinkBuffer() != 0 )
			{
				return E_DBW_UNLINK;
			}
		}
		else
		{
			//records mismatch
			return E_DBW_LINESMISMATCH;
		}
	}
	hlr_log("Exiting create past Months.",&logStream,6);
	return 0;
}

int recordsTables::createCurrentMonth()
{
	std::string yearMonthBuff;
	int res = getYearMonth(DB,yearMonthBuff);
	if ( res != 0 )
	{
		return E_DBW_GETDATE;
	}
	std::string currYear,currMonth;
	if ( yearMonthBuff != "" )
	{
		currYear = yearMonthBuff.substr(0,4);
		currMonth = yearMonthBuff.substr(4);
	} 
	std::string tableName = "records_" + currYear + currMonth;
	JTS currentMonth(DB,tableName, is2ndLevelHlr);
	res = currentMonth.create();
	if ( res != 0 )
	{
		return res;
	}
	hlr_log("Exiting create current Month.",&logStream,6);
	return 0;
	/*
	table currentMonthTable(DB,tableName);
	table oldRecordsTable(DB,"oldRecords");
	std::string whereClause = " date >= \"" + currYear + "-";
	whereClause += currMonth + "-01\"";
	int writtenLines;
	int readLines;
	res = oldRecordsTable.write(whereClause,writtenLines);
	if ( res != 0 )
	{
		return res;
	}
	res = currentMonthTable.read(whereClause,readLines);
	if ( res != 0 )
	{
		return res;
	}
	if ( writtenLines == readLines )
	{
		res = oldRecordsTable.removeRecords(whereClause);
		//expunge records from oldRecords
		if ( res != 0 )
		{
			return E_DBW_REMOVE_RECORDS;
		}
		else
		{
			//unlink Buffer.
			if ( oldRecordsTable.unlinkBuffer() != 0 )
			{
				return E_DBW_UNLINK;
			}
			else
			{
				hlr_log("Exiting create current Month.",&logStream,6);
				return 0;
			}
		}
	}
	else
	{
		//records mismatch
		return E_DBW_LINESMISMATCH;
	}
	 */
}

int table::unlock()
{
	string logBuff = tableName + "UN-Locked";
	hlr_log(logBuff, &logStream, 8);
	return unlink ( tableLock.c_str());
}

int table::lock()
{
	std::fstream cfStream(tableLock.c_str(), ios::out);
	if ( !cfStream )
	{
		string logBuff = "Could not lock " + tableName;
		hlr_log(logBuff, &logStream, 8);
		return 1;
	}
	else
	{
		string logBuff = tableName + " locked";
		hlr_log(logBuff, &logStream, 8);
		cfStream.close();
		return 0;
	}
}



std::string table::getTableLock() const
{
	return tableLock;
}

void table::setTableLock(std::string tableLock)
{
	this->tableLock = tableLock;
}

string table::lockOwner()
{
	ifstream cfStream (tableLock.c_str(), ios::in);
	if ( !cfStream )
	{
		string logBuff = tableName + " is not locked";
		hlr_log(logBuff, &logStream, 8);
		return "";
	}
	else
	{
		string returnBuffer;
		string textLine;
		while ( getline (cfStream, textLine, '\n'))
		{
			returnBuffer += textLine;
		}
		string logBuff = tableName + " locked contents:" + returnBuffer;
		hlr_log(logBuff, &logStream, 8);
		cfStream.close();
		return returnBuffer;
	}
}

bool table::locked()
{
	ifstream cfStream (tableLock.c_str(), ios::in);
	if ( !cfStream )
	{
		string logBuff = tableName + " is not locked";
		hlr_log(logBuff, &logStream, 8);
		return false;
	}
	else
	{
		string logBuff = tableName + " is locked";
		hlr_log(logBuff, &logStream, 5);
		cfStream.close();
		return true;
	}
}

int JTSManager::parseTransLog(string logString, hlrLogRecords& records)
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
					map<string,string>::value_type (param,value)
			);
		}
		it++;
	}
	records.cpuTime = atoi(logMap["CPU_TIME"].c_str());
	records.wallTime = atoi(logMap["WALL_TIME"].c_str());
	records.ceId = logMap["CE_ID"];
	records.userVo = logMap["userVo"];
	records.processors = logMap["processors"];
	records.urCreation = logMap["urCreation"];
	records.localUserId = logMap["localUserId"];
	records.localGroupId = logMap["localGroup"];
	records.lrmsId = logMap["lrmsId"];
	records.jobName = logMap["jobName"];
	records.accountingProcedure = logMap["accountingProcedure"];
	if ( logMap["start"] != "" )
	{
		records.start = logMap["start"];
	}
	else
	{
		records.start = "0";
	}
	if ( logMap["end"] != "" )
	{
		records.end = logMap["end"];
	}
	else
	{
		records.end = "0";
	}
	if ( logMap["qtime"] != "" && logMap["qtime"] != "qtime" )
	{
		records.qtime = logMap["qtime"];
	}
	else
	{
		records.qtime = "0";
	}
	if ( logMap["ctime"] != "" && logMap["ctime"] != "ctime" )
	{
		records.ctime = logMap["ctime"];
	}
	else
	{
		records.ctime = "0";
	}
	records.fqan = logMap["userFqan"];
	records.siteName = logMap["SiteName"];
	records.atmEngineVersion = logMap["atmEngineVersion"];
	records.voOrigin = logMap["voOrigin"];
	records.executingNodes = logMap["execHost"];
	records.glueCEInfoTotalCPUs = logMap["glueCEInfoTotalCPUs"];
	if ( logMap["specInt2000"] != "" )//backwardCompatibility
	{
		records.iBench = logMap["specInt2000"];
		records.iBenchType = "si2k";
	}
	if ( logMap["specFloat2000"] != "" )//backwardCompatibility
	{
		records.fBench = logMap["specFloat2000"];
		records.fBenchType = "sf2k";
	}
	if ( logMap["iBench"] != "" )
	{
		records.iBench = logMap["iBench"];
	}
	if ( logMap["fBench"] != "" )
	{
		records.fBench = logMap["fBench"];
	}
	if ( logMap["iBenchType"] != "" )
	{
		records.iBenchType = logMap["iBenchType"];
	}
	if ( logMap["fBenchType"] != "" )
	{
		records.fBenchType = logMap["fBenchType"];
	}
	string buff = logMap["VMEM"];
	size_t pos = buff.find("k");
	if ( pos != string::npos )
	{
		buff = buff.substr(0,pos);
	}
	if ( buff != "" && buff != "vmem" )
	{
		records.vMem = buff;
	}
	else
	{
		records.vMem = "0";
	}
	buff = logMap["MEM"];
	pos = buff.find("k");
	if ( pos != string::npos )
	{
		buff = buff.substr(0,pos);
	}
	if ( buff != "" && buff != "mem" )
	{
		records.mem = buff;
	}
	else
	{
		records.mem = "0";
	}
	return 0;
}

int JTSManager::removeDuplicated(string whereClause)
{
	db hlrDb ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	string logBuff = "Checking for duplicate entries.";
	hlr_log(logBuff,&logStream,6);
	string queryString = "";
	queryString = "SELECT uniqueChecksum,count(dgJobId) FROM " + jtsTableName + " " + whereClause + " GROUP BY uniqueChecksum HAVING count(dgJobId) > 1";
	logBuff = "Query:" + queryString;
	hlr_log(logBuff,&logStream,9);
	dbResult getChecksum = hlrDb.query(queryString);
	if ( hlrDb.errNo != 0 )
	{
		return E_DBW_SELECT;
	}
	else
	{
		if ( getChecksum.numRows() > 0 )
		{
			int numRows = getChecksum.numRows();
			for (int i=0; i<numRows; i++)
			{
				string uniqueChecksum = getChecksum.getItem(i,0);
				string multiplicity = getChecksum.getItem(i,1);
				logBuff = "Found:"+ uniqueChecksum+ ",entries:"+ multiplicity;
				hlr_log(logBuff,&logStream,7);
				queryString = "DELETE FROM " + jtsTableName + " WHERE uniqueChecksum='";
				queryString += uniqueChecksum + "' AND accountingProcedure='outOfBand'";
				logBuff = "Removing:" + uniqueChecksum + ",outOfBand.";
				hlr_log(logBuff,&logStream,6);
				hlrDb.query(queryString);
			}
		}

	}
	//hlrGenericQuery getChecksums(queryString);
	//getChecksums.query();
	//vector<resultRow>::const_iterator it = (getChecksums.queryResult).begin();
	//while ( it != (getChecksums.queryResult).end() )
	//{
	//      logBuff = "Found:"+(*it)[0]+ ",entries:"+ (*it)[1];
	//    hlr_log(logBuff,&logStream,7);
	//string queryString = "SELECT dgJobId,accountingProcedure FROM " + jtsTableName + " WHERE uniqueChecksum='";
	//queryString += (*it)[0] + "'";
	//logBuff = "Query:" + queryString;
	//hlr_log(logBuff,&logStream,9);
	//hlrGenericQuery getInfo(queryString);
	//getInfo.query();
	//vector<resultRow>::const_iterator it2 = (getInfo.queryResult).begin();
	//while ( it2 != (getInfo.queryResult).end() )
	//{
	//       if ( debug )
	//      {
	//                cerr << (*it2)[0] << ":" << (*it2)[1] << endl;
	//        }
	//        it2++;
	//}
	// queryString = "DELETE FROM " + jtsTableName + " WHERE uniqueChecksum='";
	// queryString += (*it)[0] + "' AND accountingProcedure='outOfBand'";
	// logBuff = "Removing:" + (*it)[0] + ",outOfBand.";
	// hlr_log(logBuff,&logStream,6);
	// hlrGenericQuery delOOB(queryString);
	// delOOB.query();
	// it++;
	//}
	return 0;
}

int execTranslationRules(database& DB, string& rulesFile)
{
	db dataBase ( DB.sqlServer,
			DB.sqlUser,
			DB.sqlPassword,
			DB.sqlDbName );
	//With great power comes great responsibility.
	ifstream inFile(rulesFile.c_str());
	if ( !inFile ) return 1;
	string logBuff = "Automatic translation rules requested.";
	hlr_log(logBuff,&logStream,3);
	logBuff = "With great power comes great responsibility.";
	hlr_log(logBuff,&logStream,4);
	string rule;        int line = 0;
	while ( getline (inFile, rule, '\n') )
	{                line++;
	if ( rule[0] == '#' )
	{
		continue;
	}
	size_t pos = rule.find("rule:");
	if ( pos == string::npos )
	{
		continue;
	}
	else
	{
		string ruleBuff = rule.substr(pos+5);
		if ( ( rule.find("DROP") != string::npos ) || ( rule.find("DELETE") != string::npos) )
		{
			string logBuff = "Sir hadn't you better buckle up? Ah, buckle this! LUDICROUS SPEED! *GO!*";
			hlr_log(logBuff,&logStream,2);
		}
		dataBase.query(ruleBuff);
		int res = dataBase.errNo;
		if ( res != 0 )
		{
			cerr << "Error in query:" << ruleBuff << ":"<< int2string(res) << endl;
		}
		string logBuff = "Executing:" + ruleBuff;
		hlr_log(logBuff,&logStream,4);
	}
	}
	inFile.close();
	return 0;
}
