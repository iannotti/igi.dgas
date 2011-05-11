//$Id: hlrTranslateDb.cpp,v 1.1.2.1.4.9 2011/05/11 15:06:22 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
/***************************************************************************
 * Code borrowed from:
 *  authors   : 
 *             
 *  
 ***************************************************************************/


#include <iostream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <csignal>
#include "glite/dgas/hlr-service/base/hlrGenericQuery.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "serviceCommonUtl.h"
#include "dbWaitress.h"

#define OPTION_STRING "C:DmvrhcM"
#define DGAS_DEF_CONF_FILE "/etc/dgas/dgas_hlr.conf"

using namespace std;

bool lazyAccountCheck= false;//just for build FIXME

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
const char * hlr_tmp_sql_dbname;
ofstream logStream;
string confFileName = DGAS_DEF_CONF_FILE;
string hlr_logFileName;
string userCertBuffer = "";
string resourceIDBuffer = "";
string mergeTablesDefinitions = "";
string mergeTablesFile = "";
string queryTypeBuffer = "";
string masterLock = "";
string cfFileName = "";
int mergeTablesPastMonths = 3;
int system_log_level = 7;
int queryLenght =100;
bool debug = false;
bool reset = false;
bool mergeReset = false;
bool needs_help = false;
bool checkVo = false;
bool checkDuplicate = false;
bool putMasterLock = false;
bool putCFLock = false;
bool is2ndLevelHlr = false;

int I = 0;
int J = 0;

struct hlrLogRecords {
	int wallTime;
	int cpuTime;
	string mem;
	string vMem;
	int cePriceTime;
	string userVo;
	string processors;
	string urCreation;
	string lrmsId;
	string localUserId;
	string jobName;
	string start;
	string end;
	string ctime;
	string qtime;
	string etime;
	string fqan;
	string iBench;
	string iBenchType;
	string fBench;
	string fBenchType;
	string ceId;
	string atmEngineVersion;
	string accountingProcedure;
	string localGroupId;
	string siteName;//in th elog seacrh for SiteName
	string hlrTid;//trans_{in,out} original tid.
	string voOrigin;//trans_{in,out} original tid.
	string glueCEInfoTotalCPUs; //number of CPUs available in the cluster.
	string executingNodes; //hostname of the executing nodes.
	string ceCertificateSubject;
};

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"Conf",1,0,'C'},
		{"debug",0,0,'D'},
		{"reset",0,0,'r'},
		{"mergeReset",0,0,'m'},
		{"checkvo",0,0,'v'},
		{"checkDuplicate",0,0,'c'},
		{"masterLock",0,0,'M'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'C': confFileName = optarg; break;
			case 'D': debug = true; break;
			case 'r': reset =true;; break;		  
			case 'm': mergeReset =true;; break;		  
			case 'v': checkVo =true;; break;
			case 'c': checkDuplicate =true;; break;
			case 'M': putMasterLock =true;; break;
			case 'h': needs_help =true;; break;		  
			default : break;
		}
	return 0;
}

int help (const char *progname)
{
	cerr << "\n " << progname << endl;
        cerr << " Version: "<< VERSION << endl;
        cerr << " Author: Andrea Guarise " << endl;
        cerr << " 26/03/2008 " << endl <<endl;
	cerr << " Updates a reference table used in queries to the database." << endl;
        cerr << " usage: " << endl;
        cerr << " " << progname << " [OPTIONS] " << endl;
        cerr << setw(30) << left << "-D --debug"<<"Ask for debug output" << endl;
	cerr << setw(30) << left << "-C --Conf"<<"HLR configuration file, if not the default: " << DGAS_DEF_CONF_FILE << endl;
        cerr << setw(30) << left << "-r --reset"<<"Clean up the query tables and recreates them from raw database info." << endl;
	cerr << setw(30) << left << "-v --checkvo"<<"Try to fix most common source of problems determining user vo." << endl;
	cerr << setw(30) << left << "-c --checkDuplicate"<<"Search for duplicate entries and expunge the one with less information." << endl;
	cerr << setw(30) << left << "-M --masterLock"<<"Put a master lock file. Other instances (e.g. via crond) will not be executed until this instance is running." << endl;
        cerr << setw(30) << left <<"-h --help"<<"This help" << endl;
        cerr << endl;
	return 0;	
}

int CFremove(string& fileName)
{
	int res = unlink ( fileName.c_str());
	if ( res != 0 )
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

int CFcreate(string& fileName)
{
	if (putMasterLock) return 0;//masterLock has precedence.
	fstream cfStream(fileName.c_str(), ios::out);
	if ( !cfStream )
	{
		return 1;
	}
	else
	{
		cfStream.close();
		return 0;
	}
}

int MLcreate(string& fileName)
{
	fstream cfStream(fileName.c_str(), ios::out);
	if ( !cfStream )
	{
		return 1;
	}
	else
	{
		cfStream.close();
		return 0;
	}
}

bool CFexists(string& fileName)
{
	ifstream cfStream (fileName.c_str(), ios::in);
	if ( !cfStream )
	{
		return false;
	}
	else
	{
		return true;
	}
}

//check the existence of the table.
bool checkTable(string tableName)
{
	string queryString = "SHOW TABLES LIKE '" + tableName + "'";
	hlrGenericQuery checkTableQuery(queryString);
	checkTableQuery.query();
	if ((checkTableQuery.queryResult).size() == 0 ) 
	{
		if ( debug )
		{
			cerr << queryString << ":false" << endl;
		}
		return false;
	}
	else
	{
		if ( debug)
		{
			cerr << queryString << ":" << endl;
		}
		return true;
	}
}

int addIndex(string dbName, string table, string index, bool primary = false)
{
	string queryString;
	if ( primary )
	{
		queryString = "CREATE UNIQUE INDEX " + index +" on " + table + " ("+index+ ")";
	}
	else
	{
		queryString = "CREATE INDEX " + index +" on " + table + " ("+index+ ")";
	}
	if ( debug )
        {
                cerr << queryString << endl;
        }
	hlrGenericQuery createIndex(dbName, queryString);
	int res = createIndex.query();
	if ( debug )
        {
		cerr << "Exited with:" << res << endl;
        }
	return res;
}

int dropIndex(string dbName, string table, string index)
{
	string queryString;
	if ( index == "PRIMARY" )
	{
		queryString = "ALTER TABLE " + table + " DROP PRIMRY KEY";
	}
	else
	{
		queryString = "ALTER TABLE " + table + " DROP INDEX "+index;
	}
	hlrGenericQuery dropIndex(queryString);
	int res = dropIndex.query();
	if ( debug )
        {
                cerr << queryString << endl;
		cerr << "Exited with:" << res << endl;
        }
	return res;
}

bool checkIndex(string dbName, string table, string index)
{
	string queryString = "DESCRIBE " + dbName + "." + table;
	hlrGenericQuery describe(queryString);
	describe.query();
	if ( debug )
	{
                cerr << queryString << endl;
	}
	if ( describe.errNo == 0 )
        {
                vector<resultRow>::const_iterator it = (describe.queryResult).begin();
                while ( it != (describe.queryResult).end() )
                {
			if ( (*it)[0] == index )
			{
                        	if ( (*it)[3] != "" ) return true;
			}
                        it++;
                }
	}
	return false;
}

bool createJobTransSummaryTable()
{
	string queryString = "";
	queryString = "CREATE TABLE jobTransSummary";
	queryString += " (dgJobId varchar(160), ";
	queryString += "date datetime, ";
	queryString += "gridResource varchar(160), ";  
	queryString += "gridUser varchar(160), ";  
	queryString += "userFqan varchar(255), ";  
	queryString += "userVo varchar(160), ";  
	//queryString += "remoteHlr varchar(160), ";  
	queryString += "cpuTime int(10) unsigned default 0, ";  
	queryString += "wallTime int(10) unsigned default 0, ";  
	queryString += "pmem int(10) unsigned default 0, ";  
	queryString += "vmem int(10) unsigned default 0, ";  
	queryString += "amount smallint(5) unsigned default 0, ";  
	queryString += "start int(10) unsigned default 0, ";  
	queryString += "end int(10) unsigned default 0, ";  
	queryString += "iBench mediumint(8) unsigned, ";//! 3.3.0  
	queryString += "iBenchType varchar(16), ";  //!3.3.0
	queryString += "fBench mediumint(8) unsigned, "; //!/3.3.0 
	queryString += "fBenchType varchar(16), "; //!3.3.0
	queryString += "acl varchar(160), ";  
	queryString += "id bigint(20) unsigned auto_increment, ";//!  
	queryString += "lrmsId varchar(160), ";//! 3.1.3 
	queryString += "localUserId varchar(32), ";//! 3.1.3 
	queryString += "hlrGroup varchar(128), ";//! 3.1.3 
	queryString += "localGroup varchar(160), ";//! 3.1.3 
	queryString += "endDate datetime, ";//! 3.1.3 
	queryString += "siteName varchar(160), ";//! 3.1.3 
	queryString += "urSourceServer varchar(255), ";//! 3.1.3 
	queryString += "hlrTid bigint(20) unsigned, ";//! 3.1.10
	queryString += "accountingProcedure varchar(32), ";//! 3.1.10
	queryString += "voOrigin varchar(16), ";//! 3.1.10
	queryString += "GlueCEInfoTotalCPUs smallint(5) unsigned, ";//! 3.1.10
	queryString += "executingNodes varchar(255), ";//! 3.4.0
	queryString += "uniqueChecksum char(32), ";//! 3.3.0
	queryString += "primary key (dgJobId), key (id))"; 
	if ( debug )
	{
		cerr << queryString << endl;
	} 
	hlrGenericQuery makeTable(queryString);
        makeTable.query();
	addIndex(hlr_sql_dbname, "jobTransSummary", "date");
	addIndex(hlr_sql_dbname, "jobTransSummary", "endDate");
	addIndex(hlr_sql_dbname, "jobTransSummary", "urSourceServer");
	addIndex(hlr_sql_dbname, "jobTransSummary", "uniqueChecksum");
	if ( !is2ndLevelHlr) addIndex(hlr_sql_dbname, "jobTransSummary", "lrmsId");
	if ( !is2ndLevelHlr) addIndex(hlr_sql_dbname, "jobTransSummary", "hlrTid");
	return checkTable("jobTransSummary");
}

int addField(string table, string fieldDesc)
{
	string queryString = "ALTER TABLE " + table +" ADD " + fieldDesc;
	hlrGenericQuery createIndex(queryString);
	int res = createIndex.query();
	if ( debug )
        {
                cerr << queryString << endl;
		cerr << "Exited with:" << res << endl;
        }
	return res;
}

int flushTables()
{
	string queryString = "FLUSH TABLES";
	hlrGenericQuery flush(queryString);
	int res = flush.query();
	if ( debug )
        {
                cerr << queryString << endl;
		cerr << "Exited with:" << res << endl;
        }
	return res;
}

int disableKeys(string dbName, string table)
{
	string queryString = "ALTER TABLE " + table + " DISABLE KEYS";
	hlrGenericQuery disable(dbName, queryString);
	int res = disable.query();
	if ( debug )
        {
                cerr << queryString << endl;
		cerr << "Exited with:" << res << endl;
        }
	return res;
}

int enableKeys(string dbName, string table)
{
	string queryString = "ALTER TABLE " + table + " ENABLE KEYS";
	hlrGenericQuery disable(dbName, queryString);
	int res = disable.query();
	if ( debug )
        {
                cerr << queryString << endl;
		cerr << "Exited with:" << res << endl;
        }
	return res;
}

bool createUrConcentratorTable()
{
	string queryString = "";
	queryString = "CREATE TABLE urConcentratorIndex";
	queryString += " (urSourceServer varchar(255), ";
	 queryString += "urSourceServerDN varchar(255), ";
	 queryString += "remoteRecordId varchar(31), ";
	 queryString += "recordDate datetime, ";
	 queryString += "recordInsertDate datetime, ";
	 queryString += "primary key (urSourceServer))";
	if ( debug )
	{
		cerr << queryString << endl;
	}
	hlrGenericQuery makeTable(queryString);
	makeTable.query();
	return checkTable("urConcentratorIndex");
}


bool createRolesTable()
{
	string queryString = "";
	queryString = "CREATE TABLE roles";
        queryString += " (id int(11), ";
        queryString += " dn varchar(255), ";
        queryString += " role varchar(160), ";
        queryString += " permission varchar(16), ";
        queryString += " queryType varchar(160), ";
        queryString += " queryAdd varchar(255), ";
        queryString += "primary key (dn,role))";
        if ( debug )
        {
                cerr << queryString << endl;
        }
        hlrGenericQuery makeTable(queryString);
        makeTable.query();
        return checkTable("roles");
}

bool createVomsAuthMapTable()
{
	string queryString = "";
        queryString = "CREATE TABLE vomsAuthMap";
        queryString += " (vo_id varchar(255), ";
        queryString += " voRole varchar(255), ";
        queryString += " hlrRole varchar(255), ";
        queryString += "primary key (vo_id,voRole))";
        if ( debug )
        {
                cerr << queryString << endl;
        }
        hlrGenericQuery makeTable(queryString);
        makeTable.query();
        return checkTable("vomsAuthMap");
}

bool isTableUpToDate (string dbName, string tableName, string &fieldList)
{
	string queryString = "DESCRIBE " + tableName;
	if ( debug )
	{
		cerr << "On DB:" << dbName << ":" << queryString << endl;
	}	
	hlrGenericQuery q(dbName, queryString);
	q.query(); 
	if ( q.errNo == 0 )
	{
		string checkBuffer;
		vector<resultRow>::iterator it = (q.queryResult).begin();
		while ( it != (q.queryResult).end() )
		{
			checkBuffer += (*it)[0];
			it++;	
		}
		size_t x = fieldList.find(";");
		while ( x < string::npos )
		{
			fieldList.erase(x,1);
			x = fieldList.find(";");
		}
		if ( debug )
		{
			cerr << "Checking: " << checkBuffer << endl;
			cerr << "against : " << fieldList << endl;
		}
		if ( checkBuffer == fieldList ) 
		{
			if ( debug ) cerr << " result: true" << endl;
			return true;
		}
	}
	if ( debug ) cerr << " result: false" << endl;
	return false;
}

int dropTable (string tableName )
{
	string queryString = "DROP TABLE " + tableName;
	if ( debug )
        {
                cerr << queryString << endl;
        }
	hlrGenericQuery q(queryString);
	int res = q.query();
        if ( res != 0 )
	{
		cerr << "Error dropping table " << tableName << ":" << int2string(res) << endl;
		return res;
	}
	return 0;
}

int cleanUpOld(string startDate)
{
	string checkString = "SELECT UNIX_TIMESTAMP(\"" + startDate +"\")";
	if ( debug )
	{
		cerr << checkString << endl;
	}
	hlrGenericQuery check(checkString);
	check.query();
	if ( check.errNo != 0)
	{
		cerr << "Error in query checkin for date validity." << endl;
		return 1;
	}
	if ( (check.queryResult).front()[0] == "0" )
	{
		cerr << "startDate is not correctly setup in the conf file. Bailing out from cleanup." << endl;
		return 1;
	}
	string queryString = "DELETE FROM jobTransSummary WHERE date < '";
	queryString += startDate + "'";
	if ( debug )	
	{
		cerr << queryString << endl;
	}	
	hlrGenericQuery q(queryString);
	q.query();
	if ( q.errNo != 0 )
	{
		cerr << "Error cleaning up jobTransSummary for jobs older than:" 			<< startDate << endl;
		return 1; 
	}
	return 0;
}

void doNothing ( int sig )
{
	cerr << "The command can't be killed, Please be patient..." << endl;
	signal (sig, doNothing) ;
}

int upgradeJTSSchema ()
{
	int res = 0;

	dropIndex(hlr_sql_dbname, "jobTransSummary", "date" );
	dropIndex(hlr_sql_dbname, "jobTransSummary", "endDate" );
	dropIndex(hlr_sql_dbname, "jobTransSummary", "lrmsId" );
	dropIndex(hlr_sql_dbname, "jobTransSummary", "urSourceServer" );
	dropIndex(hlr_sql_dbname, "jobTransSummary", "hlrTid" );
	dropIndex(hlr_sql_dbname, "jobTransSummary", "uniqueChecksum" );
	enableKeys ( hlr_sql_dbname, "jobTransSummary");
	string upgradeQuery = "ALTER TABLE jobTransSummary ";
	upgradeQuery += "CHANGE thisGridId gridResource varchar(160), ";
	upgradeQuery += "CHANGE remoteGridId gridUser varchar(160), ";
	upgradeQuery += "CHANGE cpuTime cpuTime int(10) unsigned, ";
	upgradeQuery += "CHANGE wallTime wallTime int(10) unsigned, ";
	upgradeQuery += "CHANGE pmem pmem int(10) unsigned, ";
	upgradeQuery += "CHANGE vmem vmem int(10) unsigned, ";
	upgradeQuery += "CHANGE amount amount smallint(5) unsigned, ";
	upgradeQuery += "CHANGE start start int(10) unsigned, ";
	upgradeQuery += "CHANGE end end int(10) unsigned, ";
	upgradeQuery += "CHANGE si2k iBench mediumint(8) unsigned, ";
	upgradeQuery += "CHANGE sf2k fBench mediumint(8) unsigned, ";
	upgradeQuery += "CHANGE id id bigint(20) unsigned auto_increment, ";
	upgradeQuery += "CHANGE localUserId localUserId varchar(32), ";
	upgradeQuery += "CHANGE hlrTid hlrTid bigint(20) unsigned, ";
	upgradeQuery += "CHANGE accountingProcedure accountingProcedure varchar(32), ";
	upgradeQuery += "CHANGE voOrigin voOrigin varchar(16), ";
	upgradeQuery += "CHANGE GlueCEInfoTotalCPUs GlueCEInfoTotalCPUs smallint(5) unsigned, ";
	upgradeQuery += "DROP transType, ";
	upgradeQuery += "DROP remoteHlr, ";
	upgradeQuery += "ADD executingNodes varchar(255) DEFAULT '' AFTER GlueCEInfoTotalCPUs, ";
	upgradeQuery += "ADD uniqueChecksum char(32), ";
	upgradeQuery += "ADD iBenchType varchar(16) DEFAULT 'si2k' AFTER iBench, ";
	upgradeQuery += "ADD fBenchType varchar(16) DEFAULT 'sf2k' AFTER fBench, ";
	upgradeQuery += "ADD INDEX date (date), ";
	upgradeQuery += "ADD INDEX endDate (endDate), ";
	upgradeQuery += "ADD INDEX lrmsId (lrmsId), ";
	upgradeQuery += "ADD INDEX urSourceServer (urSourceServer), ";
	upgradeQuery += "ADD INDEX hlrTid (hlrTid), ";
	upgradeQuery += "ADD INDEX uniqueChecksum (uniqueChecksum)";
	if ( debug )
	{
		cerr << upgradeQuery << endl;
	}
	hlrGenericQuery upgrade1(upgradeQuery);
	upgrade1.query();
	if ( upgrade1.errNo != 0)
	{
		cerr << "Error in query upgrading jobTransSummary (CHANGE,DROP,ADD step 1)." << endl;
		cerr << upgradeQuery << ":" << int2string(upgrade1.errNo) << endl;
		res = 1;
	}
	int percentage = 0;
	int oldPercentage = 0;
	time_t time0;
	time_t time1;
	time_t eta = 0;
	I =0;
	J = 0;
	int resLastTid = 0;
	int stepNumber = 0;
	int records = 0;
	string queryBuffer = "SELECT min(id),max(id) FROM jobTransSummary";
	hlrGenericQuery iterQuery(queryBuffer);
	res = iterQuery.query();
	if ( res != 0 )
	{
		cerr << "Error in query:" << queryBuffer << endl;
	}
	else
	{
			resLastTid = atoi((((iterQuery.queryResult).front())[0]).c_str());
			records = atoi((((iterQuery.queryResult).front())[1]).c_str()) - resLastTid;
			if ( records <= 80000 )
			{
				if ( debug ) cout << "Just one iteration is sufficient." << endl; 
				stepNumber = 1;
			}
			else
			{
				int iBuff = records/80000;
				if ( debug )
				{
					cout << "Number of records: " << int2string(records) << endl;
					cout << "First Id: " << int2string(resLastTid) << endl;
					cout << "From configuration: " << int2string(stepNumber) << endl;
					cout << "From number of transactions: " << int2string(iBuff) << endl;
				}
				stepNumber = ( iBuff >= stepNumber ) ? iBuff : stepNumber;
			}
	}
	percentage = 0;
	if ( debug )
	{
		cout << "Dividing operation in " << int2string(stepNumber) << " steps." << endl;
	}
	int step = (records/stepNumber) +1;
	// if (step <= 0 ) step = 1;
	for (int i=0; i<stepNumber; i++)
	{
		time0 = time(NULL);
		percentage = ((i+1)*100)/stepNumber;
		string queryString = "SELECT id,gridResource,lrmsId,start,wallTime,cpuTime from jobTransSummary WHERE id >=" + int2string(resLastTid) + " AND id <" + int2string(resLastTid+step);
		hlrGenericQuery stepQuery(queryString);
		if ( debug )
				cerr << queryString << endl; 
   		int res = stepQuery.query();
		if ( res != 0 )
		{
			cout << "Warning: problem in query.";
			if ( debug )
			{
				cout << queryString << endl; 
			}
		}
		int x = 0;
		int y = 0;
		int counter = 0;
	       	int records = (stepQuery.queryResult).size();
		I += records;
       		int barStep = records/40;
		vector<string> valuesJTSV;
		int valuesCounter = 0;
		string indicator = "#";
		db hlrDb (hlr_sql_server,
				hlr_sql_user,
				hlr_sql_password,
				hlr_sql_dbname);
		vector<resultRow>::const_iterator it = (stepQuery.queryResult).begin();
		while ( it != (stepQuery.queryResult).end() )
		{		//compute unique Strings
			string id = (*it)[0];	
			string gridResource = (*it)[1];	
			string lrmsId = (*it)[2];	
			string start = (*it)[3];	
			string wallTime = (*it)[4];	
			string cpuTime = (*it)[5];	
			string valuesBuffer = "";
			vector<string> ceIdBuff;
			Split (':', gridResource, &ceIdBuff);        
			if (ceIdBuff.size() > 0)
			{
				valuesBuffer  = ceIdBuff[0];//FIXME can't we find something else?
			}
			valuesBuffer += lrmsId;
			valuesBuffer += start;
			valuesBuffer += wallTime;
			valuesBuffer += cpuTime;
			string uniqueS = "";
			makeUniqueString (valuesBuffer,uniqueS);
			string queryBuffer = "";
			queryBuffer += "UPDATE jobTransSummary SET uniqueChecksum='";
			queryBuffer += uniqueS;
			queryBuffer += "' WHERE id=" + id;
			dbResult resultUTI = hlrDb.query(queryBuffer);
			if ( hlrDb.errNo != 0 )
               	       	{
				indicator = "!";
  	                    	if ( debug )
                               	{
                       	       	 	cerr << "ERROR:" <<queryBuffer << endl;
	                       	}
       	                }
			else
			{
				y++;
			}
			// compute unique strings
			x++;
			counter++;
			valuesCounter++;
       		        if (counter >= barStep )
	       	        {
       		             cout << indicator << flush;
  	                     counter = 0;
			     indicator = "#";
  		        }
			it++;
		}
		if ( debug )
		{
			cout << "Found " << x << " transactions, processed:" << y << endl;
		}
		J += y;
		resLastTid = resLastTid+step;
		if ( x == stepNumber-1 ) percentage = 100;
		time1 = time(NULL);
		if ( (percentage-oldPercentage) !=0 )	
		{
			eta = (time1-time0)*(100-percentage)/((percentage-oldPercentage)*60);
		}
		cout << " [" << setw(3) << int2string(percentage) << "%] E:"<< int2string(time1-time0) << " ETA:"<< int2string(eta) << endl;
		oldPercentage = percentage;
	}
	if ( debug )
	{
		cout << "Total: found " << I << " transactions, processed:" << J << endl;
	}
	/*
	disableKeys ( hlr_sql_dbname, "jobTransSummary");
	addIndex(hlr_sql_dbname, "jobTransSummary", "date");
	addIndex(hlr_sql_dbname, "jobTransSummary", "endDate");
	addIndex(hlr_sql_dbname, "jobTransSummary", "lrmsId");
	addIndex(hlr_sql_dbname, "jobTransSummary", "urSourceServer");
	addIndex(hlr_sql_dbname, "jobTransSummary", "hlrTid");
	addIndex(hlr_sql_dbname, "jobTransSummary", "uniqueChecksum");
	*/
	enableKeys ( hlr_sql_dbname, "jobTransSummary");
	return res;
}

int upgradeTINSchema ()
{
	int res = 0;
	disableKeys(hlr_sql_dbname, "trans_in");
	string upgradeQuery = "ALTER TABLE trans_in ";
	upgradeQuery += "CHANGE tid tid bigint(20) unsigned NOT NULL auto_increment, ";
	upgradeQuery += "CHANGE amount amount smallint(5) unsigned, ";
	upgradeQuery += "CHANGE tr_stamp tr_stamp int(10) unsigned, ";
	upgradeQuery += "CHANGE dg_jobid dgJobId varchar(160), ";
	upgradeQuery += "DROP from_url, ";
	upgradeQuery += "ADD uniqueChecksum char(32), ";
	upgradeQuery += "ADD accountingProcedure varchar(32)";
	if ( debug )
	{
		cerr << upgradeQuery << endl;
	}
	hlrGenericQuery upgrade1(upgradeQuery);
	upgrade1.query();
	if ( upgrade1.errNo != 0)
	{
		cerr << "Error in query upgrading trans_in (CHANGE,DROP,ADD step 1)." << endl;
		cerr << upgradeQuery << ":" << int2string(upgrade1.errNo) << endl;
		res = 1;
	}
	addIndex(hlr_sql_dbname, "trans_in", "uniqueChecksum");
	enableKeys(hlr_sql_dbname, "trans_in");
	return res;
}

int upgradeTQSchema ()
{
	int res = 0;
	disableKeys(hlr_tmp_sql_dbname, "trans_queue");
	string upgradeQuery = "ALTER TABLE trans_queue ";
	upgradeQuery += "CHANGE from_cert_subject gridUser varchar(160), ";
	upgradeQuery += "CHANGE to_cert_subject gridResource varchar(160), ";
	upgradeQuery += "CHANGE to_hlr_url urSource varchar(160), ";
	upgradeQuery += "CHANGE amount amount smallint(5) unsigned, ";
	upgradeQuery += "CHANGE timestamp timestamp int(10) unsigned, ";
	upgradeQuery += "CHANGE priority priority smallint(5) unsigned, ";
	upgradeQuery += "CHANGE status_time status_time int(10) unsigned, ";
	upgradeQuery += "ADD uniqueChecksum char(32), ";
	upgradeQuery += "ADD accountingProcedure varchar(32), ";
	upgradeQuery += "DROP from_hlr_url, DROP type";
	if ( debug )
	{
		cerr << upgradeQuery << endl;
	}
	hlrGenericQuery upgrade1(hlr_tmp_sql_dbname, upgradeQuery);
	upgrade1.query();
	if ( upgrade1.errNo != 0)
	{
		cerr << "Error in query upgrading trans_queue (CHANGE step 1)." << endl;
		cerr << upgradeQuery << ":" << int2string(upgrade1.errNo) << endl;
		res = 1;
	}
	addIndex(hlr_tmp_sql_dbname, "trans_queue", "uniqueChecksum", true);
	dropIndex(hlr_tmp_sql_dbname, "trans_queue", "PRIMARY" );
	enableKeys(hlr_tmp_sql_dbname,"trans_queue");
	return res;
}

int upgradeURCI ()
{
	int res = 0;
	string upgradeQuery = "ALTER TABLE urConcentratorIndex ";
	upgradeQuery += "ADD uniqueChecksum char(32)";
	if ( debug )
	{
		cerr << upgradeQuery << endl;
	}
	hlrGenericQuery upgrade1(hlr_sql_dbname, upgradeQuery);
	upgrade1.query();
	if ( upgrade1.errNo != 0)
	{
		cerr << "Error in query upgrading urConcentratorIndex (CHANGE step 1)." << endl;
		cerr << upgradeQuery << ":" << int2string(upgrade1.errNo) << endl;
		res = 1;
	}
	return res;
}

int upgradeRGV ()
{
	int res = 0;
	string query = "SELECT rid,gid from resource_group_vo";
	if ( debug )
	{
		cerr << query << endl;
	}
	hlrGenericQuery select1(hlr_sql_dbname, query);
	select1.query();
	if ( select1.errNo == 0 )
	{
		if ( (select1.queryResult).size() != 0 )
		{
			
			vector<resultRow>::iterator it = (select1.queryResult).begin();
			while ( it != (select1.queryResult).end() )
			{
				query = "UPDATE acctdesc SET gid='";
				query += (*it)[1] + "' WHERE id ='";
				query += (*it)[0] + "'";
				if ( debug )
				{
					cout << query << endl;
				}
				hlrGenericQuery replace1(hlr_sql_dbname, query);
				replace1.query();
				if ( replace1.errNo != 0 )
				{
					cerr << "Warning error in:" << query << endl;
				}
				it++;
			}
			dropTable ("resource_group_vo");
		}
		else
		{
			//No results???
			cerr << "Warning: no results in RGV." << endl;
		} 	
	}
	else
	{
		cerr << "Error in query upgrading RGV (SELECT step 1)." << endl;
		cerr << query << ":" << int2string(select1.errNo) << endl;
		res = 1;
	}
	return res;
}

int upgradeADSchema ()
{
	int res = 0;
	disableKeys(hlr_sql_dbname, "acctdesc");
	string upgradeQuery = "ALTER TABLE acctdesc ";
	upgradeQuery += "CHANGE cert_subject ceId varchar(160)";
	if ( debug )
	{
		cerr << upgradeQuery << endl;
	}
	hlrGenericQuery upgrade1(hlr_sql_dbname, upgradeQuery);
	upgrade1.query();
	if ( upgrade1.errNo != 0)
	{
		cerr << "Error in query upgrading acctdesc (CHANGE step 1)." << endl;
		cerr << upgradeQuery << ":" << int2string(upgrade1.errNo) << endl;
		res = 1;
	}
	upgradeQuery = "ALTER TABLE acctdesc";
	upgradeQuery += " DROP a_type";
	if ( debug )
	{
		cerr << upgradeQuery << endl;
	}
	hlrGenericQuery upgrade2(hlr_sql_dbname, upgradeQuery);
	upgrade2.query(); 
	if ( upgrade2.errNo != 0)
	{
		cerr << "Error in query upgrading acctdesc (DROP step 1)." << endl;
		cerr << upgradeQuery << ":" << int2string(upgrade2.errNo) << endl;
		res = 2;
	}
	upgradeQuery = "ALTER TABLE acctdesc ";
	upgradeQuery += " ADD gid varchar(128)";
	if ( debug )
	{
		cerr << upgradeQuery << endl;
	}
	hlrGenericQuery upgrade3(hlr_sql_dbname, upgradeQuery);
	upgrade3.query();
	if ( upgrade3.errNo != 0)
	{
		cerr << "Error in query upgrading acctdesc (ADD step 1)." << endl;
		cerr << upgradeQuery << ":" << int2string(upgrade2.errNo) << endl;
		res = 3;
	}
	enableKeys(hlr_sql_dbname,"acctdesc");
	return res;
}

int createVoStorageRecordsTable ()
{
	if ( checkTable("voStorageRecords") )
	{
		if ( debug )
		{
			cout << " table voStorageRecords already exists." << endl; 
		}
		return 0;
	}
	string queryString = "";
        queryString = "CREATE TABLE voStorageRecords";
        queryString += " (";
	queryString += " id bigint(20) unsigned auto_increment, ";
        queryString += " uniqueChecksum char(32), ";
        queryString += " timestamp int(10) unsigned, ";
	queryString += " siteName varchar(255), ";
        queryString += " vo varchar(255), ";
        queryString += " voDefSubClass varchar(255), ";
        queryString += " storage varchar(255), ";
        queryString += " storageSubClass varchar(255), ";
        queryString += " urSourceServer varchar(255), ";
        queryString += " usedBytes bigint(20) unsigned, ";
        queryString += " freeBytes bigint(20) unsigned, ";
        queryString += "primary key (uniqueChecksum), key(id), key (vo), key(storage))";
        if ( debug )
        {
                cerr << queryString << endl;
        }
        hlrGenericQuery makeTable(queryString);
        makeTable.query();
        return checkTable("voStorageRecords");
}

int createDGASTable ()
{
	if ( checkTable("DGAS") )
	{
		if ( debug )
		{
			cout << " table DGAS already exists." << endl;
		}
		return 0;
	}
	string queryString = "";
        queryString = "CREATE TABLE DGAS";
        queryString += " (";
        queryString += " service char(32), ";
        queryString += " version varchar(255), ";
        queryString += " confFile varchar(255), ";
        queryString += " logFile varchar(255), ";
        queryString += " lockFile varchar(255), ";
        queryString += "primary key (service))";
        if ( debug )
        {
                cerr << queryString << endl;
        }
        hlrGenericQuery makeTable(queryString);
        makeTable.query();
        return checkTable("DGAS");
}

int execTranslationRules(string& rulesFile)
{
	//With great power comes great responsibility.
	ifstream inFile(rulesFile.c_str());
	if ( !inFile ) return 1;
	string logBuff = "Automatic translation rules requested.";
	hlr_log(logBuff,&logStream,3);
	logBuff = "With great power comes great responsibility.";
	hlr_log(logBuff,&logStream,4);
	string rule;
	int line = 0;
	while ( getline (inFile, rule, '\n') )
	{
		line++;
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
			hlrGenericQuery rule(ruleBuff);
			int res = rule.query();
			if ( res != 0 )
			{
				cerr << "Error in query:" << ruleBuff << endl;
			}
			string logBuff = "Executing:" + ruleBuff;
			hlr_log(logBuff,&logStream,4);
		}
	}
	inFile.close();
	return 0;
}

int removeDuplicated ()
{
	string logBuff = "Checking for duplicate entries.";
	hlr_log(logBuff,&logStream,6);
	string queryString = "";
        queryString = "SELECT uniqueChecksum,count(dgJobId) FROM jobTransSummary GROUP BY uniqueChecksum HAVING count(dgJobId) > 1";
        if ( debug )
        {
                cerr << queryString << endl;
        }
        hlrGenericQuery getChecksums(queryString);
        getChecksums.query();
	vector<resultRow>::const_iterator it = (getChecksums.queryResult).begin();
	while ( it != (getChecksums.queryResult).end() )
	{
		logBuff = "Found:"+(*it)[0]+ ",entries:"+ (*it)[1];
		hlr_log(logBuff,&logStream,7);
		string queryString = "SELECT dgJobId,accountingProcedure from jobTransSummary WHERE uniqueChecksum='";
		queryString += (*it)[0] + "'";
		if ( debug )
		{
			cerr << queryString << endl;
		}
		hlrGenericQuery getInfo(queryString);
		getInfo.query();
		vector<resultRow>::const_iterator it2 = (getInfo.queryResult).begin();
		while ( it2 != (getInfo.queryResult).end() )
		{
			if ( debug )
			{
				cerr << (*it2)[0] << ":" << (*it2)[1] << endl;
			}
			it2++;
		}
		queryString = "DELETE from jobTransSummary WHERE uniqueChecksum='";
		queryString += (*it)[0] + "' AND accountingProcedure='outOfBand'";
		logBuff = "Removing:" + (*it)[0] + ",outOfBand.";
		hlr_log(logBuff,&logStream,6);
		hlrGenericQuery delOOB(queryString);
		delOOB.query();
		it++;
	}
	return 0;
}

void Exit (int exitStatus )
{
	if ( putMasterLock ) CFremove ( masterLock );
	if ( putCFLock ) CFremove ( cfFileName );
	exit(exitStatus);
}

int main (int argc, char **argv)
{
	options ( argc, argv );
        map <string,string> confMap;
        if ( dgas_conf_read ( confFileName, &confMap ) != 0 )
	{
		cerr << "Couldn't open configuration file: " <<
			confFileName << endl;
	}
	signal (SIGTERM, doNothing);
	signal (SIGINT, doNothing);
        hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
        hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
        hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
        hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
        hlr_tmp_sql_dbname = (confMap["hlr_tmp_sql_dbname"]).c_str();
	hlr_logFileName = confMap["hlr_def_log"];
	int stepNumber = 5;
	string stepNumberStr = (confMap["translateStepNumber"]);
	string acceptRecordsStartDate = "";
	bool autoDeleteOldRecords = false;
	string rulesFile;
	if ( ( confMap["autoDeleteOldRecords"] == "true" ) || 
		( confMap["autoDeleteOldRecords"] == "yes" )  )
	{
		autoDeleteOldRecords = true;
	}
	if ( confMap["stopTranslateFile"] != "" )
	{
		cfFileName = (confMap["stopTranslateFile"]).c_str();
	}
	else
	{
		cfFileName = dgasLocation() + "/var/dgas/stopTranslateDb";
	}
	if ( confMap["masterLock"] != "" )
	{
		masterLock = (confMap["masterLock"]).c_str();
	}
	else
	{
		masterLock = dgasLocation() + "/var/dgas/hlrTranslateDb.lock";
	}
	if ( confMap["systemLogLevel"] != "" )
	{
		system_log_level = atoi((confMap["systemLogLevel"]).c_str());
	}
	if ( CFexists( masterLock ) )
	{
		cout << "Another instance of hlr-translatedb put a lock. Exiting." << endl;
		exit(0);
	}
	if ( putMasterLock )
	{
		cout << "Locking other instances out." << endl;
		MLcreate ( masterLock );
	}
	int res = bootstrapLog(hlr_logFileName, &logStream);
	if ( res != 0 )
	{
		cerr << "Error bootstrapping the Log file:" << endl;
		cerr << hlr_logFileName<< endl;
		Exit(1);
	}
	if ( confMap["acceptRecordsStartDate"] != "" )
	{
		if ( autoDeleteOldRecords)
		{
			acceptRecordsStartDate = confMap["acceptRecordsStartDate"];
		}
	}
	if ( stepNumberStr != "" )
	{
		stepNumber = atoi(stepNumberStr.c_str());
	}
	else
	{
		stepNumber = 5;
	}
	if ( confMap["translateQueryLenght"] != "" )
	{
		queryLenght = atoi((confMap["translateQueryLenght"]).c_str());
	}
	is2ndLevelHlr = false;
	if ( confMap["is2ndLevelHlr"] == "true" )
	{
		is2ndLevelHlr =true;
	}
	if ( confMap["rulesFile"] != "" )
	{
			rulesFile = confMap["rulesFile"];
	}
	#ifdef MERGE
	bool useMergeTables = false;
	database DB(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);

	if ( confMap["useMergeTables"] == "true" )
	{
		useMergeTables = true;
	}
	if ( confMap["mergeTablesDefinitions"] != "" )
	{
		mergeTablesDefinitions = confMap["mergeTablesDefinitions"];
	}
	if ( confMap["mergeTablesFile"] != "" )
	{
		mergeTablesFile = confMap["mergeTablesFile"];
	}
	if ( confMap["mergeTablesPastMonths"] != "" )
	{
		mergeTablesPastMonths = atoi((confMap["mergeTablesPastMonths"]).c_str());
	}
	mergeTables mt(DB,
			mergeTablesDefinitions,
			mergeTablesFile,
			mergeTablesPastMonths);
	if ( mergeReset || reset ) mt.reset = true;
	#endif
	/*END merge tables definition*/
	if (needs_help)
	{
		help(argv[0]);
		Exit(0);
	}
	if ( CFexists(cfFileName ) )
	{
		cout << "Found file:" << cfFileName << " ,which is a request to not perform any operation. Probably this is set by another instance of this command. Do not remove it unless you know what you are doing." << endl;
		 Exit(0);
	}
	else
	{
		if ( reset )
		{
			CFcreate(cfFileName );
			putCFLock = true;
		}
	}
	cout << "Initializing database, this operation can take several minutes." << endl;
	if ( !checkIndex(hlr_sql_dbname, "jobTransSummary","date") ) addIndex(hlr_sql_dbname, "jobTransSummary","date");
	if ( !checkIndex(hlr_sql_dbname, "jobTransSummary","id") ) addIndex(hlr_sql_dbname, "jobTransSummary","id");
	if ( !checkIndex(hlr_sql_dbname, "jobTransSummary","endDate") ) addIndex(hlr_sql_dbname, "jobTransSummary","endDate");
	if ( !checkIndex(hlr_sql_dbname, "jobTransSummary","urSourceServer") ) addIndex(hlr_sql_dbname, "jobTransSummary","urSourceServer");
	if ( !checkIndex(hlr_sql_dbname, "jobTransSummary","uniqueChecksum") ) addIndex(hlr_sql_dbname, "jobTransSummary","uniqueChecksum");
	if ( !checkIndex(hlr_sql_dbname, "trans_in","rid") ) addIndex(hlr_sql_dbname, "trans_in","rid");
	if ( !checkIndex(hlr_sql_dbname, "trans_in","dgJobId")) addIndex(hlr_sql_dbname, "trans_in","dgJobId");
	if ( !checkIndex(hlr_sql_dbname, "trans_in","uniqueChecksum")) addIndex(hlr_sql_dbname, "trans_in","uniqueChecksum");
	if ( (!checkIndex(hlr_sql_dbname, "jobTransSummary","lrmsId")) && ( !is2ndLevelHlr ) ) addIndex(hlr_sql_dbname, "jobTransSummary","lrmsId");
	if ( (!checkIndex(hlr_sql_dbname, "jobTransSummary","hlrTid")) && ( !is2ndLevelHlr ) ) addIndex(hlr_sql_dbname, "jobTransSummary","hlrTid");
	//Remove unused 'jobAuth' table (>3.1.8)
	if ( checkTable("jobAuth") )
	{
		if ( dropTable ("jobAuth") != 0) 
		{
			if ( debug )
			{
				cerr << "Warning: Error dropping jobAuth!" << endl;
			}
		}
		
	}
	//create storage records table if it doesn't exists yet.
	createVoStorageRecordsTable();
	createDGASTable();
	//try to update the table withuot recreating it 
	//(applies just if updating 
	//from a database already containing previous updates otherwise
	//a reset will occur)
	string jobTransSummaryFields = "dgJobId;date;transType;thisGridId;remoteGridId;userFqan;userVo;remoteHlr;cpuTime;wallTime;pmem;vmem;amount;start;end;si2k;sf2k;acl;id;lrmsId;localUserId;hlrGroup;localGroup;endDate;siteName;urSourceServer;hlrTid;accountingProcedure;voOrigin;GlueCEInfoTotalCPUs";
	if ( !reset && isTableUpToDate(hlr_sql_dbname ,"jobTransSummary", jobTransSummaryFields ) )
	{
		if ( !reset && is2ndLevelHlr )
		{
			cout << "Updating jobTransSummary. This operation requires several minutes." << endl;
			//add the field
			CFcreate(cfFileName );//lock other comand instancies
			putCFLock = true;
			res = upgradeJTSSchema();
			CFremove(cfFileName);
			if ( res != 0 )
			{
				cerr << "Error upgrading database! Please contact dgas-support@to.infn.it" << endl;
				Exit(1);
			}
		}
		else
		{
			reset = true;
		}
		if (checkTable("jobTransSummaryIndex"))
		{
			dropTable ("jobTransSummaryIndex");
		}
	}
	string trans_inFields = "tid;rid;gid;from_dn;from_url;amount;tr_stamp;dg_jobid";
	if ( isTableUpToDate(hlr_sql_dbname, "trans_in", trans_inFields ) )
	{
		cout << "Updating trans_in. This operation requires several minutes, no progress bar." << endl;
		cout << "This operation is performed just once." << endl;
		//add the field
		CFcreate(cfFileName );//lock other comand instancies
		putCFLock = true;
		res = upgradeTINSchema();
		CFremove(cfFileName);
		if ( res != 0 )
		{
			cerr << "Error upgrading database! Please contact dgas-support@to.infn.it" << endl;
			Exit(1);
		}
	}
	string trans_queueFields = "transaction_id;from_cert_subject;to_cert_subject;from_hlr_url;to_hlr_url;amount;timestamp;log_data;priority;status_time;type";
	if ( isTableUpToDate(hlr_tmp_sql_dbname, "trans_queue", trans_queueFields ) )
	{
		cout << "Updating trans_queue schema. This operation requires several minutes, no progress bar will be showed during the upgrade." << endl;
		cout << "This operation is performed just once." << endl;
		//add the field
		CFcreate(cfFileName );//lock other comand instancies
		putCFLock = true;
		res = upgradeTQSchema();
		CFremove(cfFileName);
		if ( res != 0 )
		{
			cerr << "Error upgrading database! Please contact dgas-support@to.infn.it" << endl;
			Exit(1);
		}
	}
	string acctdescFields = "id;a_type;email;descr;cert_subject;acl";
	if ( isTableUpToDate(hlr_sql_dbname, "acctdesc", acctdescFields ) )
	{
		cout << "Updating acctdesc schema." << endl;
		cout << "This operation is performed just once." << endl;
		CFcreate(cfFileName );//lock other comand instancies
		putCFLock = true;
		res = upgradeADSchema();
		CFremove(cfFileName);
		if ( res != 0 )
		{
			cerr << "Error upgrading database! Please contact dgas-support@to.infn.it" << endl;
			Exit(1);
		}
		else
		{
			if (checkTable("resource_group_vo"))
			{
				res = upgradeRGV();
				if ( res != 0 )
				{
					cerr << "Error upgrading database! Please contact dgas-support@to.infn.it" << endl;
					Exit(1);
				}	
			}
		}
	}
	jobTransSummaryFields = "dgJobId;date;gridResource;gridUser;userFqan;userVo;cpuTime;wallTime;pmem;vmem;amount;start;end;iBench;iBenchType;fBench;fBenchType;acl;id;lrmsId;localUserId;hlrGroup;localGroup;endDate;siteName;urSourceServer;hlrTid;accountingProcedure;voOrigin;GlueCEInfoTotalCPUs;executingNodes;uniqueChecksum";
	if ( (!is2ndLevelHlr) && (!isTableUpToDate(hlr_sql_dbname, "jobTransSummary", jobTransSummaryFields )) )
	{
		CFcreate(cfFileName );
		putCFLock = true;
		reset = true;
	}
	if ( reset ) 
	{
		#ifdef MERGE
		if ( useMergeTables ) mt.drop();
		if ( useMergeTables ) mt.dropAll();
		#endif
		dropTable ("jobTransSummary");
		if (checkTable("jobTransSummaryIndex") ) 
			dropTable ("jobTransSummaryIndex");
	}
	if (!checkTable("jobTransSummary"))
	{
		flushTables();
		if ( !createJobTransSummaryTable() )
		{
			cerr << "Error creating the table jobTransSummary!" << endl;
			if ( reset ) CFremove(cfFileName);
			Exit(1);
		}
	}
	if (!checkTable("roles"))
        {
                if ( !createRolesTable() )
                {
                        cerr << "Error creating the  table roles!" << endl;
			if ( reset ) CFremove(cfFileName);
                        Exit(1);
                }
        }
	if (!checkTable("vomsAuthMap"))
	{
		if (!createVomsAuthMapTable())
		{
			cerr << "Error creating the  table vomsAuthMap!" << endl;
			if ( reset ) CFremove(cfFileName);
                        Exit(1);
		}
	}
	//drop transInInfo table. Not used anymore.
	if (checkTable("transInInfo")) dropTable("transInInfo");
	//drop vodesc table. Not used anymore.
	if (checkTable("vodesc")) dropTable("vodesc");
	//drop grdesc table. Not used anymore.
	if (checkTable("grdesc")) dropTable("grdesc");
	//dropping tables related to User HLR (remvoved >=3.2.0)
	if (checkTable("transOutInfo")) dropTable("transOutInfo");
	if (checkTable("transOutLog")) dropTable("transOutLog");
	if (checkTable("trans_out")) dropTable("trans_out");
	if (checkTable("user_group_vo")) dropTable("user_group_vo");
	if (checkTable("group_vo")) dropTable("group_vo");
	if (checkTable("groupAdmin")) dropTable("groupAdmin");
	if ( is2ndLevelHlr )
	{
		cout << "2ndLevelHlr is set to \"true\" in the conf file." << endl;
		string urConcentratorIndexFields = "urSourceServer;urSourceServerDN;remoteRecordId;recordDate;recordInsertDate";
		if ( !isTableUpToDate(hlr_sql_dbname, "urConcentratorIndex", urConcentratorIndexFields ) )
		{
			cout << "Resetting urConcentratorIndex." << endl;
			CFcreate(cfFileName );
			putCFLock = true;
			reset = true;
		}
		string urConcentratorIndexFieldsRel4 = "urSourceServer;urSourceServerDN;remoteRecordId;recordDate;recordInsertDate;uniqueChecksum";
		if ( !isTableUpToDate(hlr_sql_dbname, "urConcentratorIndex", urConcentratorIndexFieldsRel4 ) )
		{
			cout << "Adding uniqueChecksum to urConcentratorIndex" << endl;
			if ( upgradeURCI() != 0 )
			{
				cerr << "WARNING: error upgrading urConcentratorIndex schema for release 4" << endl;
			}
		}
		if ( reset ) 
		{
			dropTable ("urConcentratorIndex");
		}
		if (!checkTable("urConcentratorIndex"))
		{
			if ( !createUrConcentratorTable() )
			{
				cerr << "Error creating the  table urConcentratorIndex!" << endl;
				if ( reset ) CFremove(cfFileName);
				Exit(1);
			}
		}
		//cleanup entries older than the expected period.
		if ( acceptRecordsStartDate != "" )
		{
			if ( cleanUpOld(acceptRecordsStartDate) != 0 )
			{
				cerr << "Error cleaning up old records!" << endl;
				if ( reset ) CFremove(cfFileName);
				Exit(1);
			}
		}
		//clean up database from tables not needed on 2lhlr.
		if (checkTable("acctdesc")) dropTable("acctdesc");
		if (checkTable("resource_group_vo")) dropTable("resource_group_vo");
		if (checkTable("transInInfo")) dropTable("transInInfo");
		if (checkTable("transInLog")) dropTable("transInLog");
		if (checkTable("trans_in")) dropTable("trans_in");
		#ifdef MERGE
		if ( useMergeTables )
		{
			cout << "Updating merge tables status." << endl;
			mt.exec();
			//now add recordDate index on date field to records_*
			//tables
			mt.addIndex("date","recordDate");
		}
		#endif
		if ( reset ) CFremove(cfFileName);
		if ( checkDuplicate ) removeDuplicated();
		execTranslationRules(rulesFile);
		//cleanup Indexes that are not useful on 2L hlr
		if ( checkIndex(hlr_sql_dbname, "jobTransSummary","lrmsId") )
		{
			dropIndex(hlr_sql_dbname, "jobTransSummary", "lrmsId" );
		}
		if ( checkIndex(hlr_sql_dbname, "jobTransSummary","hlrTid") )
		{
			dropIndex(hlr_sql_dbname, "jobTransSummary", "hlrTid" );
		}
		cout << "We can safely exit here." << endl;
		Exit(0);
	//if this is a 2nd level HLR we can bail out here...
	}
	//otherwise we must go on...
	if (checkDuplicate ) removeDuplicated();
	execTranslationRules(rulesFile);
	/*merge tables exec end*/
	cout << "Done." << endl;
	Exit(0);
}
