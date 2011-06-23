//$Id: hlrJTSFeeder.cpp,v 1.1.2.30 2011/06/23 13:10:51 aguarise Exp $
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
#include "../base/serviceVersion.h"

#define OPTION_STRING "C:Dh"
#define DGAS_DEF_CONF_FILE "/etc/dgas/dgas_hlr.conf"

using namespace std;

bool lazyAccountCheck= false;//just for build FIXME

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
const char * hlr_tmp_sql_dbname;
ofstream logStream;
string masterLock = "";
int system_log_level = 7;
bool debug = false;
volatile sig_atomic_t keep_going = 1;

int I = 0;
int J = 0;



struct cmdLineOptions {
	bool debug;
	bool needsHelp;
	string confFileName;
};


int options ( int argc, char **argv, cmdLineOptions& opt )
{
	opt.confFileName = DGAS_DEF_CONF_FILE;
	opt.debug = false;
	opt.needsHelp = false;
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
			{"Conf",1,0,'C'},
			{"debug",0,0,'D'},
			{"help",0,0,'h'},
			{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
			long_options, &option_index)) != EOF)
		switch (option_char)
		{
		case 'C': opt.confFileName = optarg; break;
		case 'D': opt.debug = true; break;
		case 'h': opt.needsHelp =true; break;
		default : break;
		}
	return 0;
}

int help (const char *progname)
{
	cerr << "\n " << progname << endl;
	cerr << " Version: "<< VERSION << endl;
	cerr << " Author: Andrea Guarise " << endl;
	cerr << " Updates a reference table used in queries to the database." << endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS] " << endl;
	cerr << setw(30) << left << "-D --debug"<<"Ask for debug output" << endl;
	cerr << setw(30) << left << "-C --Conf"<<"HLR configuration file, if not the default: " << DGAS_DEF_CONF_FILE << endl;
	cerr << setw(30) << left << "-M --masterLock"<<"Put a master lock file. Other instances (e.g. via crond) will not be executed until this instance is running." << endl;
	cerr << setw(30) << left <<"-h --help"<<"This help" << endl;
	cerr << endl;
	return 0;	
}

int masterLockRemove(string& fileName)
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


int masterLockCreate(string& fileName)
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

bool masterLockExists(string& fileName)
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

int computeIterationsNo (int confIterations, int lastProcessedTid )
{
	string queryBuffer = "SELECT count(dgJobId) FROM trans_in WHERE tid > ";
	queryBuffer += int2string(lastProcessedTid);
	hlrGenericQuery iterQuery(queryBuffer);
	int res = iterQuery.query();
	if ( res != 0 )
	{
		cerr << "Error in query:" << queryBuffer << endl;
	}
	else
	{
		int records = atoi((((iterQuery.queryResult).front())[0]).c_str());
		if ( records < 100000 )
		{
			if ( debug ) cout << "Just one iteration is sufficient." << endl;
			return 1;
		}
		else
		{
			int iBuff = records/100000;
			if ( debug )
			{
				cout << "From configuration: " << int2string(confIterations) << endl;
				cout << "From number of transactions: " << int2string(iBuff) << endl;
			}
			return ( iBuff >= confIterations ) ? iBuff : confIterations;
		}
	}
	return 0;

}

string composeQuery(int first, int last)
{
	string queryBuff = "SELECT * FROM ";
	queryBuff += "trans_in,transInLog";
	queryBuff +=" WHERE trans_in.dgJobId=transInLog.dgJobId ";
	queryBuff += " AND trans_in.tid >" +int2string(first);//do not use >=
	//since 'first' is already
	//present in DB.
	queryBuff += " AND trans_in.tid <=" +int2string(last);
	if (debug)
	{
		cout << "From >" << first << "to <=" << last << endl;
		cerr << queryBuff << endl;
	}
	return queryBuff;
}

int parseLog(string logString, hlrLogRecords& records)
{
	vector<string> buffV;
	buffV.reserve(80);
	Split (',',logString, &buffV );
	vector<string>::const_iterator it = buffV.begin();
	vector<string>::const_iterator buffVend = buffV.end();
	map<string,string> logMap;
	while ( it != buffVend )
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
	if ( logMap["execCe"] != "" )
	{
		records.ceId = logMap["execCe"];
	}
	else
	{
		records.ceId = logMap["CE_ID"];
	}
	records.userVo = logMap["userVo"];
	records.processors = logMap["processors"];
	records.urCreation = logMap["urCreation"];
	records.localUserId = logMap["localUserId"];
	records.localGroupId = logMap["localGroup"];
	if ( logMap["lrmsId"] != "" )
	{
		records.lrmsId = logMap["lrmsId"];
	}
	else if ( logMap["LRMSID"] != "")
	{
		records.lrmsId = logMap["LRMSID"];
	}
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
	if ( logMap["si2k"] != "" )//backwardCompatibility
	{
		records.iBench = logMap["si2k"];
		records.iBenchType = "si2k";
	}
	if ( logMap["specFloat2000"] != "" )//backwardCompatibility
	{
		records.fBench = logMap["specFloat2000"];
		records.fBenchType = "sf2k";
	}
	if ( logMap["sf2k"] != "" )//backwardCompatibility
	{
		records.fBench = logMap["sf2k"];
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
	if ( logMap["ceCertificateSubject"] != "" )
	{
		records.ceCertificateSubject = logMap["ceCertificateSubject"];
	}
	return 0;
}

int populateJobTransSummaryTable ( const hlrGenericQuery& q , int queryLenght )
{
	int res = 0;
	if ((q.queryResult).size() == 0 ) res = 1;
	if ( res != 0 )
	{
		cout<< setw(19) << "writing records: "<< setw(40) << "########################################";
		return 1;
	}
	int i = 0;
	int j = 0;
	int counter = 0;
	int records = (q.queryResult).size();
	I += records;
	int step = records/40;
	string currentTid;
	string dgJobId;
	string thisGridId;
	string remoteGridId;
	string acl;
	string amount;
	string date;//start date
	string endDate;
	string queryBuffer;
	queryBuffer.reserve(4096*queryLenght);
	string valuesBuffer;
	string uniqueS;
	int numNodes =1;
	string indicator = "#";
	cout << setw(19) << "writing records: ";
	vector<string> valuesV;
	vector<string> valuesTransInV;
	valuesV.reserve(queryLenght+1);
	valuesTransInV.reserve(queryLenght+1);
	int valuesCounter = 0;
	db hlrDb (hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	vector<resultRow>::const_iterator it = (q.queryResult).begin();
	vector<resultRow>::const_iterator end = (q.queryResult).end();
	while ( keep_going && ( it != end ) )
	{
		numNodes = 1;
		currentTid = (*it)[0];
		dgJobId = (*it)[6];
		remoteGridId = (*it)[3];
		string gid = (*it)[2];
		hlrLogRecords logBuff;
		parseLog((*it)[10], logBuff);
		acl = logBuff.ceCertificateSubject;
		thisGridId = logBuff.ceId;
		if ( logBuff.start != "" && logBuff.start != "0" )
		{
			// there is a start value in the LRMS record
			// use it for 'date' field.
			date = "FROM_UNIXTIME("+ logBuff.start +")";
		}
		else
		{
			if ( logBuff.qtime != "" 
					&& logBuff.qtime != "0"
							&& logBuff.qtime != "qtime" )
			{
				//there isn't a start value in the LRMS record
				//but there is a queue value. Use it as the
				//date field.
				date = "FROM_UNIXTIME("+ logBuff.qtime +")";
			}
			else
			{
				if ( logBuff.ctime != "" 
						&& logBuff.ctime != "0"
								&& logBuff.ctime != "ctime" )
				{
					//ok, let's try with ctime (submit time for LSF)
					date = "FROM_UNIXTIME("+ logBuff.ctime +")";	
				}
				else
				{
					//There are not 'start', 'queue' and 'submit' values
					//in the LRMS record. Use the transaction
					//timestamp for the 'date' field
					date = "FROM_UNIXTIME("+ (*it)[5] +")";
				}
			}
		}
		if ( logBuff.end != "" && logBuff.end != "0" )
		{
			endDate = "FROM_UNIXTIME("+ logBuff.end +")";
		}
		else
		{
			if ( logBuff.start != "" && logBuff.start != "0" )
			{
				//assume job end == job start!
				endDate = "FROM_UNIXTIME("+ logBuff.start +")";
			}
			else
			{
				if ( logBuff.qtime != "" 
						&& logBuff.qtime != "0"
								&& logBuff.qtime != "qtime" )
				{
					//assume job end == job queue time!
					endDate = "FROM_UNIXTIME("+ logBuff.qtime +")";
				}
				else
				{
					//assume job end = job transaction 
					//timestamp
					endDate = "FROM_UNIXTIME("+ (*it)[5] +")";
				}
			}
		}
		//Old record workaround (1).
		if ( (logBuff.cpuTime < 0) && (logBuff.start == "0") ) 
		{
			//this is a record from older version of LSF
			//sensors cancelled in the queue. It has
			//uncorrect values in the transaction log.
			logBuff.cpuTime = 0;
			logBuff.wallTime = 0;
			logBuff.mem ="0";
			logBuff.vMem ="0";
		}
		string cpuTime = int2string(logBuff.cpuTime);
		if ( logBuff.wallTime < 0 ) logBuff.wallTime = 0;//PBS Exit_status=-4 negative wall time workaround.
		string wallTime = int2string(logBuff.wallTime);
		amount = (*it)[4];
		if ( logBuff.accountingProcedure == "" )
		{
			logBuff.accountingProcedure = "grid";
		}

		if ( (*it)[7] == "" )//if uniqueChecksum is not defined in trans_in (old record not already updated)
		{
			//compute unique Strings
			string valuesBuffer = "";
			vector<string> ceIdBuff;
			Split (':', thisGridId, &ceIdBuff);
			if (ceIdBuff.size() > 0)
			{
				valuesBuffer  = ceIdBuff[0];//FIXME can't we find something else?
			}
			valuesBuffer += logBuff.lrmsId;
			valuesBuffer += logBuff.start;
			valuesBuffer += wallTime;
			valuesBuffer += cpuTime;
			uniqueS = "";
			makeUniqueString (valuesBuffer,uniqueS);//compute unique strings
			queryBuffer = "";
			queryBuffer += "SET uniqueChecksum='";
			queryBuffer += uniqueS;
			queryBuffer += "',accountingProcedure='";
			queryBuffer += logBuff.accountingProcedure;
			queryBuffer += "' WHERE tid=" + currentTid;
			valuesTransInV.push_back(queryBuffer);
		}
		else
		{
			uniqueS = (*it)[7];
		}
		size_t posPlus = 0;
		while (  posPlus != string::npos )
		{
			posPlus = (logBuff.executingNodes).find_first_of('+',posPlus+1);
			if (posPlus != string::npos) numNodes++;
		}
		queryBuffer = "('";
		queryBuffer += dgJobId;
		queryBuffer += "',";
		queryBuffer += date;
		queryBuffer += ",'";
		queryBuffer += thisGridId;
		queryBuffer += "','";
		queryBuffer += remoteGridId;
		queryBuffer += "','";
		queryBuffer += logBuff.fqan;
		queryBuffer += "','";
		queryBuffer += logBuff.userVo;
		queryBuffer += "',";
		queryBuffer += cpuTime;
		queryBuffer += ",";
		queryBuffer += wallTime;
		queryBuffer += ",";
		queryBuffer += logBuff.mem;
		queryBuffer += ",";
		queryBuffer += logBuff.vMem;
		queryBuffer += ",";
		queryBuffer += amount;
		queryBuffer += ",";
		queryBuffer += logBuff.start;
		queryBuffer += ",";
		queryBuffer += logBuff.end;
		queryBuffer += ",'";
		queryBuffer += logBuff.iBench;
		queryBuffer += "','";
		queryBuffer += logBuff.iBenchType;
		queryBuffer += "','";
		queryBuffer += logBuff.fBench;
		queryBuffer += "','";
		queryBuffer += logBuff.fBenchType;
		queryBuffer += "','";
		queryBuffer += acl;
		queryBuffer += "',0,'";
		queryBuffer += logBuff.lrmsId;
		queryBuffer += "','";
		queryBuffer += logBuff.localUserId;
		queryBuffer += "','";
		queryBuffer += gid;
		queryBuffer += "','";
		queryBuffer += logBuff.localGroupId;
		queryBuffer += "',";
		queryBuffer += endDate;
		queryBuffer += ",'";
		queryBuffer += logBuff.siteName;
		queryBuffer += "','";
		queryBuffer += "";//sender should be here, but we don't have one
		queryBuffer += "',";
		queryBuffer += currentTid;//trans_in::tid
		queryBuffer += ",'";
		queryBuffer += logBuff.accountingProcedure;
		queryBuffer += "','";
		queryBuffer += logBuff.voOrigin;
		queryBuffer += "','";
		queryBuffer += logBuff.glueCEInfoTotalCPUs;
		queryBuffer += "','";
		queryBuffer += logBuff.executingNodes;
		queryBuffer += "',";
		queryBuffer += int2string(numNodes);
		queryBuffer += ",'";
		queryBuffer += uniqueS;
		queryBuffer += "')";
		valuesV.push_back(queryBuffer);
		i++;
		counter++;
		valuesCounter++;
		if (counter >= step )
		{
			cout << indicator << flush;
			counter = 0;
			indicator = "#";
		}
		if ( valuesCounter == queryLenght )
		{
			queryBuffer = "INSERT IGNORE INTO jobTransSummary VALUES ";
			vector<string>::const_iterator valuesIt = valuesV.begin();
			vector<string>::const_iterator valuesIt_end = valuesV.end();
			queryBuffer += *valuesIt;
			valuesIt++;
			while ( valuesIt != valuesIt_end )
			{
				queryBuffer += ",";
				queryBuffer += *valuesIt;
				valuesIt++;
			}
			dbResult result = hlrDb.query(queryBuffer);
			if ( hlrDb.errNo != 0 )
			{
				if ( debug )
				{
					cerr << "ERROR: " <<queryBuffer << "; errNo:" << int2string(hlrDb.errNo) << endl;
				}
				indicator = "E";
			}
			j += hlrDb.getAffectedRows();
			valuesV.clear();
			vector<string>::iterator queryUTIIT = valuesTransInV.begin();
			vector<string>::iterator queryUTIITend = valuesTransInV.end();
			while ( queryUTIIT != queryUTIITend )
			{
				queryBuffer = "UPDATE trans_in " + *queryUTIIT;
				dbResult resultUTI = hlrDb.query(queryBuffer);
				if ( hlrDb.errNo == 0 )
				{
					/*
                               	 	if ( debug )
                               	 	{
                                        	cerr <<queryBuffer << endl;
                                	}*/
				}
				else
				{
					if ( debug )
					{
						cerr << "ERROR: " <<queryBuffer << "; errNo:" << int2string(hlrDb.errNo) << endl;
					}
					indicator = "E";
				}
				queryUTIIT++;
			}
			valuesTransInV.clear();
			valuesCounter=0;
		}
		it++;
	}
	//manage trailing elements in queryResult vector.
	if ( keep_going && ( valuesCounter != 0 ) )
	{
		if ( debug )
		{
			cerr << int2string(valuesCounter) << " trailing elements." << endl;
		}
		queryBuffer = "INSERT IGNORE INTO jobTransSummary VALUES ";
		vector<string>::const_iterator valuesIt = valuesV.begin();
		vector<string>::const_iterator valuesItend = valuesV.end();
		queryBuffer += *valuesIt;
		valuesIt++;
		while ( valuesIt != valuesItend )
		{
			queryBuffer += ",";
			queryBuffer += *valuesIt;
			valuesIt++;
		}
		dbResult result = hlrDb.query(queryBuffer);
		if ( hlrDb.errNo != 0 )
		{
			if ( debug )
			{
				cerr << "ERROR: " <<queryBuffer <<"; errNo:"<< int2string(hlrDb.errNo) << endl;
			}
		}
		j += hlrDb.getAffectedRows();
		//trans_in
		vector<string>::iterator queryUTIIT = valuesTransInV.begin();
		vector<string>::iterator queryUTIIT_end = valuesTransInV.end();
		while ( queryUTIIT != queryUTIIT_end )
		{
			queryBuffer = "UPDATE trans_in " + *queryUTIIT;
			dbResult resultUTI = hlrDb.query(queryBuffer);
			if ( hlrDb.errNo == 0 )
			{
				/*if ( debug )
                               	 	{
                                        	cerr <<queryBuffer << endl;
                                	}*/
			}
			else
			{
				if ( debug )
				{
					cerr << "ERROR:" <<queryBuffer << endl;
				}
			}
			queryUTIIT++;
		}

	}

	if ( debug )
	{
		cout << " Found " << i << " transactions, processed:" << j << endl;
	}
	J += j;
	return 0;
}

int getMaxTid( int &maxTid )
{
	string queryString = "SELECT max(tid) FROM trans_in";
	if ( debug )
	{
		cerr << queryString << endl;
	}
	hlrGenericQuery indexQuery(queryString);
	indexQuery.query();
	if ( indexQuery.errNo == 0 )
	{
		vector<resultRow>::const_iterator it = (indexQuery.queryResult).begin();
		while ( it != (indexQuery.queryResult).end() )
		{
			maxTid = atoi(((*it)[0]).c_str());
			it++;
		}
		if ( debug )
		{
			cerr << "got tid: " << maxTid << endl;	
		}
		return 0;
	}
	return 1;
}

int getIndexFromTable( int &lastTid )
{
	string queryString = "SELECT hlrTid FROM jobTransSummary ORDER BY hlrTid DESC LIMIT 1";
	if ( debug )
	{
		cerr << queryString << endl;
	}
	hlrGenericQuery indexQuery(queryString);
	indexQuery.query();
	if ( indexQuery.errNo == 0 )
	{
		vector<resultRow>::const_iterator it = (indexQuery.queryResult).begin();
		while ( it != (indexQuery.queryResult).end() )
		{
			lastTid = atoi(((*it)[0]).c_str());
			it++;
		}
		if ( debug )
		{
			cerr << "got tid: " << lastTid << endl;
		}	
	}
	else
	{
		if ( debug )
		{
			cout << "Warning Error in the index query!, size:"<< (indexQuery.queryResult).size() << endl;
			cout << "It is absolutely normal if this is the firt time the command runs on an HLR!"<< endl;
		}
		return 1;
	}
	return 0;
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

void doNothing ( int sig )
{
	cerr << endl << "The command is being killed, Please be patient..." << endl;
	keep_going = 0;
	signal (sig, doNothing) ;
}

int main (int argc, char **argv)
{
	cmdLineOptions Options;
	options ( argc, argv, Options );
	debug = Options.debug;
	map <string,string> confMap;
	if ( dgas_conf_read ( Options.confFileName, &confMap ) != 0 )
	{
		cerr << "Couldn't open configuration file: " <<
				Options.confFileName << endl;
	}
	if (Options.needsHelp)
	{
		help(argv[0]);
		exit(0);
	}
	signal (SIGTERM, doNothing);
	signal (SIGINT, doNothing);
	hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
	hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
	hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
	hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
	hlr_tmp_sql_dbname = (confMap["hlr_tmp_sql_dbname"]).c_str();
	string hlr_logFileName = confMap["hlr_def_log"];
	int stepNumber = 5;
	int queryLenght =100;
	string stepNumberStr = (confMap["translateStepNumber"]);
	if ( confMap["jtsCommandLock"] != "" )
	{
		masterLock = confMap["jtsCommandLock"];
	}
	else
	{
		masterLock = "/usr/var/dgas/hlrTranslateDb.lock";
	}
	if ( confMap["systemLogLevel"] != "" )
	{
		system_log_level = atoi((confMap["systemLogLevel"]).c_str());
	}
	if ( masterLockExists( masterLock ) )
	{
		cout << "Another instance of the command put a lock. Exiting." << endl;
		exit(0);
	}
	int res = bootstrapLog(hlr_logFileName, &logStream);
	if ( res != 0 )
	{
		cerr << "Error bootstrapping the Log file:" << endl;
		cerr << hlr_logFileName<< endl;
		exit(1);
	}
	cout << "Locking other instances out." << endl;
	masterLockCreate( masterLock );
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
	bool is2ndLevelHlr = false;
	if ( confMap["is2ndLevelHlr"] == "true" )
	{
		is2ndLevelHlr =true;
	}
	serviceVersion thisServiceVersion(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	if ( !thisServiceVersion.tableExists() )
	{
		thisServiceVersion.tableCreate();
	}
	thisServiceVersion.setService("dgas-hlr-populateJobTransSummary");
	thisServiceVersion.setVersion(VERSION);
	thisServiceVersion.setHost("localhost");
	thisServiceVersion.setConfFile(Options.confFileName);
	thisServiceVersion.setLockFile(masterLock);
	thisServiceVersion.setLogFile(hlr_logFileName);
	thisServiceVersion.write();
	thisServiceVersion.updateStartup();
	string jobTransSummaryFields = "dgJobId;date;gridResource;gridUser;userFqan;userVo;cpuTime;wallTime;pmem;vmem;amount;start;end;iBench;iBenchType;fBench;fBenchType;acl;id;lrmsId;localUserId;hlrGroup;localGroup;endDate;siteName;urSourceServer;hlrTid;accountingProcedure;voOrigin;GlueCEInfoTotalCPUs;executingNodes;numNodes;uniqueChecksum";
	if ( !isTableUpToDate(hlr_sql_dbname, "jobTransSummary", jobTransSummaryFields ) )
	{
		cerr << "The jobTransSummary table needs a schema update. Run dgas-hlr-translatedb command first." << endl;
		masterLockRemove ( masterLock );
		exit(1);
	}
	if ( is2ndLevelHlr )
	{
		cout << "2ndLevelHlr is set to \"true\" in the conf file." << endl;
		masterLockRemove ( masterLock );
		exit(0);
		//if this is a 2nd level HLR we can bail out here...
	}
	//otherwise we must go on...
	time_t totalTime0 = time(NULL);
	int percentage = 0;
	int oldPercentage = 0;
	time_t time0;
	time_t time1;
	time_t eta = 0;
	I = 0;
	J = 0;
	int resLastTid = 0;
	int maxResTid;
	getIndexFromTable ( resLastTid );	
	getMaxTid( maxResTid );
	if ( maxResTid == resLastTid )
	{
		//nothing to do.
		if ( debug ) cout << "Nothing new to process." << endl;		
	}
	else
	{
		maxResTid++;
		stepNumber = computeIterationsNo ( stepNumber, resLastTid );
		percentage = 0;
		if ( debug )
		{
			cout << "Dividing operation in " << int2string(stepNumber) << " steps." << endl;
		}
		int step = 1;
		if ( stepNumber != 0 )
		{
			step = (maxResTid-resLastTid)/stepNumber;
		}
		if (step <= 0 ) step = 1;
		int res = 0;
		for (int i=0; keep_going && ( i<stepNumber ); i++)
		{
			time0 = time(NULL);
			percentage = ((i+1)*100)/stepNumber;
			string queryString = composeQuery(resLastTid,resLastTid+step);
			hlrGenericQuery selectQuery(queryString);
			res = selectQuery.query();
			if ( res != 0 )
			{
				cout << "Warning: problem in query.";
				if ( debug )
				{
					cout << int2string(res) << ":" << queryString << endl;
				}
			}
			populateJobTransSummaryTable ( selectQuery , queryLenght);
			resLastTid = resLastTid+step;
			if ( i == stepNumber-1 ) percentage = 100;
			time1 = time(NULL);
			if ( (percentage-oldPercentage) !=0 )	
			{
				eta = (time1-time0)*(100-percentage)/((percentage-oldPercentage)*60);
			}
			cout << " [" << setw(3) << int2string(percentage) << "%] E:"<< int2string(time1-time0) << " sec ETA:"<< int2string(eta) << " min"<<endl;
			oldPercentage = percentage;
		}
	}
	time_t totalTime1 = time(NULL);
	time_t elapsed = totalTime1-totalTime0;
	float recSec = 0.0;
	if ( elapsed != 0 ) recSec = (float)J/float(elapsed);
	cout << "Found " << I << " raw records, inserted in jobTransSummary:" << J << endl;
	cout << "Elapsed time:" << int2string(elapsed) << " Rec/sec:" << int2string(recSec) << endl;
	/*merge tables exec end*/
	cout << "Done." << endl;
	masterLockRemove ( masterLock );
	exit(0);
}
