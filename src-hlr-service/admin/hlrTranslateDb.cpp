//$Id: hlrTranslateDb.cpp,v 1.1.2.1.4.73 2012/06/07 14:04:42 aguarise Exp $
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

#define OPTION_STRING "C:DmrhcT"
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
string masterLock = "";
int system_log_level = 7;
bool debug = false;
bool reset = false;
bool mergeReset = false;
bool needs_help = false;
bool checkDuplicate = false;
bool translationRules = false;
bool is2ndLevelHlr = false;

int I = 0;
int J = 0;



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
			{"checkDuplicate",0,0,'c'},
			{"useTranslationRules",0,0,'T'},
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
		case 'c': checkDuplicate =true;; break;
		case 'T': translationRules = true;; break;
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
	cerr << " Updates a reference table used in queries to the database." << endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS] " << endl;
	cerr << setw(30) << left << "-D --debug"<<"Ask for debug output" << endl;
	cerr << setw(30) << left << "-C --Conf"<<"HLR configuration file, if not the default: " << DGAS_DEF_CONF_FILE << endl;
	cerr << setw(30) << left << "-r --reset"<<"Clean up the query tables and recreates them from raw database info." << endl;
	cerr << setw(30) << left << "-c --checkDuplicate"<<"Search for duplicate entries and expunge the one with less information." << endl;
	cerr << setw(30) << left << "-T --useTranslationRules"<<"Perform database translation rules defined in the rules.conf file" << endl;

	cerr << setw(30) << left << "-h --help"<<"This help" << endl;
	cerr << endl;
	return 0;	
}

int upgrade_R_3_4_0_23()
{
	int res = 0;
	bool update = false;
	string queryBuffer = "DESCRIBE jobTransSummary uniqueChecksum";
	hlrGenericQuery describe(queryBuffer);
	res = describe.query();
	if ( res != 0 )
	{
		cerr << "Error in query:" << queryBuffer << endl;
	}
	else
	{
		if ( describe.errNo == 0 )
		{
			vector<resultRow>::const_iterator it = (describe.queryResult).begin();
			while ( it != (describe.queryResult).end() )
			{
				if ( (*it)[0] == "uniqueChecksum" )
				{
					if ( (*it)[3] != "PRI" )
					{
						update = true;
					}
				}
				it++;
			}
		}
	}
	if ( update )
	{
		cout << "Table jobTransSummary needs an index update" << endl;
		cout << "Table jobTransSummary: dropping primary index..." << endl;
		string queryBuffer = "alter table jobTransSummary drop primary key";
		hlrGenericQuery dropQuery(queryBuffer);
		res = dropQuery.query();
		if ( res != 0 )
		{
			cerr << "Error in query:" << queryBuffer << endl;
			return res;
		}
		else
		{
			cout << "Table jobTransSummary: creating new primary index..." << endl;

			string queryBuffer2 = "alter table jobTransSummary ADD primary key (dgJobId,uniqueChecksum)";
			hlrGenericQuery addQuery(queryBuffer2);
			res = addQuery.query();
			if ( res != 0 )
			{
				cerr << "Error in query:" << queryBuffer2 << endl;
				return res;
			}
		}
		cout << "Table jobTransSummary: Index creation done." << endl;
	}
	return res;
}
/*
int upgrade_R_4_0_0(database& DB)
{
	int res = 0;
	table jobTransSummary(DB, "jobTransSummary");
	string jobTransSummaryFields = "dgJobId;date;gridResource;gridUser;userFqan;userVo;cpuTime;wallTime;pmem;vmem;amount;start;end;iBench;iBenchType;fBench;fBenchType;acl;id;lrmsId;localUserId;hlrGroup;localGroup;endDate;siteName;urSourceServer;hlrTid;accountingProcedure;voOrigin;GlueCEInfoTotalCPUs;executingNodes;uniqueChecksum";
	if ( jobTransSummary.checkAgainst(jobTransSummaryFields ) )//schema of 3_4_0_25
	{
		//needs upgrade
		string upgradeQuery = "ALTER TABLE jobTransSummary ";
		upgradeQuery += "ADD numNodes smallint(5) unsigned AFTER executingNodes";
		if ( debug )
		{
			cerr << upgradeQuery << endl;
		}
		hlrGenericQuery upgrade1(upgradeQuery);
		upgrade1.query();
		if ( upgrade1.errNo != 0)
		{
			cerr << "Error in query upgrading jobTransSummary (ADD step 1)." << endl;
			cerr << upgradeQuery << ":" << int2string(upgrade1.errNo) << endl;
			res = 1;
		}
	}
	return res;
}
 */

int upgrade_R_4_0_0(database& DB)
{
	int res = 0;
	int stepNumber = 1;
	int percentage = 0;
	int oldPercentage = 0;
	long int realrecords = 0;//real number of records to be processed
	long int effectiverecords;//effective number of ids (there can be more id than reecords due to fragmentation)
	long int firstTid = 0;
	long int lastTid = 0;
	table jobTransSummary(DB, "jobTransSummary");
	string jobTransSummaryFields = "dgJobId;date;gridResource;gridUser;userFqan;userVo;cpuTime;wallTime;pmem;vmem;amount;start;end;iBench;iBenchType;fBench;fBenchType;acl;id;lrmsId;localUserId;hlrGroup;localGroup;endDate;siteName;urSourceServer;hlrTid;accountingProcedure;voOrigin;GlueCEInfoTotalCPUs;executingNodes;uniqueChecksum";
	if ( jobTransSummary.checkAgainst(jobTransSummaryFields ) )//schema of 3_4_0_25
	{
		//needs upgrade

		string upgradeQuery = "CREATE TABLE JTS_tmp LIKE jobTransSummary";
		if ( debug )
		{
			cerr << upgradeQuery << endl;
		}
		hlrGenericQuery upgrade1(upgradeQuery);
		upgrade1.query();
		if ( upgrade1.errNo != 0)
		{
			cerr << "Error in query upgrading jobTransSummary (COPY step 1)." << endl;
			cerr << upgradeQuery << ":" << int2string(upgrade1.errNo) << endl;
			res = 1;
		}
		upgradeQuery = "ALTER TABLE JTS_tmp ADD numNodes smallint(5) unsigned default 1";
		if ( debug )
		{
			cerr << upgradeQuery << endl;
		}
		hlrGenericQuery upgrade2(upgradeQuery);
		upgrade2.query();
		if ( upgrade2.errNo != 0)
		{
			cerr << "Error in query upgrading jobTransSummary (ALTER step 1)." << endl;
			cerr << upgradeQuery << ":" << int2string(upgrade2.errNo) << endl;
			res = 1;
		}
		string checkQuery = "SELECT uniqueChecksum from JTS_tmp ORDER by id DESC LIMIT 1";
		if ( debug )
		{
			cerr << checkQuery << endl;
		}
		hlrGenericQuery check1(checkQuery);
		check1.query();
		if ( check1.errNo != 0 )
		{
			cerr << "Error in query upgrading jobTransSummary (CHECK step 1)." << endl;
			cerr << checkQuery << ":" << int2string(check1.errNo) << endl;
			res = 1;
		}
		else
		{
			if ( check1.Rows() == 0 )
			{
				upgradeQuery = "SELECT min(id),max(id),count(*) from jobTransSummary";
				if ( debug )
				{
					cerr << upgradeQuery << endl;
				}
			}
			else
			{
				cout << "Operation interrupted in previous run. Continuing from record:"  << ((check1.queryResult).front())[0] << endl;
				checkQuery = "SELECT id from jobTransSummary WHERE uniqueChecksum=\""+ ((check1.queryResult).front())[0] + "\"";
				if ( debug )
				{
					cerr << checkQuery << endl;
				}
				hlrGenericQuery check2(checkQuery);
				check2.query();
				if ( check2.errNo != 0)
				{
					cerr << "Error in query upgrading jobTransSummary (CHECK step 2)." << endl;
					cerr << checkQuery << ":" << int2string(check2.errNo) << endl;
					res = 1;
				}
				else

				{
					upgradeQuery = "SELECT min(id),max(id),count(*) from jobTransSummary WHERE id>" + ((check2.queryResult).front())[0];
					if ( debug )
					{
						cerr << upgradeQuery << endl;
					}
				}

			}
		}
		hlrGenericQuery upgrade3(upgradeQuery);
		upgrade3.query();
		if ( upgrade3.errNo != 0)
		{
			cerr << "Error in query upgrading jobTransSummary (MAX/MIN step 1)." << endl;
			cerr << upgradeQuery << ":" << int2string(upgrade3.errNo) << endl;
			res = 1;
		}
		else
		{
			firstTid = atoi((((upgrade3.queryResult).front())[0]).c_str());
			lastTid = atoi((((upgrade3.queryResult).front())[1]).c_str());
			realrecords = atoi((((upgrade3.queryResult).front())[2]).c_str());
			effectiverecords = atoi((((upgrade3.queryResult).front())[1]).c_str()) - atoi((((upgrade3.queryResult).front())[0]).c_str());
			if ( realrecords <= 80000 )
			{
				if ( debug ) cout << "Just one iteration is sufficient." << endl;
				stepNumber = 1;
			}
			else
			{
				long int iBuff = realrecords/80000;
				if ( debug )
				{
					cout << "Number of records: " << int2string(realrecords) << endl;
					cout << "First Id: " << firstTid << endl;
					cout << "Last Id: " << lastTid << endl;
					cout << "From configuration: " << int2string(stepNumber) << endl;
					cout << "From number of transactions: " << int2string(iBuff) << endl;
				}
				stepNumber = ( iBuff >= stepNumber ) ? iBuff : stepNumber;
			}
		}
		if ( debug )
		{
			cout << "Dividing operation in " << int2string(stepNumber) << " steps." << endl;
		}
		int step = (effectiverecords/stepNumber) +1;
		// if (step <= 0 ) step = 1;
		int x = 0;
		int barCounter = 0;
		time_t time0 = time(NULL);
		for (int i=0; i<stepNumber; i++)
		{
			percentage = ((i+1)*100)/stepNumber;
			upgradeQuery = "INSERT INTO JTS_tmp SELECT *,NULL FROM jobTransSummary WHERE id>=" + int2string(firstTid) + " AND id<" + int2string(firstTid+step);
			if ( debug )
			{
				cerr << upgradeQuery << endl;
			}
			hlrGenericQuery upgrade4(upgradeQuery);
			upgrade4.query();
			if ( upgrade4.errNo != 0)
			{
				cerr << "Error in query upgrading jobTransSummary (SELECT INTO step"<< i <<")." << endl;
				cerr << upgradeQuery << ":" << int2string(upgrade4.errNo) << endl;
				res = 1;
			}
			else
			{
				x++;
				firstTid = firstTid+step;
				if ( x == stepNumber-1 ) percentage = 100;

				time_t eta = 0;
				time_t time1 = 0;

				if ( barCounter < 50 )
				{
					cout << "#" << flush;
					barCounter++;
				}
				else
				{
					if ( (percentage-oldPercentage) !=0 )
					{
						time1 = time(NULL);
						eta = (time1-time0)*(100-percentage)/((percentage-oldPercentage)*60);
					}
					cout << " [" << setw(3) << int2string(percentage) << "%] E:"<< int2string(time1-time0) << " ETA:"<< int2string(eta) << endl;
					oldPercentage = percentage;
					barCounter = 0;
					time0 = time(NULL);
				}
			}

		}
		long int fromRecordNum = 0;
		long int toRecordNum = -1;
		upgradeQuery = "SELECT count(*) from jobTransSummary";
		if ( debug )
		{
			cerr << upgradeQuery << endl;
		}
		hlrGenericQuery recordsCheck1(upgradeQuery);
		recordsCheck1.query();
		if ( recordsCheck1.errNo == 0)
		{
			fromRecordNum = atoi((((recordsCheck1.queryResult).front())[1]).c_str());
		}
		hlrGenericQuery recordsCheck2(upgradeQuery);
		recordsCheck2.query();
		if ( recordsCheck2.errNo == 0)
		{
			toRecordNum = atoi((((recordsCheck2.queryResult).front())[1]).c_str());
		}
		if ( fromRecordNum == toRecordNum )
		{
			upgradeQuery = "DROP TABLE jobTransSummary";
			hlrGenericQuery upgrade5(upgradeQuery);
			upgrade5.query();
			if ( upgrade5.errNo != 0)
			{
				cerr << "Error in query upgrading jobTransSummary (DEL step 1)." << endl;
				cerr << upgradeQuery << ":" << int2string(upgrade5.errNo) << endl;
				res = 1;
			}
			else
			{
				upgradeQuery = "RENAME TABLE JTS_tmp TO jobTransSummary";
				hlrGenericQuery upgrade6(upgradeQuery);
				upgrade6.query();
				if ( upgrade6.errNo != 0)
				{
					cerr << "Error in query upgrading jobTransSummary (RENAME step 1)." << endl;
					cerr << upgradeQuery << ":" << int2string(upgrade6.errNo) << endl;
					res = 1;
				}
				else
				{
					upgradeQuery = "DROP TABLE JTS_tmp";
					hlrGenericQuery upgrade7(upgradeQuery);
					upgrade7.query();
					if ( upgrade7.errNo != 0)
					{
						cerr << "Error in query upgrading jobTransSummary (DEL step 1)." << endl;
						cerr << upgradeQuery << ":" << int2string(upgrade7.errNo) << endl;
						res = 1;
					}
				}
			}
		}
		else
		{
			cerr << "There was a problem in the UPGRADE phase. Please contact dgas-suuprt@to.infn.it" << endl;
			res = 2;
		}

	}
	return res;
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

bool createJobTransSummaryTable(database& DB)
{
	table jobTransSummary(DB, "jobTransSummary");
	JTS jts(DB, "jobTransSummary", is2ndLevelHlr);
	jts.create();
	return jobTransSummary.exists();
}

bool createUrConcentratorTable(database& DB)
{
	table urConcentratorIndex(DB,"urConcentratorIndex");
	string queryString = "";
	queryString = "CREATE TABLE urConcentratorIndex";
	queryString += " (urSourceServer varchar(255), ";
	queryString += "urSourceServerDN varchar(255), ";
	queryString += "remoteRecordId varchar(31), ";
	queryString += "recordDate datetime, ";
	queryString += "recordInsertDate datetime, ";
	queryString += "uniqueChecksum char(32), ";
	queryString += "primary key (urSourceServer))";
	if ( debug )
	{
		cerr << queryString << endl;
	}
	hlrGenericQuery makeTable(queryString);
	makeTable.query();
	return urConcentratorIndex.exists();
}

bool createRolesTable(database & DB)
{
	table roles(DB,"roles");
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
	return roles.exists();
}

bool createVomsAuthMapTable(database & DB)
{
	table vomsAuthMap(DB,"vomsAuthMap");
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
	return vomsAuthMap.exists();
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
		cerr << "Error in query checking for date validity." << endl;
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
		cerr << "Error cleaning up jobTransSummary for jobs older than:" << startDate << endl;
		return 1;
	}
	return 0;
}

void doNothing ( int sig )
{
	cerr << "The command can't be killed, Please be patient..." << endl;
	signal (sig, doNothing) ;
}

int upgradeJTSSchema (table& jobTransSummary)
{
	int res = 0;
	jobTransSummary.dropIndex( "date" );
	jobTransSummary.dropIndex( "endDate" );
	jobTransSummary.dropIndex( "lrmsId" );
	jobTransSummary.dropIndex( "urSourceServer" );
	jobTransSummary.dropIndex( "hlrTid" );
	jobTransSummary.dropIndex( "uniqueChecksum" );
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
		cout << "Total: found " << I << " records, processed:" << J << endl;
	}
	return res;
}

int upgradeTINSchema (database& DB)
{
	int res = 0;
	table trans_in(DB, "trans_in");
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
	trans_in.addIndex( "uniqueChecksum");
	return res;
}

int upgradeTQSchema (database& DB)
{
	int res = 0;
	table trans_queue(DB, "trans_queue");
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
	trans_queue.addIndex( "uniqueChecksum", true);
	trans_queue.dropIndex( "PRIMARY" );
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

int deleteTransInNULL()
{
	int res = 0;
	string upgradeQuery = "DELETE FROM trans_in WHERE rid IS NULL";
	if ( debug )
	{
		cerr << upgradeQuery << endl;
	}
	hlrGenericQuery upgrade1(hlr_sql_dbname, upgradeQuery);
	upgrade1.query();
	if ( upgrade1.errNo != 0)
	{
		cerr << "Error in query deleting from trans_in NULL values (DELETE step 1)." << endl;
		cerr << upgradeQuery << ":" << int2string(upgrade1.errNo) << endl;
		res = 1;
	}
	return res;
}


int RGV2ACCTDESC ()
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


int upgradeADSchema (database& DB)
{
	int res = 0;
	table acctdesc(DB, "acctdesc");
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
	return res;
}

int createVoStorageRecordsTable (database& DB)
{
	table voStorageRecords(DB, "voStorageRecords");
	if ( voStorageRecords.exists() )
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
	return voStorageRecords.exists();
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

	//Now remove lock on jobTransSummary table.
	database dBase(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	table jobTransSummary(dBase, "jobTransSummary");
	if ( jobTransSummary.locked() )
	{
		jobTransSummary.unlock();
	}
	masterLockRemove ( masterLock );
	exit(exitStatus);
}

void doOnSecondLevel(string acceptRecordsStartDate, string & rulesFile, database& DB)
{
	cout << "2ndLevelHlr is set to \"true\" in the conf file." << endl;

	table urConcentratorIndex (DB, "urConcentratorIndex");
	if(!urConcentratorIndex.exists()){
		if(!createUrConcentratorTable(DB)){
			cerr << "Error creating the  table urConcentratorIndex!" << endl;
			Exit(1);
		}
	}

	string urConcentratorIndexFieldsRel4 = "urSourceServer;urSourceServerDN;remoteRecordId;recordDate;recordInsertDate;uniqueChecksum";
	if ( !urConcentratorIndex.checkAgainst( urConcentratorIndexFieldsRel4 ) )
	{
		cout << "Adding uniqueChecksum to urConcentratorIndex" << endl;
		if(upgradeURCI() != 0){
			cerr << "WARNING: error upgrading urConcentratorIndex schema for release 4" << endl;
		}
	}


	//cleanup entries older than the expected period.
	if(acceptRecordsStartDate != ""){
		if(cleanUpOld(acceptRecordsStartDate) != 0){
			cerr << "Error cleaning up old records!" << endl;
			Exit(1);
		}
	}


	//clean up database from tables not needed on 2lhlr.
	table acctdesc (DB, "acctdesc");
	if (acctdesc.exists()) acctdesc.drop();
	table resource_group_vo (DB, "resource_group_vo");
	if (resource_group_vo.exists()) resource_group_vo.drop();
	table transInInfo (DB, "transInInfo");
	if (transInInfo.exists()) transInInfo.drop();
	table transInLog (DB, "transInLog");
	if (transInLog.exists()) transInLog.drop();
	table trans_in (DB, "trans_in");
	if (trans_in.exists()) trans_in.drop();

	//now add recordDate index on date field to records_*
	//tables

	if(checkDuplicate)
		removeDuplicated();

	if(translationRules)
		execTranslationRules(rulesFile);

	//cleanup Indexes that are not useful on 2L hlr
	table jobTransSummary (DB, "jobTransSummary");
	if ( jobTransSummary.checkIndex( "lrmsId" ) )
		jobTransSummary.dropIndex( "lrmsId" );
	if ( jobTransSummary.checkIndex( "hlrTid") )
		jobTransSummary.dropIndex( "hlrTid" );
}

int main (int argc, char **argv)
{
	options ( argc, argv );

	if (needs_help)
	{
		help(argv[0]);
		exit(0);
	}
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
		cout << "Another instance of hlr-translatedb put a lock. Exiting." << endl;
		exit(1);
	}
	cout << "Locking other instances out." << endl;
	masterLockCreate ( masterLock );
	int res = bootstrapLog(hlr_logFileName, &logStream);
	if ( res != 0 )
	{
		cerr << "Error bootstrapping the Log file:" << hlr_logFileName << endl;
		Exit(1);
	}
	database JTSdb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	database TMPdb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_tmp_sql_dbname);
	//Now put a lock on jobTransSummary table. If the table is being maintained, other commands won't use it.
	//This is a lock on the table, not on this command.
	if ( !translationRules )
	{
		database dBase(hlr_sql_server,
				hlr_sql_user,
				hlr_sql_password,
				hlr_sql_dbname);
		table jobTransSummary(dBase, "jobTransSummary");
		if ( jobTransSummary.locked() )
		{
			cout << "jobTransSummary table is locked with lock file:" << jobTransSummary.getTableLock() << endl;
			exit(0);
		}
		else
		{
			jobTransSummary.lock();
		}
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
	is2ndLevelHlr = false;
	if ( confMap["is2ndLevelHlr"] == "true" )
	{
		is2ndLevelHlr =true;
	}
	if ( confMap["rulesFile"] != "" )
	{
		rulesFile = confMap["rulesFile"];
	}

	serviceVersion thisServiceVersion(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname);
	if ( !thisServiceVersion.tableExists() )
	{
		thisServiceVersion.tableCreate();
	}
	thisServiceVersion.setService("dgas-hlr-translatedb");
	thisServiceVersion.setVersion(VERSION);
	thisServiceVersion.setHost("localhost");
	thisServiceVersion.setConfFile(confFileName);
	thisServiceVersion.setLockFile(masterLock);
	thisServiceVersion.setLogFile(hlr_logFileName);
	thisServiceVersion.write();
	thisServiceVersion.updateStartup();

	JTSManager jtsManager(JTSdb, "jobTransSummary");
	table jobTransSummary(JTSdb, "jobTransSummary");
	table trans_in(JTSdb, "trans_in");

	cout << "Initializing database, this operation can take minutes to hours to complete depending on the size of your DB. DO NOT KILL THE COMMAND PLEASE." << endl;
	if ( !jobTransSummary.checkIndex( "date") ) jobTransSummary.addIndex("date");
	if ( !jobTransSummary.checkIndex( "id") ) jobTransSummary.addIndex("id");
	if ( !jobTransSummary.checkIndex( "endDate") ) jobTransSummary.addIndex("endDate");
	if ( !jobTransSummary.checkIndex( "urSourceServer") ) jobTransSummary.addIndex("urSourceServer");
	if ( !jobTransSummary.checkIndex( "uniqueChecksum") ) jobTransSummary.addIndex("uniqueChecksum");
	if ( !jobTransSummary.checkIndex( "rid" ) ) trans_in.addIndex("rid");
	if ( !jobTransSummary.checkIndex( "dgJobId" ) ) trans_in.addIndex("dgJobId");
	if ( !jobTransSummary.checkIndex( "uniqueChecksum" ) ) trans_in.addIndex("uniqueChecksum");
	if ( (!jobTransSummary.checkIndex( "lrmsId" ) ) && ( !is2ndLevelHlr ) ) jobTransSummary.addIndex("lrmsId");
	if ( (!jobTransSummary.checkIndex( "hlrTid" ) ) && ( !is2ndLevelHlr ) ) jobTransSummary.addIndex("hlrTid");
	//create storage records table if it doesn't exists yet.
	table roles(JTSdb, "roles");
	table vomsAuthMap(JTSdb, "vomsAuthMap");
	createVoStorageRecordsTable(JTSdb);


	//try to update the table without recreating it
	//(applies just if updating
	//from a database already containing previous updates otherwise
	//a reset will occur)
	string jobTransSummaryFields = "dgJobId;date;transType;thisGridId;remoteGridId;userFqan;userVo;remoteHlr;cpuTime;wallTime;pmem;vmem;amount;start;end;si2k;sf2k;acl;id;lrmsId;localUserId;hlrGroup;localGroup;endDate;siteName;urSourceServer;hlrTid;accountingProcedure;voOrigin;GlueCEInfoTotalCPUs";
	if ( !reset && jobTransSummary.checkAgainst(jobTransSummaryFields) )
	{
		if ( !reset && is2ndLevelHlr )
		{
			cout << "Updating jobTransSummary. This operation requires several minutes." << endl;
			//add the field
			res = upgradeJTSSchema(jobTransSummary);
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

	}
	upgrade_R_4_0_0(JTSdb);
	string trans_inFields = "tid;rid;gid;from_dn;from_url;amount;tr_stamp;dg_jobid";
	if ( trans_in.checkAgainst( trans_inFields ) )
	{
		cout << "Updating trans_in. This operation requires several minutes, no progress bar." << endl;
		cout << "This operation is performed just once." << endl;
		res = upgradeTINSchema(JTSdb);
		if ( res != 0 )
		{
			cerr << "Error upgrading database! Please contact dgas-support@to.infn.it" << endl;
			Exit(1);
		}
	}
	table trans_queue(TMPdb, "trans_queue");
	string trans_queueFields = "transaction_id;from_cert_subject;to_cert_subject;from_hlr_url;to_hlr_url;amount;timestamp;log_data;priority;status_time;type";
	if ( trans_queue.checkAgainst( trans_queueFields ) )
	{
		cout << "Updating trans_queue schema. This operation requires several minutes, no progress bar will be showed during the upgrade." << endl;
		cout << "This operation is performed just once." << endl;
		res = upgradeTQSchema(TMPdb);
		if ( res != 0 )
		{
			cerr << "Error upgrading database structure! Please contact dgas-support@to.infn.it" << endl;
			Exit(1);
		}
	}
	table acctdesc (JTSdb, "acctdesc");
	string acctdescFields = "id;a_type;email;descr;cert_subject;acl";
	if ( acctdesc.checkAgainst( acctdescFields ) )
	{
		cout << "Updating acctdesc schema." << endl;
		cout << "This operation is performed just once." << endl;
		res = upgradeADSchema(JTSdb);
		if ( res != 0 )
		{
			cerr << "Error upgrading database! Please contact dgas-support@to.infn.it" << endl;
			Exit(1);
		}
		else
		{
			table resource_group_vo (JTSdb, "resource_group_vo");
			if (resource_group_vo.exists())
			{
				res = RGV2ACCTDESC();
				if ( res != 0 )
				{
					cerr << "Error upgrading database! Please contact dgas-support@to.infn.it" << endl;
					Exit(1);
				}
				else
				{
					resource_group_vo.drop();
				}
			}
		}
	}
	jobTransSummaryFields = "dgJobId;date;gridResource;gridUser;userFqan;userVo;cpuTime;wallTime;pmem;vmem;amount;start;end;iBench;iBenchType;fBench;fBenchType;acl;id;lrmsId;localUserId;hlrGroup;localGroup;endDate;siteName;urSourceServer;hlrTid;accountingProcedure;voOrigin;GlueCEInfoTotalCPUs;executingNodes;numNodes;uniqueChecksum";
	if ( (!is2ndLevelHlr) && (!jobTransSummary.checkAgainst(jobTransSummaryFields )) )
	{
		reset = true;
	}
	if ( reset )
	{
		jobTransSummary.drop();
	}
	if (!jobTransSummary.exists())
	{
		if ( !createJobTransSummaryTable(JTSdb) )
		{
			cerr << "Error creating jobTransSummary table." << endl;
			Exit(1);
		}
	}
	if ( !roles.exists() )
	{
		if ( !createRolesTable(JTSdb) )
		{
			cerr << "Error creating the roles table." << endl;
			Exit(1);
		}
	}
	if (!vomsAuthMap.exists())
	{
		if (!createVomsAuthMapTable(JTSdb))
		{
			cerr << "Error creating the vomsAuthMap table." << endl;
			Exit(1);
		}
	}
	if ( !is2ndLevelHlr ) deleteTransInNULL();
	upgrade_R_3_4_0_23();
	if ( is2ndLevelHlr )
	{
		doOnSecondLevel(acceptRecordsStartDate, rulesFile, JTSdb);
		Exit(0);
		//if this is a 2nd level HLR we can bail out here...
	}
	//otherwise we must go on...
	if ( checkDuplicate ) removeDuplicated();
	if ( translationRules ) execTranslationRules(rulesFile);
	Exit(0);
}
