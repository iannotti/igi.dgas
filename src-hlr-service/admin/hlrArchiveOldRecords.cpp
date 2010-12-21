#include <iomanip>
#include <string>
#include <vector>
#include <ctime>
#include <fstream>
#include <getopt.h>
#include "glite/dgas/hlr-service/base/hlrGenericQuery.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"

#define OPTION_STRING "C:d:p:Dnhbi:"
#define DGAS_DEF_CONF_FILE "/etc/dgas_hlr.conf"

using namespace std;

bool lazyAccountCheck = false;//for buid FIXME

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;

int system_log_level = 7;
ofstream logStream;

string confFileName = dgasLocation() + DGAS_DEF_CONF_FILE;
string referenceDate;
string prefix = dgasLocation() + "/var/dgas";
string hlr_logFileName;
string inRecordsFile;

int mainCounter = 0;
int mainRemovedCounter = 0;

bool debug = false;
bool needs_help = false;
bool useInFile = false;
bool backupOnly = false;
bool is2ndLevelHlr = false;
bool archiveOld = true;

struct logRecords {
	int start;
	int end;
	int ctime;
	int qtime;
};

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"Conf",1,0,'C'},
		{"date",1,0,'d'},
		{"inFile",1,0,'i'},
		{"debug",0,0,'D'},
		{"new",0,0,'n'},
		{"backup",0,0,'b'},
		{"prefix",1,0,'p'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'C': confFileName = optarg; break;
			case 'D': debug = true; break;
			case 'n': archiveOld = false; break;
			case 'd': referenceDate = optarg; break;
			case 'i': useInFile = true; inRecordsFile = optarg; break;
			case 'p': prefix = optarg; break;
			case 'b': backupOnly =true; break;
			case 'h': needs_help =true; break;
			default : break;
		}
	return 0;
}


int help (const char *progname)
{
	cerr << "\n " << progname << endl;
	cerr << " Version: "<< VERSION << endl;
	cerr << " Author: Andrea Guarise " << endl;
	cerr << " Archive old record of the HLR database." << endl;
	cerr << " usage: " << endl;
	cerr << " " << progname << " [OPTIONS] " << endl;
	cerr << setw(30) << left << "-D --debug"<<"Ask for debug output" << endl;
	cerr << setw(30) << left << "-C --Conf"<<"HLR configuration file, if not the default: " << DGAS_DEF_CONF_FILE << endl;
	cerr << setw(30) << left << "-n --new"<<"Inverts the command logic. Records newer than \'date\' will be archived." << endl;
	cerr << setw(30) << left << "-d --date"<<"Specifies the date in the form yyyy-mm-dd, records older than this will be expunged from the DB." << endl;
	cerr << setw(30) << left << "-i --inFile"<<"Uses the file specified as the record source. useful to ripristinate a backup made with -b option" << endl;
	cerr << setw(30) << left << "-p --prefix"<<"Used to specifie the directry where backups and record archives are stored" << endl;
	cerr << setw(30) << left <<"-b --backup"<<"Just performs a backup." << endl;
	cerr << setw(30) << left <<"-h --help"<<"This help" << endl;
	cerr << endl;
	return 0;
}

inline void SplitMC (string delim, string& input, vector<string> *output)
{
	size_t pos = 0;
	size_t prev_pos = 0;
	string buffer;
	while (( pos = input.find(delim, pos + delim.size() ))
			!= string::npos )
	{
		if ( pos == 0 )
		{
			pos = pos + delim.size();
			prev_pos=pos;
			continue;
		}
		buffer = input.substr(prev_pos, pos-prev_pos);
		buffer = buffer.substr(1,buffer.size()-2);	
		output->push_back(buffer);
		pos = pos + delim.size();
		prev_pos=pos;
	}
	buffer = input.substr( prev_pos, pos - prev_pos);
	buffer = buffer.substr(1,buffer.size()-2);	
	output->push_back (buffer);
}

int dumpDatabase(string dbName, string& prefix)
{
	time_t curtime;
	curtime = time(NULL);
	string fileName = prefix + "/" + dbName + "_" + int2string(curtime) + ".sql";
	cout << "Dumping current database contents to: " << fileName << endl;
	string mysqldump = "mysqldump -u " + (string) hlr_sql_user + " -p" + (string) hlr_sql_password;
	mysqldump += " " + (string) hlr_sql_dbname + " >" + fileName;
	cout << mysqldump << endl;
	system(mysqldump.c_str());
	cout << "Done." << endl; 
	return 0;
}

int writeRecordsIntoFile(string& prefix, string& outputFile )
{
	time_t curtime;
	curtime = time(NULL);
	string tables;
	string baseFileName;
	baseFileName = "transin";
	tables = "trans_in,transInLog";
	string fileName = prefix + "/" + baseFileName + "_" + int2string(curtime) + ".records";
	cout << "Dumping records to temporary file: " << fileName << endl;
	string queryString = "SELECT * from " + tables;
	queryString += " where trans_in.dgJobId=transInLog.dgJobId ";
	queryString += " INTO OUTFILE '" + fileName + "'";
	queryString += " FIELDS TERMINATED BY \";|\" ENCLOSED BY '\"'";
	cout << "query:" << queryString << endl; 
	hlrGenericQuery q(queryString);
	int res = q.query();
	if ( res != 0 )
	{
		return res;
	}
	outputFile = fileName;
	return 0;
}

int dropTable( string tableName )
{
	cout << "Dropping table: " << tableName << endl;
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

int createTransIn()
{
	string queryString = "CREATE TABLE trans_in (";
	queryString += "tid bigint(20) unsigned NOT NULL auto_increment,";
	queryString += "rid varchar(128) default NULL,";
	queryString += "gid varchar(128) default NULL,";
	queryString += "from_dn varchar(160) default NULL,";
	queryString += "amount smallint(5) unsigned default NULL,";
	queryString += "tr_stamp int(10) unsigned NOT NULL,";
	queryString += "dgJobId varchar(160) default NULL,";
	queryString += "uniqueChecksum varchar(32) default NULL,";
	queryString += "accountingProcedure varchar(32) default NULL,";
	queryString += "PRIMARY KEY  (tid), key (rid), key (dgJobId), key(uniqueChecksum))";
	hlrGenericQuery q(queryString);
	int res = q.query();
	if ( res != 0 )
	{	
		cout << queryString << " Got error:" << int2string(res) << endl;
		return res;
	}
	return 0;
}

int createTransInLog()
{
	string queryString = "CREATE TABLE transInLog (";
	queryString += "dgJobId varchar(160) NOT NULL,";
	queryString += "log blob NOT NULL,";
	queryString += "PRIMARY KEY  (dgJobId))";
	hlrGenericQuery q(queryString);
	int res = q.query();
	if ( res != 0 )
	{
		cout << queryString << " Got error:" << int2string(res) << endl;
		return res;
	}
	return 0;
}

int parseLog (string& log, int& date )
{
	if (debug) cout << "Parsing log:" << log << endl;
	logRecords records;
	vector<string> buffV;
	Split (',',log, &buffV );
	vector<string>::const_iterator it = buffV.begin();
	string tag;
	size_t pos = 0;
	while ( it != buffV.end() )
	{
		tag = "start=";
		pos = (*it).find(tag);
		if ( pos != string::npos )
		{
			string buff = (*it).substr(tag.size(),(*it).length()-1);
			if ( buff != "" )
                        {
                                records.start = atoi(buff.c_str());
                        }
                        else
                        {
                                records.start = 0;
                        }
		}
		tag = "end=";
		pos = (*it).find(tag);
		if ( pos != string::npos )
		{
			string buff = (*it).substr(tag.size(),(*it).length()-1);
			if ( buff != "" )
                        {
                                records.end = atoi(buff.c_str());
                        }
                        else
                        {
                                records.end = 0;
                        }
		}
		tag = "qtime=";
		pos = (*it).find(tag);
		if ( pos != string::npos )
		{
			string buff = (*it).substr(tag.size(),(*it).length()-1);
			if ( buff != "" && buff != "qtime" )
                        {
                                records.qtime = atoi(buff.c_str());
                        }
                        else
                        {
                                records.qtime = 0;
                        }
		}
		tag = "ctime=";
		pos = (*it).find(tag);
		if ( pos != string::npos )
		{
			string buff = (*it).substr(tag.size(),(*it).length()-1);
			if ( buff != "" && buff != "ctime" )
                        {
                                records.ctime = atoi(buff.c_str());
                        }
                        else
                        {
                                records.ctime = 0;
                        }
		}

		it++;
	}
	if ( records.start != 0 )
	{
		date = records.start;
	}
	else
	{
		if ( records.qtime != 0 )
		{
			date = records.qtime;
		}
		else
		{
			if ( records.ctime != 0 )
			{
				date = records.ctime;
			}
			else
			{	
				if ( records.end != 0 )
				{
					date = records.end;
				}
				else
				{
					return 1;
				}
			}
		}
	}
	return 0;
}

int insertRecord( string tType, vector<string>& fields)
{
	string table1;
	string table2;
	if ( tType == "in" )
	{
		table1 = "trans_in";
		table2 = "transInLog";
	}
	if ( tType == "out" )
	{
		table1 = "trans_out";
		table2 = "transOutLog";
	}
	string queryString1 = "INSERT INTO " + table1 + " VALUES (";
	queryString1 += "0,'";
	queryString1 += fields[1] + "','";
	queryString1 += fields[2] + "','";
	queryString1 += fields[3] + "','";
	queryString1 += fields[4] + "',";
	queryString1 += fields[5] + ",'";
	queryString1 += fields[6] + "','";
	queryString1 += fields[7] + "','";
	queryString1 += fields[8] + "')";

	string queryString2 = "INSERT INTO " + table2 + " VALUES ('";
	queryString2 += fields[9] + "','";
	queryString2 += fields[10] + "')";
	
	cout << queryString1 << endl;
	hlrGenericQuery q1(queryString1);
	int res = q1.query();
	if ( res != 0 )
	{
		return res;
	}
	cout << queryString2 << endl;
	hlrGenericQuery q2(queryString2);
	res = q2.query();
	if ( res != 0 )
	{
		return res;
	}
	
	return 0;
}

int processLine( string tType, int& referenceTs ,string& line )
{
	if ( debug ) cout << "Type: " << tType << " Processing line: " << line << endl;
	vector<string> fields;
	SplitMC ( ";|" , line, &fields);
	int thisRecordDate;
	if ( parseLog ( fields.back(), thisRecordDate ) != 0 )
	{
		cerr << "can't find date in the log!" << endl;
		return 1;
	}
	if ( archiveOld )
	{
		if ( referenceTs < thisRecordDate )
		{
			if ( fields.size() != 11 )
			{
				cerr << "record with wrong number of fields encountered." << endl;
				//wron number of fields in the record!	
			}
			else
			{
				//insertRecord.
				if ( insertRecord( tType, fields) != 0 )
				{
					cerr << "Error inserting record in the database" << endl;
				}
				else
				{
					mainCounter++;
				}
			}
		}
		else
		{
			cout << "skipping record with date = " << int2string(thisRecordDate) << endl;
			mainRemovedCounter++;
		}
	}
	else
	{
		if ( referenceTs >= thisRecordDate )
		{
			if ( fields.size() != 11 )
			{
				cerr << "record with wrong number of fields encountered." << endl;
				//wron number of fields in the record!	
			}
			else
			{
				//insertRecord.
				if ( insertRecord( tType, fields) != 0 )
				{
					cerr << "Error inserting record in the database" << endl;
				}
				else
				{
					mainCounter++;
				}
			}
		}
		else
		{
			cout << "skipping record with date = " << int2string(thisRecordDate) << endl;
			mainRemovedCounter++;
		}

	}
	return 0;
}

int convertDate(string& inputDate, int& outputTimeStamp )
{
	vector<string> dateV;
	Split('-',inputDate, &dateV);
	if (dateV.size() != 3) return 1;
	time_t tsBuff = 0;
	struct tm tmBuff;
	tmBuff.tm_sec = 0; 
	tmBuff.tm_min = 0; 
	tmBuff.tm_hour = 0; 
	tmBuff.tm_gmtoff = 3600; 
	tmBuff.tm_year=atoi(dateV[0].c_str())-1900; 
	tmBuff.tm_mon=atoi(dateV[1].c_str())-1; 
	tmBuff.tm_mday=atoi(dateV[2].c_str()); 
	tsBuff = mktime(&tmBuff);
	outputTimeStamp = tsBuff;
	if ( debug ) cout << "Date: " << inputDate << ", Converted to: " << int2string(outputTimeStamp) << endl;
	if ( outputTimeStamp == -1 )
	{
		return 2;
	}
	return 0;
}

int removeFile(string& fileName)
{
	string $cmd = "rm -f " + fileName;
	return system ($cmd.c_str());
}

int main (int argc, char* argv[] )
{
	int referenceTs = 0;
	options ( argc, argv );
	map <string,string> confMap;
	if ( dgas_conf_read ( confFileName, &confMap ) != 0 )
	{
		cerr << "Couldn't open configuration file: " <<
			confFileName << endl;
	}	
	hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
	hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
	hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
	hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
	hlr_logFileName = (confMap["hlr_def_log"]).c_str();
	if ( confMap["is2ndLevelHlr"] == "true" )
	{
		is2ndLevelHlr = true;
	}	
	if (needs_help)
	{
		help(argv[0]);
		return 0;
	}
	logStream.open( hlr_logFileName.c_str() , ios::app);
	if ( backupOnly )
	{
		cout << "Start performing database backup." << endl;
	}
	else
	{
		if ( archiveOld )
		{
			cout << "Start archiving record *older* than " << referenceDate << endl;
		}
		else
		{
			cout << "Start archiving record *newer* than " << referenceDate << endl;
		}
	}
	if ( referenceDate != "" )
	{
		if ( convertDate( referenceDate, referenceTs ) != 0 )
		{
			cerr << "Erroro converting date to timestamp." << endl;
			return 1;
		}
	}
	if ( dumpDatabase(hlr_sql_dbname, prefix )  != 0 )
	{
		cerr << "Error dumping the database, it not safe to proceed." << endl;
		return 2;
	}
	if ( is2ndLevelHlr )
	{
		cout << "Aggregator (2ndLevel) HLR detected, just dumping database. " << endl;
		cout << "The dump has been stored under:" << prefix << endl;
		return 0;
	}
	if ( !useInFile )
	{
		//IN records
		if ( writeRecordsIntoFile( prefix, inRecordsFile ) != 0 )
		{
			cerr << "Error writing raw records into temporary buffer." << endl;
			return 3;
		}
	}
	if ( backupOnly )
	{
		cout << "backup completed."<< endl;	
		return 0;
	}
	//process temp raw buffer
	ifstream inRawFile(inRecordsFile.c_str());
	if ( !inRawFile )
	{
		cerr << "Error opening raw file!" << endl;
		return 6;
	}	
	if ( dropTable("trans_in") != 0 )
	{
		cerr << "Error dropping table trans_in." << endl;
		if (!useInFile) return 4;
	}
	if ( dropTable("transInLog") != 0 )
	{
		cerr << "Error dropping table transInLog." << endl;
		if (!useInFile) return 5;
	}
	if ( createTransIn() != 0 )
	{
		cerr << "Error re-creating table trans_in." << endl;
		return 7;
	}
	if ( createTransInLog() != 0 )
	{
		cerr << "Error re-creating table transInLog." << endl;
		return 8;
	}
	string line;
	while ( getline(inRawFile, line, '\n' ))
	{
		processLine("in", referenceTs, line );
	}
	cout << "Now in the db: " << int2string(mainCounter) << " records." << endl;
	cout << "Archived: " << int2string(mainRemovedCounter) << " records." << endl;
	inRawFile.close();
	if ( !useInFile )
	{
		if ( removeFile(inRecordsFile) != 0 )
		{
			cerr << "Error removing tmpfile" << endl;
			return 9;
		}
	}
	//OUT records
	return 0;
}
