#include <iostream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include "glite/dgas/common/base/db.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/int2string.h"

#define OPTION_STRING "H:t:d:u:p:o:s:e:V:C:S:f:F:U:L:hDG"
#define DGAS_DEF_CONF_FILE "/opt/glite/etc/dgas_hlr.conf"

using namespace std;

ofstream logStream;
int system_log_level = 7;
const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_sql_dbname;
bool useVoList = false;
bool useSiteList = false;

struct conf
{
	string table;
	string database;
	string dbHost;
	string user;
	string password;
	string outputType;
	string startTime;
	string endTime;
	string userVO;
	string siteName;
	string fields;
	string outputFile;
	string confFile;
	string UniqueChecksum;
	string Limit;
	bool gridOnly;
	bool help;
	bool debug;
};

int options(int argc, char **argv, conf& c)
{
	c.debug = false;
	c.help = false;
	c.gridOnly = false;
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
	{ "Conf", 1, 0, 'C' },
	{ "debug", 0, 0, 'D' },
	{ "table", 1, 0, 't' },
	{ "dbHost", 1, 0, 'H' },
	{ "database", 1, 0, 'd' },
	{ "user", 1, 0, 'u' },
	{ "password", 1, 0, 'p' },
	{ "outputType", 1, 0, 'o' },
	{ "start", 1, 0, 's' },
	{ "end", 1, 0, 'e' },
	{ "userVO", 1, 0, 'V' },
	{ "Site", 1, 0, 'S' },
	{ "outputFile", 'F' },
	{ "UniqueChecksum", 'U' },
	{ "Limit", 'L' },
	{ "fields", 1, 0, 'f' },
	{ "Grid", 0, 0, 'G' },
	{ "help", 0, 0, 'h' },
	{ 0, 0, 0, 0 } };
	while ((option_char = getopt_long(argc, argv, OPTION_STRING, long_options,
			&option_index)) != EOF)
		switch (option_char)
		{
		case 'H':
			c.dbHost = optarg;
			break;
		case 'C':
			c.confFile = optarg;
			break;
		case 'D':
			c.debug = true;
			break;
		case 't':
			c.table = optarg;
			break;
		case 'd':
			c.database = optarg;
			break;
		case 'u':
			c.user = optarg;
			break;
		case 'p':
			c.password = optarg;
			break;
		case 'o':
			c.outputType = optarg;
			break;
		case 's':
			c.startTime = optarg;
			break;
		case 'e':
			c.endTime = optarg;
			break;
		case 'V':
			c.userVO = optarg;
			break;
		case 'S':
			c.siteName = optarg;
			break;
		case 'f':
			c.fields = optarg;
			break;
		case 'F':
			c.outputFile = optarg;
			break;
		case 'U':
			c.UniqueChecksum = optarg;
			break;
		case 'L':
			c.Limit = optarg;
			break;
		case 'G':
			c.gridOnly = true;
			break;
		case 'h':
			c.help = true;
			break;
		default:
			break;
		}
	return 0;
}

int help(const char *progname)
{
	cout << "\n " << progname << endl;
	cout << " Version: " << VERSION << endl;
	cout << " Author: Andrea Guarise " << endl;
	cout << " Output accounting records in a CSV,KEYVALUE or APELSSM format."
			<< endl;
	cout << " Usage: " << endl;
	cout << " " << progname << " [OPTIONS] " << endl;
	cout << setw(30) << left << "-D --debug" << "Ask for debug output" << endl;
	cout << setw(30) << left << "-C --Conf"
			<< "HLR configuration file, default: " << DGAS_DEF_CONF_FILE
			<< endl;
	cout << setw(30) << left << "-t --table"
			<< "table to query in the database." << endl;
	cerr << setw(30) << left << "-u --user" << "mysql user for the query."
			<< endl;
	cerr << setw(30) << left << "-p --password" << "mysql user's password."
			<< endl;
	cerr << setw(30) << left << "-o --outputType"
			<< "output type: CSV,KEYVALUE,APELSSM" << endl;
	cerr << setw(30) << left << "-s --start"
			<< "query record with endTime fields >= startTime (YYYY-MM-DD HH:MM:SS)."
			<< endl;
	cerr << setw(30) << left << "-e --end"
			<< "query record with endTime fields < endTime (YYYY-MM-DD HH:MM:SS)."
			<< endl;
	cerr << setw(30) << left << "-V --userVO"
			<< "restrict results to specified userVO." << endl;
	cerr << setw(30) << left << "-S -- Site"
			<< "restrict results to specified Site." << endl;
	cerr << setw(30) << left << "-d --database"
			<< "database containing the table to query" << endl;
	cerr << setw(30) << left << "-F --outputFile"
			<< "output file. stdout default" << endl;
	cerr << setw(30) << left << "-U --UniqueChecksum"
			<< "start from this unique checksum" << endl;
	cerr << setw(30) << left << "-L --Limit" << "Limit number of results"
			<< endl;
	cerr << setw(30) << left << "-H --dbHost" << "database host" << endl;
	cerr << setw(30) << left << "-G --Grid"
			<< "restrict query to grid only jobs." << endl;
	cerr << setw(30) << left << "-h --help" << "This help" << endl;
	cerr << endl;
	return 0;
}

string composeQuery(conf c)
{
	string queryBuff;
	string fieldBuff;
	string endTimeBuff;
	string startTimeBuff;
	string voBuff;
	string siteBuff;
	string tableBuff;
	string LimitBuff;
	string IDBuff;
	string gridOnlyBuff;
	string logOutputBuff;

	if (c.fields == "")
	{
		fieldBuff = "*";
	}
	else
	{
		fieldBuff = c.fields;
	}
	if (c.table != "")
	{
		tableBuff = c.table;
	}
	else
	{
		tableBuff = "jobTransSummary";
	}
	if (c.UniqueChecksum != "")
	{
		//retrieve id;
		db hlr(hlr_sql_server, hlr_sql_user, hlr_sql_password, hlr_sql_dbname);
		string getIdQueryString = "SELECT id from " + tableBuff
				+ " WHERE uniqueChecksum = \"" + c.UniqueChecksum + "\"";
		logOutputBuff += getIdQueryString + ";";
		dbResult r = hlr.query(getIdQueryString);
		if (hlr.errNo == 0)
		{
			IDBuff = "AND id > " + r.getItem(0, 0) + " ";
		}
		else
		{
			return "";
		}

	}
	if (c.endTime != "")
	{
		/*
		db hlr(hlr_sql_server, hlr_sql_user, hlr_sql_password, hlr_sql_dbname);
		string getIdQueryString = "SELECT max(id) FROM " + tableBuff
				+ " WHERE endDate < \"" + c.endTime
				+ "\" AND endDate >= DATE_SUB('" + c.endTime
				+ "', INTERVAL 1 DAY)";
		logOutputBuff += getIdQueryString + ";";
		dbResult r = hlr.query(getIdQueryString);
		if (hlr.errNo == 0)
		{
			string idBuff = r.getItem(0, 0);
			if (idBuff != "")
			{
				endTimeBuff = "AND id < " + idBuff + " ";
			}
		}
		else
		{
			return "";
		}
		*/
		endTimeBuff = "AND endDate < \"" + c.endTime + "\" ";

	}
	if (c.startTime != "")
	{
		/*
		db hlr(hlr_sql_server, hlr_sql_user, hlr_sql_password, hlr_sql_dbname);
		string getIdQueryString = "SELECT min(id) FROM " + tableBuff
				+ " WHERE endDate >= \"" + c.startTime
				+ "\" AND endDate < DATE_ADD('" + c.startTime
				+ "', INTERVAL 1 DAY)";
		logOutputBuff += getIdQueryString + ";";
		dbResult r = hlr.query(getIdQueryString);
		if (hlr.errNo == 0)
		{
			string idBuff = r.getItem(0, 0);
			if (idBuff != "")
			{
				startTimeBuff = "AND id >= " + idBuff + " ";
			}
		}
		else
		{
			return "";
		}
		*/
		startTimeBuff = "AND endDate >= \"" + c.startTime + "\" ";


	}
	if (c.Limit != "")
	{
		LimitBuff = "LIMIT " + c.Limit + " ";
	}
	if (c.userVO != "")
	{
		if ((c.userVO).find("list:") != string::npos)
		{
			cerr << "using  voList" << endl;
			useVoList = true;
		}
		else
		{
			voBuff = "AND userVo =\"" + c.userVO + "\"" + " ";
		}
	}
	if (c.siteName != "")
	{
		if ((c.siteName).find("list:") != string::npos)
		{
			cerr << "using siteList" << endl;
			useSiteList = true;
		}
		else
		{
			siteBuff = "AND siteName =\"" + c.siteName + "\"" + " ";
		}

	}
	/*
	 if (c.gridOnly)
	 {
	 //gridOnlyBuff = " AND ( voOrigin=\"fqan\" OR voOrigin= \"pool\" ) ";
	 }
	 */
	queryBuff = "SELECT " + fieldBuff + " FROM " + tableBuff + " WHERE 1 "
			+ IDBuff + siteBuff + voBuff + startTimeBuff + endTimeBuff
			+ gridOnlyBuff + LimitBuff;
	cout << "Query:" << logOutputBuff << ";" << queryBuff << endl;
	return queryBuff;
}

int output(conf& c, dbResult& r)
{
	string lastChecksum;
	string lastId;
	string lastRecordDate;
	int realMessageCounter;
	if (c.outputType == "CSV")
	{
		if (r.numRows() == 0)
		{
			cout << "WrittenRecords:0" << endl;
			return 1;
		}
		else
		{
			for (unsigned int i = 0; i < r.numRows(); i++)
			{
				lastChecksum = r.getItem(i, 31);
				lastId = r.getItem(i, 18);
				lastRecordDate = r.getItem(i,12);
				if (useVoList)
				{
					if ((c.userVO).find(r.getItem(i, 5)) == string::npos)
					{
						if (c.debug)
						{
							cout << "SKIP VO" << endl;
						}
						continue;
					}
				}
				if (useSiteList)
				{
					if ((c.siteName).find(r.getItem(i, 24)) == string::npos)
					{
						if (c.debug)
						{
							cout << "SKIP SITE" << endl;
						}
						continue;
					}
				}
				if (c.gridOnly)
				{
					string GridBuff = "fqan,pool";
					if (GridBuff.find(r.getItem(i, 28)) == string::npos)
					{
						if (c.debug)
						{
							cout << "SKIP NON GRID" << endl;
						}
						continue;
					}
				}
				for (unsigned int j = 0; j < r.numFields(); j++)
				{
					string buff = r.getItem(i, j);
					cout << int2string(j) << ":" << buff << endl;
				}
				cout << endl;
			}
		}
	}
	vector<string> buffV;
	string buff;
	if (c.outputType == "APELSSM")
	{
		if (c.fields != "")
		{
			cerr
					<< "Sorry you can't use -f option along with APELSSM outputType."
					<< endl;
			return 1;
		}

		buff = "APEL-individual-job-message: v0.2\n";
		if (r.numRows() == 0)
		{
			cout << "WrittenRecords:0" << endl;
			return 1;
		}
		else
		{
			realMessageCounter = 0;
			int thisMessageCounter = 0;
			for (unsigned int i = 0; i < r.numRows(); i++)
			{
				lastChecksum = r.getItem(i, 31);
				lastId = r.getItem(i, 18);

				if (useVoList)
				{
					if ((c.userVO).find(r.getItem(i, 5)) == string::npos)
					{
						if (c.debug)
						{
							cout << "SKIP VO" << endl;
						}
						continue;
					}
				}
				if (useSiteList)
				{
					if ((c.siteName).find(r.getItem(i, 24)) == string::npos)
					{
						if (c.debug)
						{
							cout << "SKIP SITE" << endl;
						}
						continue;
					}
				}
				if (c.gridOnly)
				{
					string GridBuff = "fqan,pool";
					if (GridBuff.find(r.getItem(i, 28)) == string::npos)
					{
						if (c.debug)
						{
							cout << "SKIP NON GRID" << endl;
						}
						continue;
					}
				}
				if ( r.getItem(i,7) == "0" )
				{
					cout << "SKIP startTime = 0, host:" << r.getItem(i, 2) << " LocalJobId:" << r.getItem(i, 19) << endl;
				}
				string FQANBuff = r.getItem(i, 4);
				size_t pos1 = FQANBuff.find_first_of(';');
				if (pos1 != string::npos)
				{
					FQANBuff = FQANBuff.substr(0, pos1);
				}
				buff += "Site: " + r.getItem(i, 24) + "\n";
				buff += "SubmitHost: " + r.getItem(i, 2) + "\n";
				buff += "LocalJobId: " + r.getItem(i, 19) + "\n";
				buff += "LocalUserId: " + r.getItem(i, 20) + "\n";
				buff += "GlobalUserName: " + r.getItem(i, 3) + "\n";
				buff += "FQAN: " + FQANBuff + "\n";
				buff += "WallDuration: " + r.getItem(i, 7) + "\n";
				buff += "CpuDuration: " + r.getItem(i, 6) + "\n";
				//uff += "Processors: " + r.getItem(i,31) + "\n";//FIXME to be changed in 4.0.x
				//buff += "NodeCount: " + r.getItem(i,31) + "\n";//FIXME to be changed in 4.0.x
				buff += "StartTime: " + r.getItem(i, 11) + "\n";
				buff += "EndTime: " + r.getItem(i, 12) + "\n";
				buff += "MemoryReal: " + r.getItem(i, 8) + "\n";
				buff += "MemoryVirtual: " + r.getItem(i, 9) + "\n";
				if (r.getItem(i, 14) == "si2k")
				{
					buff += "ServiceLevelType: Si2k\n";
				}
				else
				{
					buff += "ServiceLevelType: custom\n";
				}
				buff += "ServiceLevel: " + r.getItem(i, 13) + "\n";

				lastRecordDate = r.getItem(i,12);
				thisMessageCounter++;
				if ( thisMessageCounter >= 1000 )
				{
					thisMessageCounter = 0;
					buffV.push_back(buff);
					buff = "APEL-individual-job-message: v0.2\n";
				}
				if (r.numRows() > 1 && i != (r.numRows() - 1))
				{
					buff += "%%\n";
				}
				realMessageCounter++;

			}

		}
	}
	if (c.outputFile == "stdout")
	{
		cout << buff << endl;
	}
	else
	{
		//FIXME iterare sul vettore buffV...ogni stringa un file.

		if (realMessageCounter > 0)
		{
			vector<string>::iterator it = buffV.begin();
			int i = 0;
			while ( it != buffV.end() )
			{
				ofstream output;
				string fileBuff = c.outputFile + "_" + int2string(i);
				output.open((fileBuff).c_str(), ios::app);
				if (!output)
				{
					cerr << "Error: couldn't open file:" << c.outputFile << endl;
					return 2;
				}
				else
				{
					output << *it << endl;
					output.close();
				}
				i++;
				it++;
			}
		}
	}
	cout << "LastChecksum:" << lastChecksum << endl;
	cout << "LastId:" << lastId << endl;
	cout << "WrittenRecords:" << realMessageCounter << endl;
	cout << "LastRecordDate:" << lastRecordDate << endl;
}

int main(int argc, char **argv)
{
	conf c;
	c.help = false;
	c.gridOnly = false;
	int res = 0;
	options(argc, argv, c);
	if (c.help)
	{
		help(argv[0]);
	}
	map < string, string > confMap;
	if (dgas_conf_read(c.confFile, &confMap) != 0)
	{
	}
	hlr_sql_server = (confMap["hlr_sql_server"]).c_str();
	hlr_sql_user = (confMap["hlr_sql_user"]).c_str();
	hlr_sql_password = (confMap["hlr_sql_password"]).c_str();
	hlr_sql_dbname = (confMap["hlr_sql_dbname"]).c_str();
	string hlr_logFileName = (confMap["hlr_def_log"]).c_str();

	if (c.dbHost != "")
		hlr_sql_server = (c.dbHost).c_str();
	if (c.database != "")
		hlr_sql_dbname = (c.database).c_str();
	if (c.user != "")
		hlr_sql_user = (c.user).c_str();
	if (c.password != "")
		hlr_sql_password = (c.password).c_str();

	string queryString = composeQuery(c);
	if (c.debug)
	{
		cout << queryString << endl;
	}
	db hlr(hlr_sql_server, hlr_sql_user, hlr_sql_password, hlr_sql_dbname);
	dbResult r = hlr.query(queryString);
	if (hlr.errNo == 0)
	{
		output(c, r);
	}
	else
	{
		return hlr.errNo;
	}
	return 0;
}
