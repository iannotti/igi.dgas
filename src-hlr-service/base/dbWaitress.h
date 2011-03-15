#ifndef dbWaitress_h
#define dbWaitress_h

#include <string>
#include <vector>
#include <fstream>
#include <unistd.h>
#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/dgas_config.h"

#define HLRDB_LOCK_FILE  "/tmp/dgas-hlrdb.lock"
#define HLRDB_BUFF_FILE  "/tmp/dgas-hlrdb.buff"
#define DBW_MT_DEF_MONTHS 12
#define DBW_MT_MTFILE	"/etc/dgas/dgas_hlr_mt.buff"

#define E_DBW_OPENFILE   71
#define E_DBW_BADDEF     72
#define E_DBW_DBERR      73
#define E_DBW_DROPMTABLE 74
#define E_DBW_SELECT 	 75
#define E_DBW_WRITE 	 75
#define E_DBW_FILEMISMATCH 76
#define E_DBW_DBLOCKED 77
#define E_DBW_LINESMISMATCH 78
#define E_DBW_REMOVE_RECORDS 79
#define E_DBW_GETDATE 80
#define E_DBW_UNLINK 81
#define E_DBW_CREATE 82
#define E_DBW_DELETE 83
#define E_DBW_GETUDEF 84
#define E_DBW_RESTORE 85
#define E_DBW_READ 86

extern ofstream logStream;

class database {
	public:
	database(){};
	database(std::string _sqlServer,
			std::string _sqlUser,
			std::string _sqlPassword,
			std::string _sqlDbName,
			std::string _lockFile = HLRDB_LOCK_FILE
	)
	{
		sqlServer = _sqlServer;
		sqlUser = _sqlUser;
		sqlPassword = _sqlPassword;
		sqlDbName = _sqlDbName;
		lockFile = _lockFile;
		hlr_log("database()", &logStream, 7);
	};

	int lock();
	int unlock();
	bool locked();

	std::string lockFile;
	std::string sqlServer;
	std::string sqlUser;
	std::string sqlPassword;
	std::string sqlDbName;
};

class table {
	public:
	table(database& _DB, std::string _tableName)
	{
		tableName = _tableName;	
		DB = _DB;
		hlr_log("table()", &logStream, 7);
	};
	std::string tableName;
	database DB;

	int rename(std::string newLabel);
	bool exists();
	int write(std::string& whereClause,int& writtenLines ,std::string fileBuff = HLRDB_BUFF_FILE);
	int read(std::string& writeWhereClause,int& readLines ,std::string fileBuff = HLRDB_BUFF_FILE);
	int removeRecords(std::string& whereClause);
	int unlinkBuffer(std::string fileBuff = HLRDB_BUFF_FILE);
	int drop();
};

class recordsTables {
	public:
	recordsTables ( database& _DB,
		int _monthsNumber = 3 )
	{
		DB = _DB;
		monthsNumber = _monthsNumber; 
		hlr_log("recordsTables()", &logStream, 7);
	};

	database DB;
	int monthsNumber;

	int createCurrentMonth();
	int createPastMonths();
	int dropAll();
	int addIndex( std::string field, std::string indexName);
};

int getYearMonth(database& DB,std::string& yearMonth,int i = 0);

class mergeTables {
	public:
	mergeTables( database& _DB,
		std::string _defFile, 
		std::string _mergeTablesFile = dgasLocation() + DBW_MT_MTFILE,
		int _months = DBW_MT_DEF_MONTHS)
	{
		DB = _DB;
		defFile = _defFile;
		mergeTablesFile = _mergeTablesFile;
		months = _months;
		reset = false;
		hlr_log("mergeTables()", &logStream, 7);
	};

	database DB;	
	bool reset;	
	std::vector<std::string> definitions;
	int exec();//entry point	
	//drop all MyISAM tables: oldRecords,records_*
	int dropAll()
	{
		recordsTables t(DB,months);
		return t.dropAll();
	};
	//drops merge tables listed in mergeTablesFile
	int drop();
	int getDef();
	int addIndex( std::string field, std::string indexName )
	{	
		recordsTables t(DB,months);
		return t.addIndex(field,indexName);
	}
	
	private:
	std::string defFile;
	std::string mergeTablesFile;
	int months; 
	//create the MERGE tables from the definitions in *defFile*
	//previously created *MERGE* tables found in *megeTablesFile* are
	//expunged.
	int create();
	//defFile -> definitions
	int readDefFile();
	//(def) entry in definitions -> unionDef
	int produceSqlUnionDefinition(std::string& def, std::string& unionDef);
	//unionDef -> JTS with unionDef;
	int createMergeTable(std::string& mergeTableName, std::string& unionDef);
	//drops a merge table, assuring it is really a MRG_MyISAM table.	
	int dropMergeTable(std::string& mergeTableName);
	//if reset and jobTransSummary is merge, restore original MyISAM
	//jobransSummary definition and contents.
	int restoreMyISAMJTS();
};

class JTS {
	public:
	JTS(database& _DB, std::string _jtsTableName, std::string _engine = "TYPE=MyISAM")
	{
		tableDef = "CREATE TABLE " +_jtsTableName;
		tableDef += " (dgJobId varchar(160), ";
		tableDef += "date datetime, ";
		tableDef += "gridResource varchar(160), ";
		tableDef += "gridUser varchar(160), ";
		tableDef += "userFqan varchar(255), ";
		tableDef += "userVo varchar(160), ";
		tableDef += "remoteHlr varchar(160), ";
		tableDef += "cpuTime int(10) unsigned default 0, ";
		tableDef += "wallTime int(10) unsigned default 0, ";
		tableDef += "pmem int(10) unsigned default 0, ";
		tableDef += "vmem int(10) unsigned default 0, ";
		tableDef += "amount smallint(5) unsigned default 0, ";
		tableDef += "start int(10) unsigned default 0, ";
		tableDef += "end int(10) unsigned default 0, ";
		tableDef += "iBench mediumint(8) unsigned, ";//! 3.3.0
		tableDef += "iBenchType varchar(16), ";  //!3.3.0
		tableDef += "fBench mediumint(8) unsigned, "; //!/3.3.0
		tableDef += "fBenchType varchar(16), "; //!3.3.0
		tableDef += "acl varchar(160), ";
		tableDef += "id bigint(20) unsigned auto_increment, ";//!  
		tableDef += "lrmsId varchar(160), ";//! 3.1.3 
		tableDef += "localUserId varchar(32), ";//! 3.1.3 
		tableDef += "hlrGroup varchar(128), ";//! 3.1.3 
		tableDef += "localGroup varchar(160), ";//! 3.1.3 
		tableDef += "endDate datetime, ";//! 3.1.3 
		tableDef += "siteName varchar(160), ";//! 3.1.3 
		tableDef += "urSourceServer varchar(255), ";//! 3.1.3 
		tableDef += "hlrTid bigint(20) unsigned, ";//! 3.1.10
		tableDef += "accountingProcedure varchar(32), ";//! 3.1.10
		tableDef += "voOrigin varchar(16), ";//! 3.1.10
		tableDef += "GlueCEInfoTotalCPUs smallint(5) unsigned, ";//! 3.1.10
		tableDef += "uniqueChecksum char(32), ";//! 3.3.0
		tableDef += "primary key (dgJobId), key(date), key(userVo), key (id), key(lrmsId), key(siteName), key(urSourceServer), key(hlrTid), key(uniqueChecksum)) ";
		tableDef += _engine;
		engine = _engine;
		DB = _DB;
		hlr_log("JTS()", &logStream, 7);
	};
	
	database DB;

	//CREATE *jtsTableName* SQL table with JTS schema and *engine*
	//it can be also used to create a JTS based merge using 
	//*engine* something like ENGINE=MERGE UNION=(t1,t2) INSERT_METHOD=LAST
	int create();

	private:
	//gets the current create definition from the jobTransSummary table. 
	//int getCurrentDef();
	std::string tableDef;
	std::string engine;
};

#endif
