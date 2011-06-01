#ifndef COMM_STRUCT_H
#define COMM_STRUCT_H

#include <string>
#include <vector>

using namespace std;

struct vomsAC {
        string group;
        string role;
        string cap;
};

struct connInfo{
	string hostName;
	string contactString;
	int port;
	string voname;
	vector<string> fqan;
	vector<vomsAC> vomsData;
};

struct url_type {
        string hostname;
        int port;
};

struct listenerStatus {
        string dgas_var_dir;
        int system_log_level;
        int defConnTimeOut;
        int server_port;
        int threadNumber;
        int activeThreads;
        int authErrors;
        int threadUsecDelay;
        int threadPoolUsecDelay;
        int recordsPerBulkInsert;
        bool strictAccountCheck;
        bool authUserSqlQueries;
        bool is2ndLevelHlr;
        bool checkVo;
        bool useMergeTables;
        bool deleteOnReset;
        bool useBulkInsert;
        string maxItemsPerQuery;
        string acceptRecordsStartDate;
        string acceptRecordsEndDate;
        string recordsPerConnection;
        string mergeTablesDefinitions;
        string logFileName;
        string lockFileName;
        string configFileName;
        string hlr_user;
        string qtransInsertLog;
};

#endif
