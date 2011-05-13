/*
 * serviceVersion.h
 *
 *  Created on: May 12, 2011
 *      Author: guarise
 */

#ifndef SERVICEVERSION_H_
#define SERVICEVERSION_H_
#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/libdgas_log.h"

extern ofstream logStream;

class serviceVersion {
public:
	serviceVersion();
	serviceVersion(db* hlrDb);
	serviceVersion( string hlr_sql_server,
			string hlr_sql_user,
			string hlr_sql_password,
			string hlr_sql_dbname);
	virtual ~serviceVersion();
    string getConfFile() const;
    string getHost() const;
    string getLastShutdown() const;
    string getLastStartup() const;
    string getLockFile() const;
    string getLogFile() const;
    string getService() const;
    string getVersion() const;

    void setConfFile(string confFile);
    void setHost(string host);
    void setLastShutdown(string lastShutdown);
    void setLastStartup(string lastStartup);
    void setLockFile(string lockFile);
    void setLogFile(string logFile);
    void setService(string service);
    void setVersion(string version);
    bool tableExists();
    int tableCreate();
    int write();
    int updateStartup();
    int updateShutdown();
    int read(string& s);
    int entries(vector<string>& v);
private:
string service;
string version;
string host;
string confFile;
string logFile;
string lockFile;
string lastStartup;
string lastShutdown;
db* _hlrDb;
};



#endif /* SERVICEVERSION_H_ */
