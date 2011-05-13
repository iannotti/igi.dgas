/*
 * serviceVersion.cpp
 *
 *  Created on: May 12, 2011
 *      Author: guarise
 */

#include "serviceVersion.h"
#include "../../interface-common/glite/dgas/common/base/int2string.h"

serviceVersion::serviceVersion() {
	// TODO Auto-generated constructor stub

}

serviceVersion::serviceVersion(string hlr_sql_server, string hlr_sql_user, string hlr_sql_password, string hlr_sql_dbname )
{
	_hlrDb = new db(hlr_sql_server, hlr_sql_user, hlr_sql_password, hlr_sql_dbname );
	string logBuff = "serviceVersion():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
	hlr_log(logBuff,&logStream,6);
}

serviceVersion::serviceVersion(db* hlrDb)
{
	_hlrDb = hlrDb;
}

serviceVersion::~serviceVersion() {
	// TODO Auto-generated destructor stub
}

string serviceVersion::getConfFile() const
{
	return confFile;
}

string serviceVersion::getHost() const
{
	return host;
}

string serviceVersion::getLastShutdown() const
{
	return lastShutdown;
}

string serviceVersion::getLastStartup() const
{
	return lastStartup;
}

string serviceVersion::getLockFile() const
{
	return lockFile;
}

string serviceVersion::getLogFile() const
{
	return logFile;
}

string serviceVersion::getService() const
{
	return service;
}

string serviceVersion::getVersion() const
{
	return version;
}

void serviceVersion::setConfFile(string confFile)
{
	this->confFile = confFile;
}

void serviceVersion::setHost(string host)
{
	this->host = host;
}

void serviceVersion::setLastShutdown(string lastShutdown)
{
	this->lastShutdown = lastShutdown;
}

void serviceVersion::setLastStartup(string lastStartup)
{
	this->lastStartup = lastStartup;
}

void serviceVersion::setLockFile(string lockFile)
{
	this->lockFile = lockFile;
}

void serviceVersion::setLogFile(string logFile)
{
	this->logFile = logFile;
}

void serviceVersion::setService(string service)
{
	this->service = service;
}

void serviceVersion::setVersion(string version)
{
	this->version = version;
}

bool serviceVersion::tableExists()
{
	if ( _hlrDb->errNo == 0 )
	{
		dbResult result = _hlrDb->query("SHOW TABLES LIKE \"serviceVersion\"");
		if ( _hlrDb->errNo != 0 )
		{
			string logBuff = "serviceVersion:tableExists():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
			hlr_log(logBuff,&logStream,6);
			hlr_log("SHOW TABLES LIKE serviceVersion",&logStream,6);
			return false;
		}
		else
		{
			if ( result.numRows() != 0 )
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	else
	{
		string logBuff = "serviceVersion:tableExists():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
		hlr_log(logBuff,&logStream,6);
		return false;
	}
}

int serviceVersion::tableCreate()
{
	if ( _hlrDb->errNo == 0 )
	{
		string queryString = "";
		queryString = "CREATE TABLE serviceVersion";
		queryString += " (";
		queryString += " service char(64), ";
		queryString += " version varchar(255), ";
		queryString += " host varchar(255), ";
		queryString += " confFile varchar(255), ";
		queryString += " logFile varchar(255), ";
		queryString += " lockFile varchar(255), ";
		queryString += " lastStartup datetime, ";
		queryString += " lastShutdown datetime, ";
		queryString += "primary key (service))";
		dbResult result = _hlrDb->query(queryString);
		if ( _hlrDb->errNo != 0 )
		{
			string logBuff = "serviceVersion:tableCreate():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
			hlr_log(logBuff,&logStream,6);
			hlr_log(queryString,&logStream,6);
			return _hlrDb->errNo;
		}
		else
		{
			return 0;
		}
	}
	else
	{
		string logBuff = "serviceVersion:tableCreate():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
		hlr_log(logBuff,&logStream,6);
		return _hlrDb->errNo;
	}
}

int serviceVersion::write()
{
	if ( _hlrDb->errNo == 0 )
	{
		string queryStr = "REPLACE INTO serviceVersion VALUES ('";
		queryStr += service + "','";
		queryStr += version + "','";
		queryStr += host + "','";
		queryStr += confFile + "','";
		queryStr += logFile + "','";
		queryStr += lockFile + "',";
		queryStr += "''";//lastStartup placeholder
		queryStr += ",'')";//lastShutdown placeholder
		dbResult result = _hlrDb->query(queryStr);
		if ( _hlrDb->errNo != 0 )
		{
			string logBuff = "serviceVersion:write():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
			hlr_log(logBuff,&logStream,6);
			hlr_log(queryStr,&logStream,6);
			return _hlrDb->errNo;
		}
		else
		{
			return 0;
		}
	}
	else
	{
		string logBuff = "serviceVersion:write():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
		hlr_log(logBuff,&logStream,6);
		return _hlrDb->errNo;
	}
}



int serviceVersion::entries(vector<string> & v)
{
}



int serviceVersion::read(string & s)
{
}

int serviceVersion::updateStartup()
{
	if ( _hlrDb->errNo == 0 )
	{
		string queryStr = "UPDATE serviceVersion SET lastStartup=NOW() WHERE service='" + service + "'";
		dbResult result = _hlrDb->query(queryStr);
		if ( _hlrDb->errNo != 0 )
		{
			string logBuff = "serviceVersion:lastStartup():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
			hlr_log(logBuff,&logStream,6);
			return _hlrDb->errNo;
		}
		else
		{
			return 0;
		}
	}
	else
	{
		string logBuff = "serviceVersion:lastStartup():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
		hlr_log(logBuff,&logStream,6);
		return _hlrDb->errNo;
	}
}



int serviceVersion::updateShutdown()
{
	if ( _hlrDb->errNo == 0 )
	{
		string queryStr = "UPDATE jobTransSummary SET lastShutdown=NOW() WHERE service='" + service + "'";
		dbResult result = _hlrDb->query(queryStr);
		if ( _hlrDb->errNo != 0 )
		{
			string logBuff = "serviceVersion:lastStartup():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
			hlr_log(logBuff,&logStream,6);
			return _hlrDb->errNo;
		}
		else
		{
			return 0;
		}
	}
	else
	{
		string logBuff = "serviceVersion:lastStartup():_hlrDb.errNo=" + int2string(_hlrDb->errNo) + "," + _hlrDb->errMsg;
		hlr_log(logBuff,&logStream,6);
		return _hlrDb->errNo;
	}
}













