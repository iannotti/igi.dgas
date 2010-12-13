
#include <sstream>
#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/int2string.h"   
#include "glite/dgas/hlr-service/base/transInLog.h"

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_sql_dbname;



transInLog::transInLog (
		string _dgJobId, 
		string _log)
{
	dgJobId = _dgJobId;
	log = _log;
}

int transInLog::put()
{
	db userHlr(hlr_sql_server, 
			hlr_sql_user, 
			hlr_sql_password, 
			hlr_sql_dbname);
	if (userHlr.errNo == 0)
	{
		string queryStr = "INSERT INTO transInLog VALUES ('";
		queryStr += dgJobId;
		queryStr += "','";
		queryStr += log;
		queryStr += "')";
		userHlr.query(queryStr);
		return userHlr.errNo;
		
	}
	else
	{
		return userHlr.errNo;
	}
	
}

int transInLog::get(string dgJobId)
{
	db userHlr(hlr_sql_server,
	                hlr_sql_user,
                        hlr_sql_password,
                        hlr_sql_dbname);
	if (userHlr.errNo == 0)
	{
		string queryStr = "SELECT * FROM transInLog WHERE dgJobId = '";
		queryStr+= dgJobId;
		queryStr+= "'";
		dbResult result = userHlr.query(queryStr);
		if ( userHlr.errNo == 0 )
		{
			if ( result.numRows() == 1 )
			{
				dgJobId = result.getData()[0][0];
				log = result.getData()[0][1];
				return 0;
			}
			else
			{
				return 1;
			}
		}
		else
		{
			return userHlr.errNo;
		}
	}
	return userHlr.errNo;
}


int transInLog::remove()
{
        db userHlr(hlr_sql_server,
                        hlr_sql_user,
                        hlr_sql_password,
                        hlr_sql_dbname);
        if (userHlr.errNo == 0)
        {
		string queryStr = "DELETE  FROM transInLog WHERE dgJobId = '";
		queryStr += dgJobId;
		queryStr += "'";
		userHlr.query(queryStr);
		return userHlr.errNo;
		
	}
	else
	{
		return userHlr.errNo;
	}

}
