#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/stringSplit.h"     
#include "glite/dgas/common/base/xmlUtil.h"     
#include "glite/dgas/hlr-service/base/hlrTransaction.h"
#include "glite/dgas/hlr-service/base/transInLog.h"
#include "glite/dgas/hlr-service/base/hlrResource.h"
#include"glite/dgas/hlr-service/base/hlrTransIn.h"
#include"glite/dgas/common/base/libdgas_log.h"


#define E_GET_TRANSIN 	1
#define E_GET_RESOURCE 	2
#define E_PUT_TRANSIN 	5
#define E_PUT 		7
#define E_DEL_TRANSIN 	9
#define E_DEL 		10
#define E_GET 		11
#define E_PROCESS 	14

extern const char * hlr_def_port;
extern const char * hlr_server_contact;

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_sql_dbname;

extern ofstream logStream;

extern bool lazyAccountCheck;

hlrTransaction::hlrTransaction(
			       int _tid,
			       string _id,
			       string _gridUser,
			       string _gridResource,
			       string _urSource,
			       int _amount,
			       string _timeStamp,
			       string _logData,
			       string _uniqueChecksum,
			       string _accountingProcedure
			     )
{
	tid = _tid;
	id =_id;
	gridUser=_gridUser;
	gridResource=_gridResource;
	urSource=_urSource;
	amount=_amount;
	timeStamp=_timeStamp;
	logData=_logData;
	uniqueChecksum=_uniqueChecksum;
	accountingProcedure=_accountingProcedure;
}

// make a specific transaction obsolote
int makeObsolete(string &jobId)
{
	return makeTransInObsolete(jobId);
}

bool hlrTransaction::exists()
{
	bool res = false;
        string logBuff;
	res = this->existsIn();
	if ( res )
	{
		logBuff="hlrTransaction::exists():, record for job: " +
			this->id + " exists.";
		hlr_log(logBuff, &logStream, 8);
	}
	else
	{
		logBuff="hlrTransaction::exists():, record for job: " +
			this->id + " doesn't exists.";
		hlr_log(logBuff, &logStream, 8);
	}
	return res;
}

int hlrTransaction::process()
{
	hlr_log("hlrTransaction::process(): processing record",&logStream, 8);
	//process the transaction
	//insert transaction record in the Db
	if ( this->put() != 0 )
	{
		hlr_log("hlrTransaction::process(): record not inserted!",&logStream,3);
		return E_PROCESS;
	}
	else
	{
		hlr_log("hlrTransaction::process(): record processed!", &logStream, 7);
		return 0;
	}
}


//Private methods


int hlrTransaction::get()
{
	//retrieve rid and gid from toId
	hlrResource rBuff;
	if ( gridResource != "" )
	{
		rBuff.ceId=gridResource;
		rBuff.connectionInfo = connectionInfo;
		if ( rBuff.get() != 0 ) //get the object containing
		{			//the resource info
			 return E_GET_RESOURCE;
		}
	}
	hlrTransIn TransInBuff(
			tid,
			rBuff.rid, 
			rBuff.gid,
			gridUser,
			amount,
			timeStamp,
			id,
			uniqueChecksum,
			accountingProcedure
			);
	if ( TransInBuff.get() != 0 )
	{
		//there were errors retrieving the transaction
		string logBuff = "hlrTransaction::get(): E_GET_TRANSIN,";
		        logBuff += "Error retrieving resInfo for:";
			logBuff += TransInBuff.tid;
			logBuff += "uniqueChecksum:" + uniqueChecksum;
        		hlr_log(logBuff,&logStream,7);
		return E_GET_TRANSIN;
	}
	if ( gridResource == "" )
	{
		//we don't have the toId, so we can find it from 
		//the rid value
		rBuff.rid = TransInBuff.rid;
		rBuff.connectionInfo = connectionInfo; //for auth (added from)
							//infnforge
		if ( rBuff.get() != 0 )
		{
			string logBuff = "hlrTransaction::get(): E_GET_RESOURCE,";
		        logBuff += "Error retrieving resInfo for:";
			logBuff += rBuff.rid;
        		hlr_log(logBuff,&logStream,7);
			return E_GET_RESOURCE;
		}
	}
	tid = TransInBuff.tid;
        id = TransInBuff.dgJobId;
        gridUser = TransInBuff.fromDn;
        amount = TransInBuff.amount;
        timeStamp = TransInBuff.timeStamp;
        gridResource = rBuff.ceId;
	//find the logData (if any)
	transInLog tLogBuff;
	if ( tLogBuff.get(id) !=0 )
	{
		string logBuff = "hlrTransaction::getIn(): ";
	        logBuff += "Error retrieving transaction log for:";
		logBuff += TransInBuff.dgJobId;
        	hlr_log(logBuff,&logStream,7);
	}
	else
        {
                logData = tLogBuff.log;
        }
	return 0;
}

int getJobId(string &uniqueChecksum, string &jobIdResult,
	       string &accountingProcedureResult) 
{
	db hlrDb ( hlr_sql_server,
		hlr_sql_user,
		hlr_sql_password,
		hlr_sql_dbname
		);
	if ( hlrDb.errNo == 0 )
	{
		string querystr = "SELECT dgJobId,accountingProcedure FROM ";
		querystr += "trans_in WHERE uniqueChecksum='";
		querystr += uniqueChecksum + "'";
        	hlr_log(querystr,&logStream,7);
		dbResult result = hlrDb.query(querystr);
		if ( hlrDb.errNo == 0)
		{
			//FIXME WHATIF multiple result???
			if ( result.numRows() > 0 )
			{
				jobIdResult = result.getItem(0,0);	
				accountingProcedureResult = result.getItem(0,1);
        			hlr_log("Found matching entries",&logStream,7);
				return 0;
			}	
			else
			{
        			hlr_log("Don't found matching entries",&logStream,7);
				return 1;
			}
		}
		else
		{
        		hlr_log("QUERY NOT OK,ERROR",&logStream,7);
			
			return hlrDb.errNo;
		}
	}
	else
	{
        	hlr_log("QUERY NOT OK,DB",&logStream,7);
		return hlrDb.errNo;
	}
        hlr_log("QUERY NOT OK,END of funtction",&logStream,7);
}

//int hlrTransaction::putIn()
int hlrTransaction::put()
{
	//retrieve rid and gid of the resource (to item )from toId
	hlrResource rBuff;
	if ( lazyAccountCheck )
	{
		rBuff.rid = gridResource;
		rBuff.gid = "NONE";
	}
	else
	{
		if ( gridResource != "" )
		{
			rBuff.ceId=gridResource;
			if ( rBuff.get() != 0 ) //get the object containing
			{			//the resource info
				 return E_GET_RESOURCE;
			}
		}
	}
	hlrTransIn transInBuff(0,
			rBuff.rid,
			rBuff.gid,
			gridUser,
			amount,
			timeStamp,
			id,
			uniqueChecksum,
			accountingProcedure);
	if ( transInBuff.put() != 0 )
	{
		//there was an error inserting the transaction.
		string logBuff = "hlrTransaction::put(): ";
                logBuff += "Error inserting the hlrTransIn data for:";
                logBuff += transInBuff.dgJobId;
                logBuff += ",";
                logBuff += transInBuff.uniqueChecksum;
                hlr_log(logBuff,&logStream,3);
		return E_PUT_TRANSIN;
	}
		tid= transInBuff.tid;
		string logBuff = "hlrTransaction::put(): ";
                logBuff += "Inserted the hlrTransIn data for:";
                logBuff += transInBuff.dgJobId;
                logBuff += ",";
                logBuff += transInBuff.uniqueChecksum;
                logBuff += ",";
                logBuff += int2string(tid);
                hlr_log(logBuff,&logStream,9);
	return 0;
}

//int hlrTransaction::delIn()
int hlrTransaction::del()
{
	hlrResource rBuff;
	if ( gridResource != "" )
	{
		rBuff.ceId = gridResource;
		if ( rBuff.get() != 0 ) //get the object containing
		{			//the suer info
			 return E_GET_RESOURCE;
		}
	}
	hlrTransIn TransInBuff(
			tid,
			rBuff.rid,
			rBuff.gid,
			gridUser,
			amount,
			timeStamp,
			id,
			uniqueChecksum,
			accountingProcedure
			);
	if ( TransInBuff.del() != 0 )
	{
		//there was an error deleting the transaction(s).
		 return E_DEL_TRANSIN;
	}
	else
	{
		//FIXME here we must delete the transaction log record
	}
	return 0;
}

bool hlrTransaction::existsIn()
{
	hlrTransIn TransInBuff(
			tid,
			"",
			"",
			gridUser,
			amount,
			timeStamp,
			id,
			uniqueChecksum,
			accountingProcedure
			);
	return TransInBuff.exists();
}

ostream& operator << ( ostream& os, const hlrTransaction& tr )
{
	os << "tid=" << tr.tid;
	os << ",gridUser=" << tr.gridUser;
	os << ",gridResource=" << tr.gridResource;
	os << ",urSource=" << tr.urSource;
	os << ",amount=" << tr.amount;
	os << ",timeStamp=" << tr.timeStamp;
	os << ",logData=" << tr.logData;
	os << ",uniqueChecksum=" << tr.uniqueChecksum;
	os << ",accountingProcedure=" << tr.accountingProcedure;
	return os;
}

