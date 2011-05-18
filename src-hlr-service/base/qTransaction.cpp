#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/hlr-service/base/qTransaction.h"
#include <fstream>

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_tmp_sql_dbname;
extern string qtransInsertLog;

qTransaction::qTransaction (
		string _transactionId,
		string _gridUser,
		string _gridResource,
		string _urSource,
		string _timestamp,
		string _logData,
		int _priority,
		int _statusTime,
		string _uniqueChecksum,
		string _accountingProcedure
		)
{
	transactionId = _transactionId;
	gridUser = _gridUser;
	gridResource = _gridResource;
	urSource = _urSource;
	timestamp = _timestamp;
	logData = _logData;
	priority = _priority;
	statusTime = _statusTime;
	uniqueChecksum = _uniqueChecksum;
	accountingProcedure = _accountingProcedure;
}

int qTransaction::put()
{
	 db hlrTmp(hlr_sql_server,
			 hlr_sql_user,
			 hlr_sql_password,
			 hlr_tmp_sql_dbname);
	 if (hlrTmp.errNo == 0)
	 {
		 string queryStr = "INSERT INTO trans_queue VALUES ('";
		 queryStr += transactionId;
		 queryStr += "','";
		 queryStr += gridUser;
		 queryStr += "','";
		 queryStr += gridResource;
		 queryStr += "','";
		 queryStr += urSource;
		 queryStr += "',0";
		 queryStr += ",'";
		 queryStr += timestamp;
		 queryStr += "','";
		 queryStr += logData;
		 queryStr += "',";
		 queryStr += int2string(priority);
		 queryStr += ",";
		 queryStr += int2string(statusTime);
		 queryStr += ",'";
		 queryStr += uniqueChecksum;
		 queryStr += "','";
		 queryStr += accountingProcedure;
		 queryStr += "')";
		 hlrTmp.query(queryStr);
		 int res =  hlrTmp.errNo;
		if ( res == 0 )
		{
			if ( qtransInsertLog != "" )
			{
				ofstream iLog;
				iLog.open(qtransInsertLog.c_str(), ios::app);
				if ( !(iLog) )
				{
					return atoi(E_NO_DB);
				}
				else
				{
					iLog << queryStr << endl;
					iLog.close();
				}
			}
		}
		return res;
	 }
	 else
	 {
		  return atoi(E_NO_DB);
	 }
}

int qTransaction::get(int _priority)
{
	 db hlrTmp(hlr_sql_server,
			 hlr_sql_user,
			 hlr_sql_password,
			 hlr_tmp_sql_dbname);
	 if (hlrTmp.errNo == 0)
	 {
		 string queryStr = "SELECT * FROM trans_queue WHERE priority =";
		 queryStr += int2string(_priority);
		 dbResult result = hlrTmp.query(queryStr);
		 if ( hlrTmp.errNo == 0 )
		 {
			 //query fine
			 if (result.numRows() == 0)
			 {
				 //no records with this priority found;
				 return 1;
			 }
			 else
			 {
				 //found some records, return th first;
				 transactionId=result.getItem(0,0);
				 gridUser=result.getItem(0,1);
				 gridResource=result.getItem(0,2);
				 urSource=result.getItem(0,3);
				 amount=atoi(result.getItem(0,4).c_str());
				 timestamp = result.getItem(0,5);
				 logData = result.getItem(0,6);
				 priority = atoi(result.getItem(0,7).c_str());
				 statusTime = atoi(result.getItem(0,8).c_str());
				 uniqueChecksum = result.getItem(0,9);
				 accountingProcedure = result.getItem(0,10);
				 return 0;
			 }
		 }
		 else
		 {
			 return hlrTmp.errNo;
		 }
		 
	 }
	 else
	 {
		 return hlrTmp.errNo;
	 }
	
}

int qTransaction::get(string _transactionId)
{
         db hlrTmp(hlr_sql_server,
                         hlr_sql_user,
                         hlr_sql_password,
                         hlr_tmp_sql_dbname);
         if (hlrTmp.errNo == 0)
         {
                 string queryStr = "SELECT * FROM trans_queue WHERE transaction_id ='";
                 queryStr += _transactionId;
                 queryStr += "'";
                 dbResult result = hlrTmp.query(queryStr);
                 if ( hlrTmp.errNo == 0 )
                 {
                         //query fine
                         if (result.numRows() == 0)
                         {
                                 //no records with this priority found;
                                 return 1;
                         }
                         else
                         {
                                 //found some records, return th first;
                                 transactionId=result.getItem(0,0);
				 gridUser=result.getItem(0,1);
				 gridResource=result.getItem(0,2);
				 urSource=result.getItem(0,3);
				 amount=atoi(result.getItem(0,4).c_str());
				 timestamp = result.getItem(0,5);
				 logData = result.getItem(0,6);
				 priority = atoi(result.getItem(0,7).c_str());
				 statusTime = atoi(result.getItem(0,8).c_str());
				 uniqueChecksum = result.getItem(0,9);
				 accountingProcedure = result.getItem(0,10);
				return 0;
                         }
                 }
                 else
                 {
                         return hlrTmp.errNo;
                 }

         }
         else
         {
                 return hlrTmp.errNo;
         }
}

int qTransaction::remove()
{
	 db hlrTmp(hlr_sql_server,
			 hlr_sql_user,
			 hlr_sql_password,
			 hlr_tmp_sql_dbname);
	 if (hlrTmp.errNo == 0)
	 {
		 
		 string queryStr = "DELETE FROM trans_queue WHERE transaction_id = '";	 
		 queryStr += transactionId;
		 queryStr += "'";
		 hlrTmp.query(queryStr);
		 return hlrTmp.errNo;
	 }
	 else
	 {
		 return hlrTmp.errNo;
	 }
	
}

int qTrans::removeGreaterThan(int pri)
{
	 db hlrTmp(hlr_sql_server,
			 hlr_sql_user,
			 hlr_sql_password,
			 hlr_tmp_sql_dbname);
	 if (hlrTmp.errNo == 0)
	 {
		 
		 string queryStr = "DELETE FROM trans_queue WHERE priority > ";	 
		 queryStr += pri;
		 hlrTmp.query(queryStr);
		 return hlrTmp.errNo;
		 
	 }
	 else
	 {
		 return hlrTmp.errNo;
	 }
	
}

int qTrans::archiveGreaterThan(int pri,string file)
{
	 db hlrTmp(hlr_sql_server,
			 hlr_sql_user,
			 hlr_sql_password,
			 hlr_tmp_sql_dbname);
	 if (hlrTmp.errNo == 0)
	 {
		 
		 string queryStr = "SELECT * INTO OUTFILE \"" + file +"\" ";
			queryStr +="FROM  trans_queue WHERE priority > ";
			queryStr += pri;

		 hlrTmp.query(queryStr);
		 return hlrTmp.errNo;
		 
	 }
	 else
	 {
		 return hlrTmp.errNo;
	 }
}


int qTransaction::update()
{
	 db hlrTmp(hlr_sql_server,
			 hlr_sql_user,
			 hlr_sql_password,
			 hlr_tmp_sql_dbname);
	 if (hlrTmp.errNo == 0)
	 {
	 	string queryStr = "UPDATE trans_queue SET ";
		queryStr += "priority=";
		queryStr += int2string(priority);
		queryStr += " , ";
		queryStr += "status_time=";
		queryStr += int2string(statusTime);
		queryStr += " WHERE transaction_id='";
		queryStr += transactionId;
		queryStr += "'";
		hlrTmp.query(queryStr);
		return hlrTmp.errNo;
	 }
	 else
	 {
		 return hlrTmp.errNo;
	 }
}

int qTrans::get(int _priority, vector<string> &keys)
{
         db hlrTmp(hlr_sql_server,
                         hlr_sql_user,
                         hlr_sql_password,
                         hlr_tmp_sql_dbname);
         if (hlrTmp.errNo == 0)
         {
                 string queryStr = "SELECT * FROM trans_queue WHERE priority =";
                 queryStr += int2string(_priority);
                 dbResult result = hlrTmp.query(queryStr);
                 if ( hlrTmp.errNo == 0 )
                 {
                         //query fine
                         if (result.numRows() == 0)
                         {
                                 //no records with this priority found;
                                 return 1;
                         }
                         else
                         {
                                 //found some records, return th first;
                                 for (unsigned int i=0; i< result.numRows(); i++)
                                 {
                                        string buffer;
                                        buffer=result.getItem(i,0);
                                        keys.push_back(buffer);
                                 }
                                 return 0;
                         }
                 }
                 else
                 {
                         return hlrTmp.errNo;
                 }
         }
         else
         {
                 return hlrTmp.errNo;
         }
}
