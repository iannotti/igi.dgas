// $Id: hlrTransIn.cpp,v 1.1.2.1.4.5 2012/06/27 07:34:17 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Authors: Andrea Guarise <andrea.guarise@to.infn.it>
/***************************************************************************
 * Code borrowed from:
 *  authors   : 
 *             
 *  
 ***************************************************************************/


// This class doesn't manage directly the timestamp field,
// it should be set up manually at higher levels!!

#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/hlr-service/base/hlrTransIn.h"
#include "glite/dgas/common/base/libdgas_log.h"

#define TOO_MANY_ROWS 1
#define NO_RECORD 2

extern ofstream logStream;

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_sql_dbname;


hlrTransIn::hlrTransIn( int _tid,
		string _rid,
		string _gid,
		string _fromDn,
		int _amount,
		string _timeStamp,
		string _dgJobId,
		string _uniqueChecksum,
		string _accountingProcedure)
{
	tid = _tid;
	rid = _rid;
	gid = _gid;
	fromDn = _fromDn;
	amount = _amount;
	timeStamp = _timeStamp;
	dgJobId = _dgJobId;
	uniqueChecksum = _uniqueChecksum;
	accountingProcedure = _accountingProcedure;
}


int hlrTransIn::get()
{
	db hlrDb ( hlr_sql_server, 
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		 );
	if ( hlrDb.errNo == 0 )
	{
		string _tid = int2string(tid);
		if (tid == 0 ) _tid = "%";
		string queryStr =  "SELECT * FROM trans_in WHERE ";
		queryStr += "tid LIKE '" + _tid + "' ";
		if ( rid != "" ) queryStr += "AND rid LIKE '" + rid + "' ";
		if ( gid != "" ) queryStr += "AND gid LIKE '" + gid + "' ";
		if ( fromDn != "" ) queryStr += "AND from_dn LIKE '" + fromDn + "' ";
		if ( timeStamp != "" ) queryStr += "AND tr_stamp LIKE '" + timeStamp + "' ";
		if ( dgJobId != "" ) 
		{
			if ( dgJobId.rfind("%") != string::npos )
			{
				queryStr += "AND dgJobId LIKE '" + dgJobId + "'";
			}
			else
			{
				queryStr += "AND dgJobId='" + dgJobId + "'";
			}
		}
		if ( uniqueChecksum != "" ) queryStr += "AND uniqueChecksum='" + uniqueChecksum + "'";
		if ( accountingProcedure != "" ) queryStr += "AND accountingProcedure='" + accountingProcedure + "'";
		 dbResult result = hlrDb.query(queryStr);
		 if ( hlrDb.errNo == 0)
		 {
			//FIXME convert to switch case statement
			int numRows = result.numRows();
			if ( numRows > 1 )
			{
				return TOO_MANY_ROWS;
			}
			if ( numRows == 0 )
			{
				return NO_RECORD;
			}
			if ( numRows == 1 )
			{
				tid = atoi(result.getItem(0,0).c_str());
				rid = result.getItem(0,1);
				gid = result.getItem(0,2);
				fromDn = result.getItem(0,3);
				amount = atoi(result.getItem(0,4).c_str());
				timeStamp = result.getItem(0,5);
				dgJobId = result.getItem(0,6);
				uniqueChecksum = result.getItem(0,7);
				accountingProcedure = result.getItem(0,8);
				 return 0;
			}
		 }
		 else
		 {
			return hlrDb.errNo;
		 }
	}
	else
	{
		//connection to the DB failed, return error
		return hlrDb.errNo; 
	}
	return 0;
}

int hlrTransIn::put()
{
	db hlrDb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
	       );
	if ( hlrDb.errNo == 0)
	{
		//connection ok, go on
		string queryStr;
		queryStr = "INSERT INTO trans_in VALUES (";
		queryStr += "0,'";
		queryStr += rid + "','";
		queryStr += gid + "','";
		queryStr += fromDn + "',";
		queryStr += int2string(amount) + ",";
		queryStr += timeStamp + ",'";
		queryStr += dgJobId + "','";
		queryStr += uniqueChecksum + "','";
		queryStr += accountingProcedure + "')";
		hlrDb.query(queryStr);
		if ( hlrDb.errNo != 0 )
		{
			return hlrDb.errNo;
		}	
		queryStr = "SELECT LAST_INSERT_ID()";
		dbResult result = hlrDb.query(queryStr);
		if ( hlrDb.errNo == 0 )
		{
			tid = atoi(result.getItem(0,0).c_str());
		}
		return 0;
	}
	else
	{
		return hlrDb.errNo; 
	}
}



int hlrTransIn::del()
{
	db hlrDb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (hlrDb.errNo == 0)
	{
		string queryStr;
		//FIXME modify to use wildcards
		queryStr = "DELETE FROM trans_in WHERE tid=";
		queryStr += int2string(tid);
		hlrDb.query(queryStr);
		if ( hlrDb.errNo != 0 )
		{
			return hlrDb.errNo;
		}
		else
		{
			return 0;
		}
	}
	else
	{
		return hlrDb.errNo; 
	}
}



// friend
// makes a specific transaction obsolete by setting rid and gid to NULL
int makeTransInObsolete(string &jobId)
{
	db hlrDb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (hlrDb.errNo == 0)
	{
		string queryStr;
		queryStr = "DELETE FROM trans_in WHERE ";
		queryStr += "dgJobId='" + jobId + "'";
		hlrDb.query(queryStr);
		if ( hlrDb.errNo != 0 )
		{
			return hlrDb.errNo;
		}
		else
		{
			//DELETE from jobTransSummary removed. It caused troubles (see tkt 9056)
                        //when the deleted record was the last in the table: autoincremet id was
                        //diminishing. This triggered a record reset in case urForward was publishing
                        //to second Level HLR before new records get inserted.
			// now try to delete the corresponding entry in jobTransSummary
			//queryStr = "DELETE FROM jobTransSummary WHERE dgJobId='"+jobId;
			//queryStr += "'";
			//hlrDb.query(queryStr);
			// don't complain if anything goes wrong since the table might
			// not exist or the record not yet created (done
			// asynchronously)!
			queryStr = "DELETE FROM transInLog WHERE dgJobId='"+jobId;
			queryStr += "'";
			hlrDb.query(queryStr);
			return 0;
		}
	}
	else
	{
		return hlrDb.errNo; 
	}
}


bool hlrTransIn::exists()
{
	db hlrDb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (hlrDb.errNo == 0)
	{	
		string _tid = int2string(tid);
		if (tid == 0 ) _tid = "%";
		string queryStr =  "SELECT tid FROM trans_in WHERE ";
		queryStr += "tid LIKE '" + _tid + "' ";
		if ( rid != "" ) queryStr += "AND rid LIKE '" + rid + "' ";
		if ( gid != "" ) queryStr += "AND gid LIKE '" + gid + "' ";
		if ( fromDn != "" ) queryStr += "AND from_dn LIKE '" + fromDn + "' ";
		if ( timeStamp != "" ) queryStr += "AND tr_stamp LIKE '" + timeStamp + "' ";
		if ( dgJobId != "" ) 
		{
			if ( dgJobId.rfind("%") != string::npos )
			{
				queryStr += "AND dgJobId LIKE '" + dgJobId + "'";
			}
			else
			{
				queryStr += "AND dgJobId='" + dgJobId + "'";
			}
		}
		if ( uniqueChecksum != "" ) queryStr += "AND uniqueChecksum='" + uniqueChecksum + "'";
		if ( accountingProcedure != "" ) queryStr += "AND accountingProcedure='" + accountingProcedure + "'";
		dbResult result = hlrDb.query(queryStr);
		string logBuff = "TransIn:exists(), Query:" + queryStr;
		 hlr_log (logBuff, &logStream,8);
		if ( hlrDb.errNo == 0)
		{
			if ( result.numRows() == 1 )
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
	}
	else
	{
		return false; 
	}
}
