// $Id: hlrGenericQuery.cpp,v 1.1.2.1 2010/10/13 12:59:49 aguarise Exp $
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


#include "glite/dgas/common/base/db.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/hlr-service/base/hlrGenericQuery.h"

#define NO_RECORD 1

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_sql_dbname;

hlrGenericQuery::hlrGenericQuery( string _dbname, string _query )
{
	errNo = 0;
	errMsg = "";
	queryString = _query;
	dbname = _dbname;
}

hlrGenericQuery::hlrGenericQuery( string _query )
{
	errNo = 0;
	errMsg = "";
	queryString = _query;
	dbname = hlr_sql_dbname;
}

hlrGenericQuery::hlrGenericQuery()
{
	errNo = 0;
	errMsg = "";
	dbname = hlr_sql_dbname;
}

int hlrGenericQuery::query(string _query)
{
	db hlrDb ( hlr_sql_server, 
			hlr_sql_user,
			hlr_sql_password,
			dbname
		 );
	if ( hlrDb.errNo == 0 )
	{
		//connection to the DB Ok, go on
		if (_query != "" ) queryString = _query;
		 dbResult result = hlrDb.query(queryString);
		 if ( hlrDb.errNo == 0 )
		 {
			numRows = result.numRows();
			if ( numRows == 0 )
			{
				return NO_RECORD;
				errNo = NO_RECORD;
			}
			queryResult = result.getData();
			fieldNames = result.fieldNames;
			numColumns = result.numFields();
			return 0;
		 }
		 else
		 {
			if ( hlrDb.errNo != 0 )
			{
				errNo = hlrDb.errNo;
				errMsg = hlrDb.errMsg;
				return hlrDb.errNo;
			}
			else
			{
				errNo = -1;
				return -1;
			}
		 }
	}
	else
	{
		//connection to the DB failed, return error
		errNo = hlrDb.errNo;
		errMsg = hlrDb.errMsg;
		return hlrDb.errNo; 
	}
}

