
// $Id: db.cpp,v 1.1.2.6 2011/11/08 08:36:21 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
/***************************************************************************
 * Code borrowed from:
 *  authors   : 
 *             
 *  
 ***************************************************************************/


#include "glite/dgas/hlr-service/base/db.h"


#ifdef WITH_UNIXODBC
#include <sql.h>
#include <sqlext.h>
#endif


#ifndef WITH_UNIXODBC
db::db ( string serverI, 
		string userI, 
		string passwdI,
		string dbNameI )
{
	server = serverI;//s
	user   = userI;//s
	passwd = passwdI;//s
        dbName = dbNameI;//s
	unsigned int retry = 0;//s
	affectedRows = 0;
	dbhandle = new MYSQL;  
	do {
		errNo = 0;//s
		errMsg = "";//s
                mysql_init(dbhandle);
                mysql_options(dbhandle,MYSQL_READ_DEFAULT_GROUP,"dgas");

		if (!mysql_real_connect(dbhandle,server.c_str(),user.c_str(),passwd.c_str(),dbName.c_str(),0,NULL,0))
 	       {
			cerr << "problem connecting to server:" << serverI << " error:" << mysql_error(dbhandle);
                        errNo= mysql_errno(dbhandle);
                        mysql_close(dbhandle);
                        sleep(retry);
                        cerr << ";Retrying." << endl;
                        retry++;
	       }
	} while ( (errNo != 0) && ( retry < 13 ) );
	if ( errNo != 0 )
		cerr << "Warning: reached maximum retry value!" << endl;
}

string db::escape_string(string& input)
{
	unsigned long inputLength = input.size();
	char buffer[(inputLength*2)+1];
	mysql_real_escape_string(dbhandle,buffer,input.c_str(),inputLength);
	return buffer;
}

db::~db ()
{
	mysql_close(dbhandle);
	delete dbhandle;
}

dbResult::dbResult db::query ( string queryString )
{
	if ( ( mysql_real_query( dbhandle, queryString.c_str(), queryString.size() ) ) != 0 )
	{
		errNo=mysql_errno(dbhandle);
		errMsg = mysql_error(dbhandle);
		affectedRows =0;
		return NULL;
	}
	else
	{
		errNo=0;
		errMsg = "";
		affectedRows = mysql_affected_rows(dbhandle);
		return mysql_store_result(dbhandle);
	}
}

long long db::getAffectedRows() 
{
	affectedRows = mysql_affected_rows(dbhandle);
	return affectedRows;
}
#else

static void extract_error(
	char*fn,
	SQLHANDLE handle,
	SQLSMALLINT type);

void extract_error(
        char*fn,
        SQLHANDLE handle,
        SQLSMALLINT type)
{
	SQLINTEGER i = 0;
	SQLINTEGER native;
	SQLCHAR state[7];
	SQLCHAR text[256];
	SQLSMALLINT len;
	SQLRETURN ret;
	cerr << "The driver reported following diagnostic while running:" <<
		fn << endl;
	do 
	{
		ret = SQLGetDiagRec(type, handle, ++i, state, &native, text ,
					sizeof(text),&len);
		if (SQL_SUCCEEDED(ret))
		{
			cerr << state << ":" << i << ":" << native << ":" << text << endl;
		}
	} while ( ret == SQL_SUCCESS );
}

db::db ( string serverI,
                string userI,
                string passwdI,
                string dbNameI )
{
        server = serverI;
        user   = userI;
        passwd = passwdI;
        dbName = dbNameI;
        errNo=0;
        //mysql_init(dbhandle);
	SQLRETURN odbc_ret;
	//SQLCHAR odbc_outstr[1024];
	//SQLSMALLINT odbc_outstrlen;

	SQLAllocHandle(SQL_HANDLE_ENV, 
			SQL_NULL_HANDLE, 
			&odbc_env);
	SQLSetEnvAttr(odbc_env, 
			SQL_ATTR_ODBC_VERSION,
			(void *) SQL_OV_ODBC3,
			0);
	SQLAllocHandle(SQL_HANDLE_DBC,
			odbc_env,
			dbhandle);
	string dsn = "DSN=" + dbName + ";";
	odbc_ret = SQLDriverConnect(dbhandle, NULL, (SQLCHAR *) dsn.c_str(),
			SQL_NTS,
			NULL,
			0,
			NULL,
			SQL_DRIVER_NOPROMPT);
        //mysql_options(dbhandle,MYSQL_READ_DEFAULT_GROUP,"a.out");
	/*
        if (!mysql_real_connect(dbhandle,server.c_str(),user.c_str(),passwd.c_str(),dbName.c_str(),0,NULL,0))
        {
                cerr << "Failed to connect to server" << endl;
                errNo= -1;
        }
	*/
	if ( SQL_SUCCEEDED(odbc_ret) )
	{
		#ifdef DEBUG
		cout << "connected to:" << dsn << endl;
		#endif
		errNo = 0;
	}
	else
	{
		extract_error("SQLDriverConnect", dbhandle, SQL_HANDLE_DBC);
	        //errNo=mysql_errno(dbhandle);
		//FIXME Find how to get an error number!
		cout << "Error conencting to to:" << dsn << endl;
		errNo = -1;
	}
}

db::~db ()
{
	SQLDisconnect(dbhandle);
	SQLFreeHandle(SQL_HANDLE_DBC, dbhandle);
	SQLFreeHandle(SQL_HANDLE_ENV, odbc_env);
        //mysql_close(dbhandle);
}

dbResult::dbResult db::query ( string queryString )
{
	return dbResult::dbResult(dbhandle,queryString,affectedRows);
}

long long db::getAffectedRows()
{
        return affectedRows;
}
#endif
