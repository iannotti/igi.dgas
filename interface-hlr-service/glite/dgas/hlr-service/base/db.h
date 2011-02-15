// $Id: db.h,v 1.1.2.3 2011/02/15 09:47:49 aguarise Exp $
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

#ifndef DB_H
#define DB_H
#ifdef WITH_UNIXODBC
	#include <sql.h>
#else
	extern "C" {
	#include <mysql.h>
	}
#endif

#ifdef WITH_UNIXODBC
#define E_DB_DUPLICATE_ENTRY 23000 //FIXME this won't work!
#else
#define E_DB_DUPLICATE_ENTRY 1022 
#endif

#include <string>
#include <iostream>
#include "glite/dgas/hlr-service/base/dbResult.h"

using namespace std;

class db {
public:
	db ();
	db (string serverI, string userI, string passwdI, string dbNameI);
	~db ();
	dbResult::dbResult  query ( string queryString );
	string escape_string(string& input);
	long long getAffectedRows();
	int errNo;
	string errMsg;
	#ifdef WITH_UNIXODBC
	SQLHDBC dbhandle;
	SQLHENV odbc_env;
	#else
	MYSQL* dbhandle;
	#endif
private:
	string server;
	string user;
	string passwd;
	string dbName;
	long long affectedRows;

	db (const db&) ; // avoid copying of dbhandle
	db& operator=(const db&) ; 

};

#endif
