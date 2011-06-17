// $Id: dbResult.h,v 1.1.2.2 2011/06/17 08:42:48 aguarise Exp $
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

#ifndef DBRESULT_H
#define DBRESULT_H

#include<iostream>
#include<string>
#include<vector>
#ifdef WITH_UNIXODBC
#include <sql.h>
#include <sqlext.h>
#else
extern "C" {
#include<mysql.h>
}
#endif

using namespace std;

typedef vector<string> Row;
typedef vector<Row> Res;
class dbResult {
public:
	dbResult ();
#ifdef WITH_UNIXODBC
	dbResult (SQLHDBC& dbh, string& query, long long& r);
#else
	dbResult (MYSQL_RES * res);
#endif
	size_t numRows() { return Rows; };
	size_t numFields() { return Fields; };
	Res getData() { return Result; };
	Row getRow(size_t row);
	string getItem(size_t row, size_t column);
	vector<string> fieldNames;	

private:
	Res 	    Result;
	size_t	    Rows;
	size_t	    Fields;

};
#endif
