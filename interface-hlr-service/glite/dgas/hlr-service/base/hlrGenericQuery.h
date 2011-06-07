// $Id: hlrGenericQuery.h,v 1.1.2.1.4.3 2011/06/07 12:08:45 aguarise Exp $
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
#ifndef hlrGenericQuery_h
#define hlrGenericQuery_h

#include<string>
#include<vector>
#include<iostream>
#include<sstream>

using namespace std;

typedef vector<string> resultRow;
typedef vector<resultRow> hlrQueryResult;

class hlrGenericQuery {
	public:
		string queryString;
		string dbname;
		string dbuser;
		string dbpassword;
		int errNo;
		string errMsg;
		hlrQueryResult queryResult;

		hlrGenericQuery();
		hlrGenericQuery(string _query ); 
		hlrGenericQuery(string _dbname, string _query ); 
		hlrGenericQuery(string _dbname, string _dbuser, string _dbpassword, string _query );

		int query(string _query=""); //actually performs the query
		size_t Rows(){return numRows;};
		size_t Columns(){return numColumns;};
		vector<string> fieldNames;
	protected: 
		size_t numRows;
		size_t numColumns;

};

#endif



