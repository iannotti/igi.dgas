// $Id: dbResult.cpp,v 1.1.2.4 2011/06/21 13:01:26 aguarise Exp $
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

#include "glite/dgas/hlr-service/base/dbResult.h"

#ifndef WITH_UNIXODBC
dbResult::dbResult (MYSQL_RES * res)
{
	Rows = 0;
	Fields = 0;
	if (res != NULL)
	{
		Rows = mysql_num_rows(res);
		Fields = mysql_num_fields(res);
		MYSQL_ROW row_buff;
		Row Row_buff;
		if ( Fields != 0) Row_buff.reserve(Fields);
		if ( Rows != 0) Result.reserve(Rows);
		while (( row_buff = mysql_fetch_row(res)))
		{
			for (size_t i = 0; i < Fields; i++)
			{
				if ( row_buff[i] != NULL) 
				{
					Row_buff.push_back(row_buff[i]);
				}
				else
				{
					 Row_buff.push_back("");
				}
			}
			Result.push_back(Row_buff);
			Row_buff.clear();
		}
		MYSQL_FIELD *field;
		if ( Fields != 0) fieldNames.reserve(Fields);
		while((field = mysql_fetch_field(res)))
		{
			fieldNames.push_back(field->name);
		}
	mysql_free_result(res); //clear the result allocation
	}
}
#else
dbResult::dbResult (SQLHDBC& dbhandle, string& queryString,  long long& affectedRows)
{

	SQLHSTMT odbc_stmt;
	SQLRETURN ret;
	SQLSMALLINT columns;
	SQLLEN rows;
	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbhandle, &odbc_stmt);
	if ( !SQL_SUCCEEDED(ret) )
	{
		//extract_error("SQLExecDirect", dbhandle, SQL_HANDLE_DBC);
                //errNo=-3;
	}
	else
	{
		SQLExecDirect(odbc_stmt, (SQLCHAR *) queryString.c_str(), SQL_NTS);
		SQLNumResultCols (odbc_stmt, &columns);
		Fields = columns;
		SQLRowCount (odbc_stmt, &rows);
		Rows = rows;
		affectedRows = Rows;
		while ( SQL_SUCCEEDED(SQLFetch(odbc_stmt)))
		{
			//now fetch results ant put them inside a dbresult struct.
			Row Row_buff;
			SQLSMALLINT i;
			for (i=1;i<=columns;i++)
			{
				SQLINTEGER indicator;
				char buff[8192];//FIXME This is bad! Should find
						//a better way (Try with string??)
				ret = SQLGetData(odbc_stmt,
					i,
					SQL_C_CHAR,
					buff,
					sizeof(buff),
					&indicator);
				if (SQL_SUCCEEDED(ret))
				{
					if (indicator == SQL_NULL_DATA )
					{
						strcpy (buff,"NULL");
					}
					string strBuff = buff;
					Row_buff.push_back(strBuff);
				}
			}
			Result.push_back(Row_buff);
		}
		SQLFreeHandle(SQL_HANDLE_STMT, odbc_stmt);	
	}
}
#endif

Row dbResult::getRow(size_t row)
{
	if ( row < Rows ) 
	{
		return Result[row];
	}
	else
	{
		vector<string> foo;
		return foo;
	}

}

string dbResult::getItem(size_t row, size_t column)
{
	Row rBuff = this->getRow(row);
        if ( column < rBuff.size() ) 
	{
		return rBuff[column];	
	}
	else
	{
		return "";
	}
}
