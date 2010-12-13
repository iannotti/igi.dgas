// $Id: hlrResource.cpp,v 1.1.2.1.4.2 2010/12/13 10:18:36 aguarise Exp $
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

#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/hlr-service/base/hlrResource.h"

#define E_GET_ADESC 	1
#define E_GET_ROLLBACK 	3
#define E_DEL_RGF	6
#define E_GET_KEYS 	7
#define E_NOT_AUTH	13
#define E_RID_DUPLICATE	14
#define E_CERT_DUPLICATE	15
#define E_DEL_MANDATORY		16
#define E_PUT_ACCT	18

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_sql_dbname;

hlrResource::hlrResource( string _rid,
		string _email,
		string _descr,
		string _ceId,
		string _acl,
		string _gid
		)
{
	rid = _rid;
	email = _email;
	descr = _descr;
	ceId = _ceId;
	acl = _acl;
	gid = _gid;
}

int hlrResource::get()
{
	db hlrDb ( hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (  hlrDb.errNo == 0 )
	{
		string queryStr = "SELECT * FROM acctdesc WHERE ";
		queryStr += "1 ";
		if ( rid != "" ) queryStr += "AND id LIKE '" + rid + "' ";
		if ( email != "" ) queryStr += "AND email LIKE '" + email + "' ";
		if ( descr != "" ) queryStr += "AND descr LIKE '" + descr + "' ";
		if ( gid != "" ) queryStr += "AND gid LIKE '" + gid + "' ";
		if ( ceId != "" ) queryStr += "AND BINARY ceId LIKE '" + ceId + "'";//certSubject field
									//in the Db is used to contain ceID.
		dbResult result = hlrDb.query(queryStr);
		if ( hlrDb.errNo == 0) 
		{
			int numRows = result.numRows();
			if ( numRows == 1 )
			{
				if ( connectionInfo.contactString != "" )//for local queries,
				{					 //contactString is set by the 
									 //engine
					hlrAdmin a(connectionInfo.contactString);
					if ( !a.exists() )//Authorize hlr administrator
					{
						return E_NOT_AUTH;
					}
				}
				rid = result.getItem(0,0);
				email = result.getItem(0,1);
				descr = result.getItem(0,2);
				ceId = result.getItem(0,3);
				acl = result.getItem(0,4);
				gid = result.getItem(0,5);
				return 0;
			}
			if ( numRows == 0 )
			{
				return E_GET_ADESC;
			}
			if ( numRows > 1 )
			{
				return E_GET_ADESC;
			}
			
		}
		else
		{		
			return hlrDb.errNo;
		}
	} 
	else
	{
		return hlrDb.errNo;
	}
	return 0;
}

int hlrResource::get(vector<hlrResource>& rv)
{
	db hlrDb ( hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (  hlrDb.errNo == 0 )
	{
		string queryStr = "SELECT * FROM acctdesc WHERE ";
		queryStr += "1 ";
		if ( rid != "" ) queryStr += "AND id LIKE '" + rid + "' ";
		if ( email != "" ) queryStr += "AND email LIKE '" + email + "' ";
		if ( descr != "" ) queryStr += "AND descr LIKE '" + descr + "' ";
		if ( gid != "" ) queryStr += "AND gid LIKE '" + gid + "' ";
		if ( ceId != "" ) queryStr += "AND BINARY ceId LIKE '" + ceId + "'";//certSubject field
									//in the Db is used to contain ceID.
		dbResult result = hlrDb.query(queryStr);
		if ( hlrDb.errNo == 0 )
		{
			int numRows = result.numRows();	
			if ( numRows > 0 )
			{
				hlrResource r;
				for (int i = 0;i < numRows; i++ )
				{
					//acctdesc part
					r.rid = result.getItem(i,0);
					r.email = result.getItem(i,1);
					r.descr = result.getItem(i,2);
					r.ceId = result.getItem(i,3);
					r.acl = result.getItem(i,4);
					r.gid  = result.getItem(i,5);
					rv.push_back(r);
					
				}
				return 0;
			}
			else
			{
				return  E_GET_ADESC;
			}
		}
		else
		{
			return hlrDb.errNo;
		}

	}
	else
	{
		return hlrDb.errNo;
	}
}

int hlrResource::put()
{
	db hlrDb ( hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (  hlrDb.errNo == 0 )
	{
		//check for RID duplication.
		string queryStr;
		queryStr = "SELECT id FROM acctdesc WHERE id LIKE '" + rid;
		dbResult result = hlrDb.query(queryStr);
		if ( (hlrDb.errNo == 0) && result.numRows() == 1 )
		{
			//another account with this ris already exists.
			return E_RID_DUPLICATE;
		}
		queryStr = "SELECT id FROM acctdesc WHERE BINARY ceId LIKE '" + ceId;
		result = hlrDb.query(queryStr);
		if ( (hlrDb.errNo == 0) && result.numRows() == 1 )
		{
			//another accoutn with the same ceId is present.
			return E_CERT_DUPLICATE;
		}
		//go on inserting acctdesc
		queryStr = "REPLACE INTO acctdesc VALUES ('";
		queryStr += rid + "','";
		queryStr += email + "','";
		queryStr += descr + "','";
		queryStr += ceId + "','";
		queryStr += acl + "','";
		queryStr += gid + "')";
		hlrDb.query(queryStr);
		if ( hlrDb.errNo != 0 )
		{
			return E_PUT_ACCT;
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

int hlrResource::del()
{
	if ( rid == "" || gid == "" )
        {
                return E_DEL_MANDATORY;
        }
	if ( this->exists() )
	{
		db hlrDb ( hlr_sql_server,
				hlr_sql_user,
				hlr_sql_password,
				hlr_sql_dbname
			);
		if ( hlrDb.errNo == 0 )
		{
			string queryStr;
			queryStr = "DELETE FROM acctdesc WHERE id='";
			queryStr += rid + "' AND gid='";
			queryStr += gid + "'";
			hlrDb.query(queryStr);
			if ( hlrDb.errNo != 0 )
			{
				return E_DEL_RGF;
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
	else
	{
		return E_GET_ROLLBACK;
	}
}

bool hlrResource::exists(string _type, string &_acl)
{
	 db hlrDb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
	);
	if (hlrDb.errNo == 0)
	{
		//connection to the DB Ok, go on
		string queryStr =  "SELECT id FROM acctdesc WHERE ";
		queryStr += "BINARY acl LIKE '%" + _acl + "%'";
		dbResult result = hlrDb.query(queryStr);
		if ( (hlrDb.errNo == 0) && ( result.numRows() > 0 ))
		{
			return true;
		}
	}
	return false;
}

bool hlrResource::exists()
{
	db hlrDb ( hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (  hlrDb.errNo == 0 )
	{
		string _rid = rid;
		string _gid = gid;
		if (rid == "" ) _rid = "%";
		if (gid == "" ) _gid = "%";
		string queryStr =  "SELECT id FROM acctdesc WHERE ";
		queryStr += "id LIKE '" + _rid + "' ";
		if ( email != "" ) queryStr += "AND email LIKE '" + email + "' ";
		if ( descr != "" ) queryStr += "AND descr LIKE '" + descr + "' ";
		if ( gid != "" ) queryStr += "AND gid LIKE '" + _gid + "' ";
		if ( ceId != "" ) queryStr += "AND BINARY ceId LIKE '" + ceId + "'";
		dbResult result = hlrDb.query(queryStr);
		if (  ( hlrDb.errNo == 0 ) && ( result.numRows() > 0) )
		{
			return true;
		}
	}
	return false;
}

int hlrResource::getKeys(vector<string>& keys)
{
	db hlrDb ( hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (  hlrDb.errNo == 0 )
	{
		string _rid = rid;
		string _email = email;
		string _descr = descr;
		string _ceId = ceId;
		if (rid == "" ) _rid = "%";
		if (email == "" ) _email = "%";
		if (descr == "" ) _descr = "%";
		if (ceId == "" ) _ceId = "%";
		string queryStr =  "SELECT id FROM acctdesc WHERE ";
		queryStr += "id LIKE '" + _rid + "' AND ";
		queryStr += "email LIKE '" + _email + "' AND ";
		queryStr += "descr LIKE '" + _descr + "' AND ";
		queryStr += "BINARY ceId LIKE '" + _ceId + "'";
		dbResult result = hlrDb.query(queryStr);
		if ( hlrDb.errNo == 0)
		{
			int numRows = result.numRows();
			if ( numRows == 0 )
			{
				return E_GET_KEYS;
			}
			else
			{
				string k;
				for (int i =0; i < numRows; i++ )
				{
					k = result.getItem(i,0);
					keys.push_back(k);
				}
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
		return hlrDb.errNo;
	}

}

ostream& operator << ( ostream& os, const hlrResource& r )
{
	os << "rid=" << r.rid;
	os << ",email=" << r.email;
	os << ",descr=" << r.descr;
	os << ",ceId=" << r.ceId;
	os << ",gid=" << r.gid;
	return os;
}
