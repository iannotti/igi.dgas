// $Id: hlrAdmin.cpp,v 1.1.2.1.4.2 2010/12/13 10:18:36 aguarise Exp $
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
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/hlr-service/base/hlrAdmin.h"
#include "glite/dgas/common/base/libdgas_log.h"

#define TOO_MANY_ROWS 1
#define NO_RECORD 2

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_sql_dbname;

extern ofstream logStream;

hlrAdmin::hlrAdmin( string _acl)
{
	acl = _acl;
}

int hlrAdmin::get()
{
	db hlrDb ( hlr_sql_server, 
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		 );
	if ( hlrDb.errNo == 0 )
	{
		string _acl = acl;
		//connection to the DB Ok, go on
		if (acl == "" ) _acl = "%";
		string queryStr =  "SELECT * FROM hlrAdmin WHERE ";
		queryStr += "acl LIKE '" + _acl + "'";
		 dbResult result = hlrDb.query(queryStr);
		 if ( hlrDb.errNo == 0)
		 {
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
				acl = result.getData()[0][0];
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

int hlrAdmin::get(vector<hlrAdmin>& gv)
{
	db hlrDb ( hlr_sql_server, 
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		 );
	if ( hlrDb.errNo == 0 )
	{
		string _acl = acl;
		//connection to the DB Ok, go on
		if (acl == "" ) _acl = "%";
		string queryStr =  "SELECT * FROM hlrAdmin WHERE ";
		queryStr += "acl LIKE '" + _acl + "'";
		 dbResult result = hlrDb.query(queryStr);
		 if ( hlrDb.errNo == 0)
		 {
			int numRows = result.numRows();
			if ( numRows == 0 )
			{
				return NO_RECORD;
			}
			else
			{
				hlrAdmin a;
				for ( int i = 0; i < numRows; i++ )
				{
					a.acl = result.getItem(i,0);
					gv.push_back(a);
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
		//connection to the DB failed, return error
		return hlrDb.errNo; 
	}
}


int hlrAdmin::put()
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
		queryStr = "REPLACE INTO hlrAdmin VALUES ('";
		queryStr += acl + "')";
		hlrDb.query(queryStr);
		if ( hlrDb.errNo != 0 )
		{
			return hlrDb.errNo;
		}
		return 0;
	}
	else
	{
		return hlrDb.errNo; 
	}
}


int hlrAdmin::del()
{
	db hlrDb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (hlrDb.errNo == 0)
	{
		string queryStr;
		queryStr = "DELETE FROM hlrAdmin WHERE acl='";
		queryStr += acl + "'";
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



bool hlrAdmin::exists()
{
	db hlrDb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (hlrDb.errNo == 0)
	{
		string _acl = acl;
		//connection to the DB Ok, go on
		if (acl == "" ) _acl = "%";
		string queryStr =  "SELECT acl FROM hlrAdmin WHERE ";
		queryStr += "acl LIKE '" + _acl + "'";
		dbResult result = hlrDb.query(queryStr);
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



hlrVoAcl::hlrVoAcl( string _voId, string _acl)
{
        voId = _voId;
        acl = _acl;
}

int hlrVoAcl::put()
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
		queryStr = "REPLACE INTO voAdmin VALUES ('";
		queryStr += voId + "','";
		queryStr += acl + "')";
		hlrDb.query(queryStr);
		if ( hlrDb.errNo != 0 )
		{
			return hlrDb.errNo;
		}
		return 0;
	}
	else
	{
		return hlrDb.errNo; 
	}
}


int hlrVoAcl::get(vector<hlrVoAcl>& v)
{
	string logBuff;
	db hlrDb ( hlr_sql_server, 
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		 );
	if ( hlrDb.errNo == 0 )
	{
		string _acl = acl;
		string _voId = voId;
		//connection to the DB Ok, go on
		if (acl == "" ) _acl = "%";
		if (voId == "" ) _voId = "%";
		string queryStr =  "SELECT * FROM voAdmin WHERE ";
		queryStr += "acl LIKE '" + _acl + "'";
		queryStr += " AND vo_id LIKE '" + _voId + "'";
		logBuff = "hlrVoAdmin::get(),";
		logBuff += "Query:" + queryStr;
		hlr_log(logBuff, &logStream, 8);
		 dbResult result = hlrDb.query(queryStr);
		 if ( hlrDb.errNo == 0)
		 {
			int numRows = result.numRows();
			if ( numRows == 0 )
			{
				logBuff = "hlrVoAdmin::get(),Record not found";
				hlr_log(logBuff, &logStream, 7);
				return NO_RECORD;
			}
			else
			{
				hlrVoAcl a;
				for ( int i = 0; i < numRows; i++ )
				{
					a.voId = result.getItem(i,0);
					a.acl = result.getItem(i,1);
					v.push_back(a);
				}
				logBuff = "hlrVoAdmin::get(),entries:";
				logBuff += int2string(numRows);
				hlr_log(logBuff, &logStream, 8);
				 return 0;
			}
		 }
		 else
		 {
		 	logBuff =  "hlrVoAdmin::get(),Error in query.";
			hlr_log(logBuff, &logStream, 7);
			return hlrDb.errNo;
		 }
	}
	else
	{
		//connection to the DB failed, return error
		logBuff =  "hlrVoAdmin::get(),Error connecting to the DB.";
		hlr_log(logBuff, &logStream, 0);
		return hlrDb.errNo; 
	}
}

int hlrVoAcl::del()
{
	db hlrDb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
		);
	if (hlrDb.errNo == 0)
	{
		string queryStr;
		queryStr = "DELETE FROM voAdmin WHERE acl='";
		queryStr += acl + "'";
		queryStr += " AND vo_id='" + voId + "'";
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

bool hlrVoAcl::exists()
{
	db hlrDb(hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
			);
	string logBuff;
	if (hlrDb.errNo == 0)
	{
		string _acl = acl;
		string _voId = voId;
	
		//connection to the DB Ok, go on
		string queryStr =  "SELECT acl FROM voAdmin WHERE ";
		queryStr += "acl='" + _acl + "'";
		queryStr += " AND vo_id='" + _voId + "'";
		dbResult result = hlrDb.query(queryStr);
		if ( hlrDb.errNo == 0)
		{
			if ( result.numRows() >= 1 )
			{
				hlr_log("hlrVoAcl:Found ACL in DB", &logStream, 8);
				return true;
			}
			else
			{
				logBuff = "Could not find acl:" + acl;
				logBuff += ",for vo:" + voId;
				hlr_log(logBuff, &logStream, 7);
				return false;
			}
		}
		else
		{
		 	logBuff =  "hlrVoAdmin::get(),Error in query.";
			hlr_log(logBuff, &logStream, 7);
			return false;
		}
	}
	else
	{
		logBuff =  "hlrVoAdmin::get(),Error connecting to the DB.";
		hlr_log(logBuff, &logStream, 0);
		return false; 
	}
}


roles::roles( string _dn, string _role)
{
        dn = _dn;
        role = _role;
}

int roles::put()
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
                queryStr = "REPLACE INTO roles VALUES (0,'";
                queryStr += dn + "','";
                queryStr += role + "','";
                queryStr += permission + "','";
                queryStr += queryType + "','";
                queryStr += queryAdd + "')";
                hlrDb.query(queryStr);
                if ( hlrDb.errNo != 0 )
                {
                        return hlrDb.errNo;
                }
                return 0;
        }
        else
        {
                return hlrDb.errNo;
        }
}

int roles::get(vector<roles>& v)
{
        string logBuff;
        db hlrDb ( hlr_sql_server,
                        hlr_sql_user,
                        hlr_sql_password,
                        hlr_sql_dbname
                 );
        if ( hlrDb.errNo == 0 )
        {
                string _dn = dn;
                string _role = role;
                //connection to the DB Ok, go on
                if (dn == "" ) _dn = "%";
                if (role == "" ) _role = "%";
                string queryStr =  "SELECT * FROM roles WHERE ";
                queryStr += "dn LIKE '" + _dn + "'";
                queryStr += " AND role LIKE '" + _role + "'";
                logBuff = "roles::get(),";
                logBuff += "Query:" + queryStr;
                hlr_log(logBuff, &logStream, 8);
                 dbResult result = hlrDb.query(queryStr);
                 if ( hlrDb.errNo == 0)
                 {
                        int numRows = result.numRows();
                        if ( numRows == 0 )
                        {
                                logBuff = "roles::get(),Record not found";
                                hlr_log(logBuff, &logStream, 7);
                                return NO_RECORD;
                        }
                        else
                        {
                                roles a;
                                for ( int i = 0; i < numRows; i++ )
                                {
                                        a.seqNumber = atoi((result.getItem(i,0)).c_str());
                                        a.dn = result.getItem(i,1);
                                        a.role = result.getItem(i,2);
                                        a.permission = result.getItem(i,3);
                                        a.queryType = result.getItem(i,4);
                                        a.queryAdd = result.getItem(i,5);
                                        v.push_back(a);
                                }
                                logBuff = "roles::get(),entries:";
                                logBuff += int2string(numRows);
                                hlr_log(logBuff, &logStream, 8);
                                 return 0;
                        }
                 }
                 else
                 {
                        logBuff =  "roles::get(),Error in query.";
                        hlr_log(logBuff, &logStream, 7);
                        return hlrDb.errNo;
                 }
        }
        else
	{
                //connection to the DB failed, return error
                logBuff =  "roles::get(),Error connecting to the DB.";
                hlr_log(logBuff, &logStream, 0);
                return hlrDb.errNo;
        }
}

int roles::del()
{
        db hlrDb(hlr_sql_server,
                        hlr_sql_user,
                        hlr_sql_password,
                        hlr_sql_dbname
                );
        if (hlrDb.errNo == 0)
        {
                string queryStr;
                queryStr = "DELETE FROM roles WHERE dn='";
                queryStr += dn + "'";
                queryStr += " AND role='" + role + "'";
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

bool roles::exists()
{
        db hlrDb(hlr_sql_server,
                        hlr_sql_user,
                        hlr_sql_password,
                        hlr_sql_dbname
                        );
        string logBuff;
        if (hlrDb.errNo == 0)
        {
                //connection to the DB Ok, go on
                string queryStr =  "SELECT dn,role FROM roles WHERE ";
                queryStr += "dn='" + dn + "'";
                queryStr += " AND role='" + role + "'";
                dbResult result = hlrDb.query(queryStr);
                if ( hlrDb.errNo == 0)
                {
                        if ( result.numRows() == 1 )
                        {
                                hlr_log("roles:Found entry in DB", &logStream, 8);
                                return true;
                        }
                        else
                        {
                                logBuff = "Could not find entry:" + dn;
                                logBuff += ",role:" + role;
                                hlr_log(logBuff, &logStream, 7);
                                return false;
                        }
                }
                else
                {
                        logBuff =  "roles::get(),Error in query.";
                        hlr_log(logBuff, &logStream, 7);
                        return false;
                }
        }
        else
        {
                logBuff =  "roles::get(),Error connecting to the DB.";
                hlr_log(logBuff, &logStream, 0);
                return false;
        }
}


hlrVomsAuthMap::hlrVomsAuthMap( string _voId, string _voRole, string _hlrRole )
{
        voId = _voId;
        voRole = _voRole;
        hlrRole = _hlrRole;
}


string hlrRoleGet(connInfo& c)
{
	string role = "normalUser";
	vector<vomsAC>::const_iterator it = (c.vomsData).begin();
	while ( it != (c.vomsData).end() )
	{
		hlrVomsAuthMap authMap (c.voname,(*it).role);
		if ( authMap.get() == 0 )
		{
			role = authMap.hlrRole;
		}
		it++;
	}
	return role;
}

int hlrVomsAuthMap::put()
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
                queryStr = "REPLACE INTO vomsAuthMap VALUES ('";
                queryStr += voId + "','";
                queryStr += voRole + "','";
                queryStr += hlrRole + "')";
                hlrDb.query(queryStr);
                if ( hlrDb.errNo != 0 )
                {
                        return hlrDb.errNo;
                }
                return 0;
        }
        else
        {
                return hlrDb.errNo;
        }
}

int hlrVomsAuthMap::get(vector<hlrVomsAuthMap>& vv)
{
	string logBuff;
	db hlrDb ( hlr_sql_server,
			hlr_sql_user,
			hlr_sql_password,
			hlr_sql_dbname
			);
	if ( hlrDb.errNo == 0 )
	{
		string _voId = "%";
		string _voRole = "%";
		string _hlrRole = "%";
		if ( voId != "" ) _voId = voId;
		if ( voRole != "" ) _voRole = voRole;
		if ( hlrRole != "" ) _hlrRole = hlrRole;
		string queryStr =  "SELECT * FROM vomsAuthMap WHERE ";
		queryStr += "vo_id LIKE '" + _voId + "'";
		queryStr += " AND voRole LIKE '" + _voRole + "'";
		queryStr += " AND hlrRole LIKE '" + _hlrRole + "'";
		logBuff = "hlrVomsAuthMApe::get(),";
		logBuff += "Query:" + queryStr;
		hlr_log(logBuff, &logStream, 8);
		dbResult result = hlrDb.query(queryStr);
		if ( hlrDb.errNo == 0)
		{
			int numRows = result.numRows();
			if ( numRows == 0 )
			{
				return NO_RECORD;
			}
			else
			{
				hlrVomsAuthMap v;
				for ( int i = 0; i < numRows; i++ )
				{
					v.voId = result.getItem(i,0);
					v.voRole = result.getItem(i,1);
					v.hlrRole = result.getItem(i,2);
					vv.push_back(v);
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

int hlrVomsAuthMap::get()
{
        string logBuff;
        db hlrDb ( hlr_sql_server,
                        hlr_sql_user,
                        hlr_sql_password,
                        hlr_sql_dbname
                 );
        if ( hlrDb.errNo == 0 )
        {
                string _voId = voId;
                string _voRole = voRole;
                string _hlrRole = hlrRole;
                //connection to the DB Ok, go on
                string queryStr =  "SELECT * FROM vomsAuthMap WHERE ";
                queryStr += "vo_id LIKE '" + _voId + "'";
                queryStr += " AND voRole LIKE '" + _voRole + "'";
                logBuff = "hlrVomsAuthMApe::get(),";
                logBuff += "Query:" + queryStr;
                hlr_log(logBuff, &logStream, 8);
                 dbResult result = hlrDb.query(queryStr);
                 if ( hlrDb.errNo == 0)
                 {
                        int numRows = result.numRows();
                        if ( numRows == 0 )
                        {
                                logBuff = "hlrVomsAuthMap::get(),Record not found";
                                hlr_log(logBuff, &logStream, 7);
                                return NO_RECORD;
                        }
                        else
                        {
                                voId = result.getItem(0,0);
                                voRole = result.getItem(0,1);
                                hlrRole = result.getItem(0,2);
                                return 0;
                        }
                 }
                 else
                 {
                        logBuff =  "hlrVomsAuthMap::get(),Error in query.";
                        hlr_log(logBuff, &logStream, 7);
                        return hlrDb.errNo;
                 }
        }
        else
        {
                //connection to the DB failed, return error
                logBuff =  "hlrVomsAuthMap::get(),Error connecting to the DB.";
                hlr_log(logBuff, &logStream, 0);
                return hlrDb.errNo;
        }
}

int hlrVomsAuthMap::del()
{
        db hlrDb(hlr_sql_server,
                        hlr_sql_user,
                        hlr_sql_password,
                        hlr_sql_dbname
                );
        if (hlrDb.errNo == 0)
        {
                string queryStr;
                queryStr = "DELETE FROM vomsAuthMap WHERE vo_id='";
                queryStr += voId + "'";
                queryStr += " AND voRole='" + voRole + "'";
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

bool hlrVomsAuthMap::exists()
{
        db hlrDb(hlr_sql_server,
                        hlr_sql_user,
                        hlr_sql_password,
                        hlr_sql_dbname
                        );
        string logBuff;
        if (hlrDb.errNo == 0)
        {

                //connection to the DB Ok, go on
                string queryStr =  "SELECT hlrRole FROM vomsAuthMap WHERE ";
                queryStr += "vo_id='" + voId + "'";
                queryStr += " AND voRole='" + voRole + "'";
                dbResult result = hlrDb.query(queryStr);
                if ( hlrDb.errNo == 0)
                {
                        if ( result.numRows() >= 1 )
                        {
                                hlr_log("hlrVomsAuthMap:Found ACL in DB", &logStream, 8);
                                return true;
                        }
                        else
                        {
                                logBuff = "Could not find voRole:" + voRole;
                                logBuff += ",for vo:" + voId;
                                hlr_log(logBuff, &logStream, 7);
                                return false;
                        }
                }
                else
                {
                        logBuff =  "hlrVomsAuthMap::get(),Error in query.";
                        hlr_log(logBuff, &logStream, 7);
                        return false;
                }
        }
        else
        {
                logBuff =  "hlrVomsAuthMap::get(),Error connecting to the DB.";
                hlr_log(logBuff, &logStream, 0);
                return false;
        }
}

