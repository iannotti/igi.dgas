// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: getRecordEngine.cpp,v 1.1.2.1.4.2 2011/02/18 08:52:13 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
 /***************************************************************************
 * Code borrowed from:
 *  authors   :
 *  copyright : 
 ***************************************************************************/
//
//    

// ---------------------------------------------------------------------------
//  Includes
// ---------------------------------------------------------------------------
#include "getRecordEngine.h"
#include "serviceCommonUtl.h"

extern ofstream logStream;
extern bool strictAccountCheck;

extern const char * hlr_sql_server;
extern const char * hlr_sql_user;
extern const char * hlr_sql_password;
extern const char * hlr_sql_dbname;



inline int urlSplit(char delim, string url_string , url_type *url_buff)
{
        size_t pos = 0;
        pos = url_string.find_first_of( delim, 0);
        url_buff->hostname=url_string.substr(0,pos);
        url_buff->port=atoi((url_string.substr(pos+1,url_string.size()).c_str()));
				                
        return 0;
}

bool authorize(string& contactString)
{
        roles rolesBuff(contactString, "recordSource" );
        return rolesBuff.exists();
}

int parse_xml (string &doc, baseRecord *r, map<string,string>& fieldsValues)
{
	string logBuff;
	node tagBuff;
	tagBuff = parse(&doc, "tableName");
	if ( tagBuff.status == 0 )
	{
		logBuff = "getRecord engine: found tableName=" + tagBuff.text;
		r->tableName = tagBuff.text;
		hlr_log(logBuff, &logStream, 7);
	}
	tagBuff.release();
	while ( 1 )
	{
		node nodeBuff = parse(&doc, "record");
		if ( nodeBuff.status != 0 )
			break;
		bool goOn = true;
		if ( r->tableName == "voStorageRecords" )
		{
			//the table is voStorageRecords:
			//mysql> describe voStorageRecords;
			//+-----------------+---------------------+
			//| Field           | Type                |
			//+-----------------+---------------------+
			//| uniqueChecksum  | char(32)            |  
			//| timestamp       | int(10) unsigned    |  
			//| siteName        | varchar(255)        |  
			//| vo              | varchar(255)        |  
			//| voDefSubClass   | varchar(255)        |  
			//| storage         | varchar(255)        |  
			//| storageSubClass | varchar(255)        |  
			//| urSourceServer  | varchar(255)        |  
			//| usedBytes       | bigint(20) unsigned |  
			//| freeBytes      | bigint(20) unsigned | 
			//+-----------------+---------------------+
			while ( goOn )
			{
			 	node jobInfoNode;
				jobInfoNode = parse(&nodeBuff.text, "dgas:item");
				if ( jobInfoNode.status == 0 )
				{
					attrType attributes;
					attributes = jobInfoNode.getAttributes();
					string buffer = "";
					buffer =
						parseAttribute ("timestamp", attributes);
					if ( buffer != "")
					{
						logBuff = "ATMengine: found timstamp=" + buffer;
						r->timestamp = buffer;	
						hlr_log(logBuff, &logStream, 7);
					}
					buffer = "";
					buffer =
						parseAttribute ("siteName", attributes);
					if ( buffer != "")
					{
						logBuff = "ATMengine: found siteName=" + buffer;
						r->siteName = buffer;
						hlr_log(logBuff, &logStream, 7);
					}
					buffer = "";
					buffer = 
						parseAttribute ("vo", attributes);
					if ( buffer != "")
					{
						logBuff = "ATMengine: found vo=" + buffer;
						r->vo = buffer;
						hlr_log(logBuff, &logStream, 7);
					}
					buffer = "";
					buffer = 
						parseAttribute ("voDefSubClass", attributes);
					if ( buffer != "")
					{
						logBuff = "ATMengine: found voDefSubClass=" + buffer;
						r->voDefSubClass = buffer;
						hlr_log(logBuff, &logStream, 7);
					}
				
					buffer = "";
					buffer = 
						parseAttribute ("storage", attributes);
					if ( buffer != "")
					{
						logBuff = "ATMengine: found storage=" + buffer;
						r->storage = buffer;
						hlr_log(logBuff, &logStream, 7);
					}
					buffer = "";
					buffer = 
						parseAttribute ("storageDefSubClass", attributes);
					if ( buffer != "")
					{
						logBuff = "ATMengine: found storagedefSubClass=" + buffer;
						r->storageDefSubClass = buffer;
						hlr_log(logBuff, &logStream, 7);
					}
				
					buffer = "";
					buffer = 
						parseAttribute ("usedBytes", attributes);
					if ( buffer != "")
					{
						logBuff = "ATMengine: found usedBytes=" + buffer;
						r->usedBytes = buffer;
						hlr_log(logBuff, &logStream, 7);
					}
					
					buffer = "";
					buffer = 
						parseAttribute ("freeBytes", attributes);
					if ( buffer != "")
					{
						logBuff = "ATMengine: found freeBytes=" + buffer;
						r->freeBytes = buffer;
						hlr_log(logBuff, &logStream, 7);
					}
					jobInfoNode.release();
				}
				else
				{
					goOn = false;
				}
			}
			nodeBuff.release();
		}
		else
		{
			//the table is system defined.
			//uniqueConstraint is that it's name MUST begin with
			//the key: "sysDef"
			if ( (r->tableName).find("sysDef") == string::npos )
			{
				return atoi(ATM_E_AUTH);
			}
			goOn = true;
			while ( goOn )
			{
			 	node jobInfoNode;
				jobInfoNode = parse(&nodeBuff.text, "dgas:item");
				if ( jobInfoNode.status == 0 )
				{
					attrType attributes;

					attributes = jobInfoNode.getAttributes();
					map<string,string>::iterator it = attributes.begin();
					string key= (*it).first;
					string value = (*it).second;
					if ( key != "" )
					{
						logBuff = "ATMengine: found " + key + "=" + value;
						fieldsValues.insert(pair<string,string>(key,value));	
						hlr_log(logBuff, &logStream, 7);
					}
					jobInfoNode.release();
				}
				else
				{
					goOn =false;
				}
			}
			nodeBuff.release();
		}
	}
	return 0;
	
}//parse_xml (string &doc, ATM_job_data *job_data, ATM_usage_info *usage_info)

int compose_xml(baseRecord &usage_info ,string status_msg, string *output)
{
	*output = "<HLR type=\"ATM_answer\">\n";
	*output += "<BODY>\n";
	*output += "<dgas:item tableName=\"" + usage_info.tableName + "\"\\>\n";
	*output += "<dgas:item timestamp=\"" + usage_info.timestamp + "\"\\>\n";
	*output += "<dgas:item vo=\"" + usage_info.vo + "\"\\>\n";
	*output += "<dgas:item uniqueChecksum=\"" + usage_info.uniqueChecksum + "\"\\>\n";
	*output += status_msg;
	*output += "</BODY>\n";
	*output += "</HLR>\n";
	return 0;
}//compose_xml()

//get the xml object from the daemon, parse the information 
int getRecordEngine( string &input, connInfo &connectionInfo, string *output )
{
	hlr_log ("getRecord Engine: Entering.", &logStream,4);
	hlrError e;
	bool success = true;
	int code = 0;
	if ( !authorize(connectionInfo.contactString) )
	{
		success = false;
		code = atoi(ATM_E_AUTH);
	}
	//Get info concerning the job from the incoming request originating 
	//from the sensor
	baseRecord usage_info;
	map<string,string> fieldsValues;
	if ( success )
	{
		if ( parse_xml(input, &usage_info, fieldsValues) != 0 )
		{
			//something went wrong parsing the input DGASML
			 hlr_log ("getRecord Engine: Error parsing the XML, reporting error.", &logStream,1);
			 code = atoi(E_PARSE_ERROR);
			 success = false;
		} 
		else
		{
			if ( (usage_info.tableName).find("sysDef") != string::npos )
			{
				//sysDef table
				hlr_log ("getRecord Engine: Processing sysDef record", &logStream,6);
				db hlrDb ( hlr_sql_server,
					hlr_sql_user,
					hlr_sql_password,
					hlr_sql_dbname);
				if ( hlrDb.errNo != 0 )
				{
					code = atoi(E_NO_DB);
					success = false;
				}
				else
				{
					//getTable Definition.
					string queryStr = "DESCRIBE " + usage_info.tableName;
					dbResult result = hlrDb.query(queryStr);
					if ( hlrDb.errNo != 0 )
					{
						success = false;
						hlr_log("Warning: the requested table does not exists. Create it first!", &logStream, 3);
						code = atoi(ATM_E_TRANS);
					}
					else
					{
						//check that table size is
						//equal to number of fields
						//specified by teh client. 
						//throw an erro if not
						if ( result.numRows() != fieldsValues.size() )
						{
							//there is a mismatch
							success = false;
							code = atoi(E_NO_ATTRIBUTES);
							hlr_log("Wrong number of attributes from the client", &logStream, 3);
						}
						else
						{
							//create a map containing field names and corresponding types
							//as defined in the database.
							map<string,string> tableDef;
							for ( unsigned int i = 0; i< result.numRows(); i++ )
							{
								string field =result.getItem(i,0);
								string type =result.getItem(i,1);
								tableDef.insert(pair<string,string>(field,type));
							}
							//compose INSERT query managing field types.
							string queryStr = "INSERT INTO " + usage_info.tableName + " SET ";
							map<string,string>::iterator it = fieldsValues.begin();
							bool alreadyStarted = false;
							while ( it != fieldsValues.end() )
							{
								if ( (*it).second != "" )
								{ 
									if ( (it != fieldsValues.begin()) && alreadyStarted )
									{
										queryStr += ",";
									}
									queryStr += (*it).first+"=";
									string typeBuff = tableDef[(*it).first];
									if ( typeBuff.find("int") != string::npos )
									{
										queryStr += (*it).second; 
									} 
									if ( typeBuff.find("date") != string::npos )
									{
										//integer type
										queryStr += (*it).second; 
									} 
									if ( typeBuff.find("char") != string::npos )
									{
										//string type
										queryStr += "\"" + (*it).second + "\""; 
									}
									alreadyStarted=true;
								}
								it++;
							}	
							hlr_log ( queryStr,&logStream,8);
							//perform query
							dbResult insert = hlrDb.query(queryStr);
							if ( hlrDb.errNo != 0 )
							{
								success = false;
								code = atoi(ATM_E_TRANS);
							}
						}
					}
				}
			}
			else
			{
				//voStorageRecords table
				hlr_log ("getRecord Engine: Processing record", &logStream,6);
				time_t currTime;
				time (&currTime);
				if ( usage_info.timestamp == "" )
				{
					//if time is not available use current time!
					usage_info.timestamp = int2string(currTime);
				}
				string buff = usage_info.timestamp + usage_info.vo + usage_info.voDefSubClass + usage_info.storage + usage_info.storageDefSubClass + usage_info.usedBytes + usage_info.freeBytes;
				usage_info.uniqueChecksum = md5sum(buff);
				string logBuff ="Inserting into " + usage_info.tableName + ":"; 
				usage_info.uniqueChecksum + "," + 
				usage_info.timestamp + "," + 
				usage_info.vo + "," +
				usage_info.voDefSubClass + "," +
				usage_info.storage+","+
				usage_info.usedBytes+","+
				usage_info.freeBytes;
				hlr_log (logBuff, &logStream,6);
				string queryStr = "INSERT INTO " + usage_info.tableName;
				queryStr += " VALUES (0,'";
				queryStr += usage_info.uniqueChecksum + "',";
				queryStr += usage_info.timestamp + ",'";
				queryStr += usage_info.siteName + "','";
				queryStr += usage_info.vo + "','";
				queryStr += usage_info.voDefSubClass + "','";
				queryStr += usage_info.storage + "','";
				queryStr += usage_info.storageDefSubClass + "','";
				queryStr += connectionInfo.hostName + "',";
				queryStr += usage_info.usedBytes + ",";
				queryStr += usage_info.freeBytes + ")";
				hlr_log(queryStr, &logStream, 8);
				db hlrDb ( hlr_sql_server,
					hlr_sql_user,
					hlr_sql_password,
					hlr_sql_dbname);
				if ( hlrDb.errNo == 0 )
				{
					hlrDb.query(queryStr);
					if ( hlrDb.errNo != 0 )
					{
						success = false;
						code = atoi(ATM_E_TRANS);
					}
				}
				else
				{
					code = atoi(E_NO_DB);
					success = false;
				} 
			}
		}
	}//ifsuccess
	string message;
        if (success)
        {
		message += "<dgas:info status=\"ok\"\\>\n";
        }
	else
	{
		message += "<dgas:info status=\"failed\"\\>\n";
		message += "<errMsg>";
        	message += e.error[int2string(code)];
        	message += "</errMsg>";
	}
	message += "<CODE>\n";
	message += int2string(code);
	message += "\n</CODE>\n";
        if ( compose_xml(usage_info, message, output) != 0 )
        {
	         hlr_log ( "getRecord engine: Error composing the XML answer!",&logStream,3);
        }
	hlr_log ("getRecord Engine: Exiting.", &logStream,4);	
	return code;
} //Engine( string doc, connInfo &connectionInfo, string *output )


