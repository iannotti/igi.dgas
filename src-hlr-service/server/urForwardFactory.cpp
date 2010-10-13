#include "urForwardFactory.h"
#include <setjmp.h> 

extern volatile sig_atomic_t keep_going;
jmp_buf jump_on_alarm;
string additionalMessageBuffer;


using namespace std;
using namespace glite::wmsutils::tls::socket_pp;

void catch_alarm( int sig );

int urForward::run()
{
	signal ( SIGALRM, catch_alarm );
	string logBuff;
	//read serversFile.
	vector<string> serverList;
	if ( getServers(serverList) != 0 )
	{
		logBuff = "Error reading server list from:" + conf.serversFile;
		hlr_log(logBuff,&logStream,3);
		return E_NO_SERVERSFILE;
	}
	vector<string>::iterator it = serverList.begin();
	while ( keep_going && it != serverList.end() )
	{
		logBuff = "run(),Contacting server:" + *it;
		hlr_log(logBuff,&logStream,4);
		//for each entry in serverFile :
		hlrLocation hlr(*it);
		serverParameters hlrParams;
		//get working parameters recorded in 2nd level HLR.
		if ( getInfo(hlr, hlrParams) != 0 )
		{
			logBuff = "Error retrieving info from 2LHLR:" + *it;
			hlr_log(logBuff,&logStream,3);
		}
		else
		{
			logBuff = "Server answer:";
			logBuff += *it;
			hlr_log(logBuff,&logStream,4);
			//Now send usage records to server.
			int res = 0;	
			res = sendUsageRecords(hlr, hlrParams); 
			if ( res != 0 )
			{
				if ( res == -1 )
				{
					logBuff = "Sending reset request to:";
					logBuff += *it;
					hlr_log(logBuff,&logStream,3);
					sendReset(hlr);
				}
				else
				{
					logBuff = "Error sending UR to:";
					logBuff += *it;
					hlr_log(logBuff,&logStream,3);
				}
					
			}
		}
		it++;
	}
	additionalMessageBuffer = "";
	return 0;
}

string urForward::getMaxId()
{
	string logBuff;
        string queryString = "select max(id) FROM jobTransSummary";
        logBuff = "Performing query:" + queryString;
        hlr_log(logBuff,&logStream,6);
        hlrGenericQuery currentMax(queryString); 
        int res = currentMax.query();
        if ( res != 0 )
        {               
                logBuff = "Error in query:"+ queryString; 
                hlr_log(logBuff,&logStream,3);
                return "";
        }
	else
	{
		if ( ((currentMax.queryResult).front())[0] != "" )
		{
			return ((currentMax.queryResult).front())[0];
		}	
	}	
	return "";
}

int urForward::sendUsageRecords(hlrLocation &hlr, serverParameters& serverParms)
{
	string logBuff = "Entering sendUsegeRecords()";
	hlr_log(logBuff,&logStream,5);
	string recordsStartDate;
	string recordsEndDate;
	string lastForwardedRecord;
	ep.serverVersion = serverParms.serverVersion;
	//Number of records sent per connection will be the minimum
	//between conf.recordsPerConnection and 
	//serverParms.recordsPerConnection
	int a = atoi((conf.recordsPerConnection).c_str());
	int b = atoi((serverParms.recordsPerConnection).c_str());
	ep.recordsPerConnection = ( a <= b ) ? a : b; 
		
	ep.recordsStartDate = serverParms.acceptRecordsStartDate;
	ep.recordsEndDate = serverParms.acceptRecordsEndDate;
	ep.recordDate = serverParms.recordDate;
	
	if ( serverParms.remoteRecordId != "" )
	{
		ep.lastForwardedRecord = serverParms.remoteRecordId;
	}
	else
	{
		ep.lastForwardedRecord = "0";
	}

	string firstTid, lastTid;
	int res = 0;	
	res = getFirstAndLast ( firstTid, lastTid ); 
	if ( res != 0 )
	{
		logBuff = "Error retrieving First and Last records id to send.";
		hlr_log(logBuff,&logStream,3);
		if ( res == -1 )
		{
			//reset
			return -1;
		}
		return 1;
	}
	//now send records from first to last.
	res = 0;
	long startTid = atoi(firstTid.c_str());
	long endTid = startTid + ep.recordsPerConnection;
	while ( keep_going && res == 0 )
	{
		//send burst from startTid -> startTid + recordsPerConn
		//checking if lastTid has been rached.
		logBuff = "sendBurst(" + int2string(startTid) + ",";
		logBuff += int2string(endTid) + "," + lastTid + ")";
		hlr_log(logBuff, &logStream, 8);
		res = sendBurst(hlr, startTid, endTid, lastTid);
		startTid= endTid;
		endTid = startTid + ep.recordsPerConnection;
		
	}
	if ( res == 1 )
	{
		//everything is ok, lastTid reached.
		return 0;
	}
	else
	{
		//some problems around.
		return 2;
	}
}

int urForward::sendBurst(hlrLocation& h, long startTid, long endTid, string& lastTid)
{
	//exit == 0 : ok, go on, queries remaining.
	//exit == 1 : ok, stop. Last item reached.
	//exit >=2  : not ok, some errors!
	string logBuff;
	string queryString = "SELECT * FROM jobTransSummary WHERE id >=";
	queryString += int2string(startTid);
	queryString += " AND id < " + int2string(endTid);
	if ( (h.voList).size() != 0 )
	{
		string voQueryBuff = " AND ( ";
		vector<string>::iterator vo = (h.voList).begin();
		while ( vo != (h.voList).end() )
		{
			logBuff = "Forwarding userVo:" + *vo;
			hlr_log(logBuff,&logStream,7);
			voQueryBuff += "userVo ='" + *vo + "'";
			vo++;
			if (vo != (h.voList).end())
			{
				voQueryBuff += " OR ";
			}
		}
		voQueryBuff += " ) ";
		queryString += voQueryBuff;
	}
        queryString += " AND date >='";
        queryString += ep.recordsStartDate + "' AND date <='";
        queryString += ep.recordsEndDate + "'";
	queryString += " AND id <= " + lastTid;
	logBuff = "Performing query:" + queryString;
	hlr_log(logBuff,&logStream,6);
	hlrGenericQuery select(queryString);
	int res = select.query();
	logBuff = "Query res=" + int2string(res);
	hlr_log(logBuff,&logStream,8);
	if ( (res != 0 ) )
	{
		if ( endTid < atoi(lastTid.c_str()) )
		{
			//more to go
			res = 0;
		}
		else
		{
			//no more to go
			logBuff = "Last item reached for this round.";
			hlr_log(logBuff,&logStream,3);
			return 1;
		}
	}
	if ( (res != 0) && (res != 1))
	{
		logBuff = "Got error in query:";
		logBuff += int2string(res);
		hlr_log(logBuff,&logStream,3);
		return res;
	}
	else
	{
		string message;
		if ( urBurst2XML(select, message) != 0 )
		{
			logBuff = "Got error composing ur XML!";
			hlr_log(logBuff,&logStream,3);
			return 2;
		}
		if ( message != "" )
		{
			//send message if it is not empty
			string answer;
			if ( contactServer(h, message, answer ) != 0 )
			{
				logBuff = "Got error sending XML!";
	                        hlr_log(logBuff,&logStream,3);
        	                return 3;
			}
			else
			{
				res = getStatus(answer);//check for server reporting error!
				if ( res != 0 )
				{
					logBuff = "Got STATUS=" + int2string(res);
		                        hlr_log(logBuff,&logStream,3);
					return 4;
				}	
			}
		}
	}
	return 0;
}

int urForward::getStatus(string& answer)
{
	string logBuff;
	node statusBuff = parse (&answer, "STATUS");
	if ( statusBuff.status == 0 )
	{
		if ( statusBuff.text != "" )
		{
			logBuff = "STATUS ==" +  statusBuff.text;
			hlr_log(logBuff,&logStream,4);
			return atoi((statusBuff.text).c_str()); 
		}
		
	}
	else
	{
		logBuff = "Error parsing STATUS!";
		hlr_log(logBuff,&logStream,3);
		return -1;
	}
}

int urForward::urBurst2XML(hlrGenericQuery& q, string& message)
{
	if ( (q.queryResult).size() != 0 )
	{
		string jobTransSummaryBuff = "";
		string recordBuff = "";
		vector<resultRow>::iterator it = (q.queryResult).begin();
		while ( it != (q.queryResult).end() )
		{
			//build record tag.
			recordBuff = tagAdd( "dgJobId", (*it)[0] );
			recordBuff += tagAdd( "date", (*it)[1] );
			recordBuff += tagAdd( "userFqan", (*it)[4] );
			recordBuff += tagAdd( "userVo", (*it)[5] );
			//recordBuff += tagAdd( "remoteHlr", (*it)[6] );
			recordBuff += tagAdd( "cpuTime", (*it)[6] );
			recordBuff += tagAdd( "wallTime", (*it)[7] );
			recordBuff += tagAdd( "pmem", (*it)[8] );
			recordBuff += tagAdd( "vmem", (*it)[9] );
			recordBuff += tagAdd( "amount", (*it)[10] );
			recordBuff += tagAdd( "start", (*it)[11] );
			recordBuff += tagAdd( "end", (*it)[12] );
			//backward compatibility: new forward old concentrator
			if ( ep.serverVersion == "3.4.0" )
			{
				recordBuff += tagAdd( "gridResource", (*it)[2] );
				recordBuff += tagAdd( "gridUser", (*it)[3] );
				recordBuff += tagAdd( "iBench", (*it)[13] );
				recordBuff += tagAdd( "iBenchType", (*it)[14] );
				recordBuff += tagAdd( "fBench", (*it)[15] );
				recordBuff += tagAdd( "fBenchType", (*it)[16] );
			}
			else
			{
				recordBuff += tagAdd( "thisGridId", (*it)[2] );
				recordBuff += tagAdd( "remoteGridId", (*it)[3] );
				recordBuff += tagAdd( "transType", "resource" );
				recordBuff += tagAdd( "si2k", (*it)[13] );
				recordBuff += tagAdd( "sf2k", (*it)[15] );
			}
			recordBuff += tagAdd( "acl", (*it)[17] );
			recordBuff += tagAdd( "id", (*it)[18] );
			recordBuff += tagAdd( "lrmsId", (*it)[19] );
			recordBuff += tagAdd( "localUserId", (*it)[20] );
			recordBuff += tagAdd( "hlrGroup", (*it)[21] );
			recordBuff += tagAdd( "localGroup", (*it)[22] );
			recordBuff += tagAdd( "endDate", (*it)[23] );
			recordBuff += tagAdd( "siteName", (*it)[24] );
			recordBuff += tagAdd( "urSourceServer", (*it)[25] );
			recordBuff += tagAdd( "hlrTid", (*it)[26] );
			recordBuff += tagAdd( "accountingProcedure", (*it)[27] );
			recordBuff += tagAdd( "voOrigin", (*it)[28] );
			recordBuff += tagAdd( "glueCEInfoTotalCPUs", (*it)[29] );
			recordBuff += tagAdd( "executingNodes", (*it)[30] );
			recordBuff += tagAdd( "uniqueChecksum", (*it)[31] );
			//add record to jobTransSummary tag.
			jobTransSummaryBuff += tagAdd( "record", recordBuff );
			it++;
		}
		message = "<HLR type=\"urConcentrator\">\n";
		message += tagAdd( "requestType", "insertRecords" );
		message += tagAdd( "jobTransSummary", jobTransSummaryBuff );
		message += tagAdd( "additionalInfo", additionalMessageBuffer );
		message += "</HLR>";
	}
	return 0;
}



int urForward::getFirstAndLast (string& firstTid,string& lastTid )
{
	//performs a query to the DB to find the first id and 
	//the last id to send according to the effective paremeters
	//requested.
	string logBuff;
	string queryString = "select max(id) FROM jobTransSummary";
        logBuff = "Performing query:" + queryString;
        hlr_log(logBuff,&logStream,6);
	bool needReset = false;
        hlrGenericQuery currentMax(queryString);
        int res = currentMax.query();
        if ( res != 0 )
	{
                logBuff = "Error in query!";
                hlr_log(logBuff,&logStream,3);
                return 1;
	}
	else
	{
		if ( ((currentMax.queryResult).front())[0] != "" )
		{
			unsigned int maxLocal = atoi((((currentMax.queryResult).front())[0]).c_str());
			unsigned int maxRemote = atoi((ep.lastForwardedRecord).c_str());
			logBuff = "this server max(id)=" + int2string(maxLocal);
			logBuff += ",remote server max(id)=" + int2string(maxRemote);
			//check if max(id) in this server is lower than last inserted id in remote
			//2LHLR. If true it means that table was cleaned upconslidated or 
			//whatsoever.. So use this Server max(id) in lastForwardedRecord.
			needReset = ( maxLocal < maxRemote ) ? true : false;
			unsigned int buffer = ( maxLocal < maxRemote ) ? maxLocal : maxRemote;
			ep.lastForwardedRecord = int2string(buffer);
			logBuff += ";using max(id)=" + ep.lastForwardedRecord;
			logBuff += ";needReset==" + needReset;
			additionalMessageBuffer += logBuff;
			hlr_log(logBuff,&logStream,7);
		}
	}
	if ( needReset )
	{
		logBuff = "Reset Needed.";
		hlr_log(logBuff,&logStream,6);
		//current max(id) < server max(id). Need table reset on 2LHLR
		return -1; 
	}
	else
	{
        	queryString = "SELECT min(id),max(id) FROM jobTransSummary WHERE date >='";
	        queryString += ep.recordsStartDate + "' AND date <='";
	        queryString += ep.recordsEndDate + "' AND id >";
	        queryString += ep.lastForwardedRecord;
	}
        logBuff = "Performing query:" + queryString;
        hlr_log(logBuff,&logStream,6);
        hlrGenericQuery query(queryString);
        res = query.query();
        if ( res != 0 )
        {
                logBuff = "Error in query!";
                hlr_log(logBuff,&logStream,3);
                return 1;
        }
        else
        {
                firstTid = ((query.queryResult).front())[0];
                lastTid = ((query.queryResult).front())[1];
                logBuff = "fisrt:" + firstTid + ",last:" +lastTid;
                hlr_log(logBuff,&logStream,7);
        }
        return 0;
}

int urForward::contactServer(hlrLocation& s, string& message, string& answer)
{
	//on alarm jump here.
	//This sucks! remove as soon as auth_timeout is surely
	//implemented on all 2LHLRs
	if (setjmp (jump_on_alarm) != 0 )
	{
		hlr_log("Could not set longjump!",&logStream,1);
	}
	string logBuff = "Entering contactServer()";
	hlr_log(logBuff,&logStream,5);
	logBuff = s.host + ":" + int2string(s.p) + ":" + s.dn;
	hlr_log(logBuff,&logStream,3);
	int res = 0;
	//alarm ON here
	alarm(30*conf.defConnTimeout);
	GSISocketClient *theClient;
	theClient = new GSISocketClient(s.host, 
				s.p);
	theClient -> ServerContact(s.dn);
	theClient-> SetTimeout(conf.defConnTimeout);
	theClient-> set_auth_timeout(2*conf.defConnTimeout);
	logBuff = "open()";
	hlr_log(logBuff,&logStream,8);
	if ( !(theClient -> Open()) )
	{
		res = atoi(E_NO_CONNECTION);
	}
	else
	{
		logBuff = "send()";
		hlr_log(logBuff,&logStream,8);
		if ( !(theClient->Send(message)))
		{
			res = atoi(E_SEND_MESSAGE);
		}
		logBuff = "receive()";
		hlr_log(logBuff,&logStream,8);
		if ( !(theClient->Receive(answer)) )
		{
			res = atoi(E_RECEIVE_MESSAGE);
		}
		logBuff = "close()";
		hlr_log(logBuff,&logStream,8);
		theClient->Close();
	}
	if ( theClient ) delete theClient;
	//alarm OFF here
	alarm(0);
	return res;
}

int urForward::getInfo(hlrLocation &s, serverParameters& serverParms)
{
	string logBuff = "Entering getInfo()";
	hlr_log(logBuff,&logStream,3);
	string message = getInfo2XML();
	string answer;
	if ( message != "" )
	{
		if ( contactServer(s, message, answer) == 0 )
		{
			if ( XML2serverParams(answer, serverParms) == 0 )
			{
				logBuff = "getInfo():got server parameters:";
				hlr_log(logBuff,&logStream,5);
				logBuff = "serverVersion:" + serverParms.serverVersion;
				hlr_log(logBuff,&logStream,5);
				logBuff = "acceptRecordsStartDate:" + serverParms.acceptRecordsStartDate;
				hlr_log(logBuff,&logStream,6);
				logBuff = "acceptRecordsEndDate:" + serverParms.acceptRecordsEndDate;
				hlr_log(logBuff,&logStream,6);
				logBuff = "recordsPerConnection:" + serverParms.recordsPerConnection;
				hlr_log(logBuff,&logStream,6);
				logBuff = "urSourceServer:" + serverParms.urSourceServer;
				hlr_log(logBuff,&logStream,5);
				logBuff = "remoteRecordId:" + serverParms.remoteRecordId;
				hlr_log(logBuff,&logStream,5);
				logBuff = "recordDate:" + serverParms.recordDate;
				hlr_log(logBuff,&logStream,5);
				logBuff = "recordInsertDate:" + serverParms.recordInsertDate;
				hlr_log(logBuff,&logStream,5);

				return 0;
			}
			else
			{
				//error parsing answer
				logBuff = "getInfo():error parsing answer.";
				hlr_log(logBuff,&logStream,3);
				return 1;
			}
			
		}
		else
		{
			//error contacting server
			logBuff = "getInfo():error contacting server.";
			hlr_log(logBuff,&logStream,3);
			return 2;
		}
	}
	else
	{
		//error composing message
		logBuff = "getInfo():error composing message.";
		hlr_log(logBuff,&logStream,3);
		return 3;
	}
	
}

string urForward::getInfo2XML()
{
	string buff;
	string maxIdBuff = getMaxId();
	buff = "<HLR type=\"urConcentrator\">\n";
	buff +="<requestType>infoRequest</requestType>\n";
	buff +="<producerMaxId>"+maxIdBuff+"</producerMaxId>\n";
	buff +="</HLR>";
	return buff;
}

int urForward::XML2serverParams(string& xml, serverParameters& serverParms)
{
	string logBuff;
	node nodeBuff;
	nodeBuff = parse (&xml, "BODY");
	if ( nodeBuff.status == 0 )
	{
		node tagBuff;
		node statusBuff = parse (&nodeBuff.text, "STATUS");
		if ( statusBuff.status == 0 )
		{
			if ( statusBuff.text != "0" )
			{
				logBuff = "STATUS ==" +  statusBuff.text;
				hlr_log(logBuff,&logStream,3);
				return 2;
			}
			else
			{
				//STATUS defined and == 0 go on.
				tagBuff = parse (&nodeBuff.text , "serverVersion");
				if ( tagBuff.status == 0 )
				{
					serverParms.serverVersion = tagBuff.text;
				}	
				tagBuff = parse (&nodeBuff.text , "acceptRecordsStartDate");
				if ( tagBuff.status == 0 )
				{
					serverParms.acceptRecordsStartDate = tagBuff.text;
				}	
				tagBuff = parse (&nodeBuff.text, "acceptRecordsEndDate");
				if ( tagBuff.status == 0 )
				{
					serverParms.acceptRecordsEndDate = tagBuff.text;
				}
				tagBuff = parse (&nodeBuff.text, "recordsPerConnection");
				if ( tagBuff.status == 0 )
				{
					serverParms.recordsPerConnection = tagBuff.text;
				}
				tagBuff = parse (&nodeBuff.text, "urSourceServer");
				if ( tagBuff.status == 0 )
				{
					serverParms.urSourceServer = tagBuff.text;
				}
				tagBuff = parse (&nodeBuff.text, "urSourceServerDN");
				if ( tagBuff.status == 0 )
				{
					serverParms.urSourceServerDN = tagBuff.text;
				}
				tagBuff = parse (&nodeBuff.text, "remoteRecordId");
				if ( tagBuff.status == 0 )
				{
					serverParms.remoteRecordId = tagBuff.text;
				}
				tagBuff = parse (&nodeBuff.text, "recordDate");
				if ( tagBuff.status == 0 )
				{
					serverParms.recordDate = tagBuff.text;
				}
				tagBuff = parse (&nodeBuff.text, "recordInsertDate");
				if ( tagBuff.status == 0 )
				{
					serverParms.recordInsertDate = tagBuff.text;
				}
			}
		}
		else
		{
			logBuff = "Error retrieving STATUS tag!";
			hlr_log(logBuff,&logStream,3);
			return 1;
		}
	}
	else
	{
		logBuff = "Error retrieving BODY tag, probably not an level2 HLR!";
		hlr_log(logBuff,&logStream,3);
		return 1;
	}
	nodeBuff.release();
	return 0;
}

int urForward::getServers(vector<string>& serverList )
{
	string logBuff;
	ifstream f((conf.serversFile).c_str(), ios_base::in );
	if ( !f )
	{
		logBuff = "Could not open servers file:"; 
		logBuff += conf.serversFile;
		hlr_log(logBuff,&logStream,3);	
		return 1;
	}
	string textLine;
	int pos =0 ;
	string buff;
	while ( getline (f, textLine, '\n'))
	{
		pos = textLine.find_first_not_of(" \n");
		buff = textLine.substr(pos, textLine.size()-pos);
		if ( buff[0] == '#' )
		{
			continue;
		}
		pos = buff.find_last_not_of(" \n");
		buff = buff.substr(0, pos+1);
		serverList.push_back(buff);
	}	
	return 0;
}

int urForward::sendReset(hlrLocation& s)
{
	string logBuff = "Entering sendReset()";
	hlr_log(logBuff,&logStream,6);
	string message = reset2XML();
	string answer;
	if ( message != "" )
	{
		if ( contactServer(s, message, answer) == 0 )
		{
			node statusBuff = parse (&answer, "STATUS");
			if ( statusBuff.status == 0 )
			{
				if ( statusBuff.text != "0" )
				{
					logBuff = "STATUS ==" +  statusBuff.text;
					hlr_log(logBuff,&logStream,3);
					return 2;
				}
			}
			else
			{
				hlr_log("Malformed answer.",&logStream,2);
				return 2;
			}
			
				
		}
		else
		{
			//error parsing answer
                        logBuff = "getInfo():error contacting server.";
                        hlr_log(logBuff,&logStream,3);
                        return 1;
		}
	}
	else
	{
		//error composing message
                logBuff = "getInfo():error composing message.";
                hlr_log(logBuff,&logStream,3);
                return 3;
	}
	return 0;
}

string urForward::reset2XML()
{
	string buff;
	buff = "<HLR type=\"urConcentrator\">\n";
	buff +="<requestType>resetRecords</requestType>\n";
	buff +="<additionalInfo>"+additionalMessageBuffer+"</additionalInfo>\n";
	buff +="</HLR>";
	return buff;
}

int urForward::reset()
{
	string logBuff;
	bool thereAreErrors = false;
	//read serversFile.
	vector<string> serverList;
	if ( getServers(serverList) != 0 )
	{
		logBuff = "Error reading server list from:" + conf.serversFile;
		hlr_log(logBuff,&logStream,3);
		return E_NO_SERVERSFILE;
	}
	vector<string>::iterator it = serverList.begin();
	while ( keep_going && it != serverList.end() )
	{
		logBuff = "run(),Contacting server:" + *it;
		hlr_log(logBuff,&logStream,5);
		//for each entry in serverFile :
		hlrLocation hlr(*it);
		int res = sendReset(hlr);
		if ( res != 0 )
		{
			logBuff = "Error sending reset request to 2ndLevel HLR:" + *it;
			hlr_log(logBuff,&logStream,3);
			thereAreErrors = true;
		}
		it++;
	}
	if ( thereAreErrors )
	{
		logBuff = "There where errors sending reset requests to one or more of the 2L HLR servers, see above messages for details!";
		hlr_log(logBuff,&logStream,2);
		return E_ERROR_IN_RESET;
	}
	return 0;
}

void catch_alarm ( int sig )
{
	longjmp(jump_on_alarm, 1 );
}
