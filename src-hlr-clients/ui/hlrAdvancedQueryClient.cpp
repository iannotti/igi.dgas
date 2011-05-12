// $Id: hlrAdvancedQueryClient.cpp,v 1.1.2.1.4.3 2011/05/12 13:13:24 aguarise Exp $
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

#include <unistd.h>
#include <iostream>
#include <iomanip>
#include <getopt.h>
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/tls/SocketClient.h"
#include "glite/dgas/common/tls/SocketAgent.h"
#include "glite/dgas/common/tls/GSISocketClient.h"
#include "glite/dgas/common/tls/GSISocketAgent.h"

#define OPTION_STRING "H:j:u:r:t:g:v:R:LQ:F:A:DhG:S:O:o:a:"

using namespace std;
using namespace glite::wmsutils::tls::socket_pp;
string hlrLocation = "";

bool needs_help = false;
bool wrongOpt = false;
bool isListOk = false;

string lastTidBuff ="";
string startTidBuff ="";

struct inputData
{
  string userCertBuffer;
  string resourceIDBuffer;
  string timeBuffer;
  string groupIDBuffer;
  string vomsRoleBuffer;
  string voIDBuffer;
  string frequencyBuffer;
  string timeIndexBuff;
  string queryTypeBuffer;
  string jobId;  
  string startTid;  
  string lastTid;  
  string aggregateStringBuff;
  string groupBy;
  string SiteName;
  string urOrigin;
  string orderBy;
  string authoriseAs;
  bool itsHeading;
  bool debug;
  bool listFlag;
};

inputData inputStruct;

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"hlrLocation",1,0,'H'},
		{"userCert",1,0,'u'},
		{"resourceId",1,0,'r'},
		{"time",1,0,'t'},
		{"hlrGroup",1,0,'g'},
		{"voId",1,0,'v'},
		{"vomsRole",1,0,'R'},
		{"list",0,0,'L'},
		{"queryType",1,0,'Q'},
		{"Conf",1,0,'C'},
		{"debug",0,0,'D'},
		{"Frequency",1,0,'F'},
		{"AggregateString",1,0,'A'},
		{"GroupBy",1,0,'G'},
		{"jobId",1,0,'j'},
		{"SiteName",1,0,'S'},
		{"urOrigin",1,0,'O'},
		{"orderBy",1,0,'o'},
		{"authoriseAs",1,0,'a'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'H': hlrLocation = optarg; break;
			case 'u': inputStruct.userCertBuffer = optarg; break;
			case 'r': inputStruct.resourceIDBuffer = optarg; break;
			case 't': inputStruct.timeBuffer = optarg; break;
			case 'g': inputStruct.groupIDBuffer = optarg; break;
			case 'v': inputStruct.voIDBuffer = optarg; break;
			case 'R': inputStruct.vomsRoleBuffer = optarg; break;
			case 'L': inputStruct.listFlag = true; break;
			case 'Q': inputStruct.queryTypeBuffer = optarg; break;
			case 'D': inputStruct.debug = true; break;
			case 'F': inputStruct.frequencyBuffer = optarg; break;
			case 'A': inputStruct.aggregateStringBuff = optarg; break;
			case 'G': inputStruct.groupBy = optarg; break;
			case 'S': inputStruct.SiteName = optarg; break;
			case 'O': inputStruct.urOrigin = optarg; break;
			case 'o': inputStruct.orderBy = optarg; break;
			case 'a': inputStruct.authoriseAs = optarg; break;
			case 'j': inputStruct.jobId = optarg; break;
			case 'h': needs_help =true;; break;		  
			case '?': needs_help =true;wrongOpt=true; break;		  
			default : break;
		}
	return 0;
}

int help (const char *progname)
{
	cerr << "\n " << progname << endl;
        cerr << " Author: Andrea Guarise " << endl;
        cerr << " 13/09/2005 " << endl <<endl;
        cerr << " usage: " << endl;
	if ( inputStruct.queryTypeBuffer == "" )
	{
		cerr << " " << progname << " -Q <queryType> [OPTIONS] " << endl;
        	cerr << "The parameter <queryType> can be one of the following:" << endl;
	        cerr << endl;
	        cerr << setw(22) << left<< "\"resourceAggregate\":"<<"Aggregate usage records for a resource" << endl;
	        cerr << setw(20) << left<< "\"sql\":"<<"Direct SQL query to the HLR database (Only Admins are authorised to this command)" << endl;
	        cerr << setw(20) << left<< "\"sqlCsv\":"<<"Direct SQL query to the HLR database, CSV output (Only Admins are authorised to this command)" << endl;
		cerr << "Use: " << progname << " -Q <queryType> -h to see the list of the available options for that <queryType>" << endl;
	}
	if ( inputStruct.queryTypeBuffer == "sql" || inputStruct.queryTypeBuffer == "sqlCsv" )
	{
        	cerr << " " << progname << " [OPTIONS] " << endl;
        	cerr << setw(30) << left << "-H --hlrLocation <hlrContact>" << "Specifies the contact string of the HLR to query." << endl;
		cerr << setw(30) << left << "         " << "The HLR contact string is of the form: \"host[:port[:host_cert_subject]], if port is not specified the default one (56568) will be used\"" << endl;
        	cerr << setw(30) << left << "-Q --queryType <queryType>" << "Specifies the type of query" << endl;
		cerr << setw(30) << left << "-A --AggregateString"<<"Specify SQL command for sql and sqlCsv queries." << endl;
        	cerr << setw(30) << left << "-v --voId <voID>" << "Used to specify the virtual organization (VOMS)" << endl;
        	cerr << setw(30) << left << "-a --authoriseAs <certificate DN>"<<"ask the server to authorise the request as if coming from the specified DN (neeeds hlrAdmin privileges for the certificate being effectively used.)" << endl;
		cerr << setw(30) << left << "         " << "it is also possible to directly specify \"voAdmin\" (must be used with the -v option)  and \"siteAdmin\" roles"  << endl;
	}
	if ( inputStruct.queryTypeBuffer == "resourceAggregate" )
	{
   	        cerr << " " << progname << " [OPTIONS] " << endl;
        	cerr << setw(30) << left << "-H --hlrLocation <hlrContact>" << "Specifies the contact string of the HLR to query." << endl;
		cerr << setw(30) << left << "         " << "The HLR contact string is of the form: \"host[:port[:host_cert_subject]], if port is not specified the default one (56568) will be used\"" << endl;
        cerr << setw(30) << left << "-Q --queryType <queryType>" << "Specifies the type of query" << endl;
        cerr << setw(30) << left << "-u --userCert <cert_subject>" << "Used to specify the user's certificate subject" << endl;
        cerr << setw(30) << left << "-r --resourceId <ceID>" << "Used to specify the global CE ID for the resource" << endl;
        cerr << setw(30) << left << "-v --voId <voID>" << "Used to specify the virtual organization (VOMS)" << endl;
        cerr << setw(30) << left << "-R --vomsRole <role>" << "Used to specify the voms role (fqan)" << endl;
	cerr << setw(30) << left << "-S --SiteName <site name>" << "Used to specify the site name to look for." << endl;
	cerr << setw(30) << left << "-O --urOrigin <address of urOrigin>" << "Used on 2nd level HLR to query for jobs coming from a given 1st level HLR." << endl;
        cerr << setw(30) << left << "-g --hlrGroup <hlrGroup>" << "Restrict the queries for users to a given HLR group (gid)" << endl;
        cerr << setw(30) << left << "-t --time <interval>" << "Time interval to use in the query, in the form:" << endl;
        cerr << setw(30) << left << "         " << "\"<YYYY-MM-DD HH:MM:SS>_<YYYY-MM-DD HH:MM:SS>\" : all the info in the given period" << endl;
        cerr << setw(30) << left << "         " << "\"_<YYYY-MM-DD HH:MM:SS>\" : all the info available until the given date" << endl;
        cerr << setw(30) << left << "         " << "\"<YYYY-MM-DD HH:MM:SS>_\" : all the info available from the guven date up to now" << endl;
        cerr << setw(30) << left << "         " << "If not specified or '*': no filtering for the time period." << endl;
        cerr << setw(30) << left << "-F --Frequency <freq>"<<"Frequency for the aggregation of query results" << endl;
        cerr << setw(30) << left << "-D --debug"<<"Ask for debug output" << endl;
        cerr << setw(30) << left << "-j --jobId <jobID>"<<"Select a specific global job ID" << endl;
	cerr << setw(30) << left << "-A --AggregateString"<<"Specify info to show in aggregate queries." << endl;
	 cerr << setw(30) << left << "-G --GroupBy"<<"Used in aggregate queries to specify elements used to group results, e.g. -G \"userVo\" or -G \"userVo, month(date)\"" << endl;
        cerr << setw(30) << left << "-o --orderBy <SQL clause>"<<"order by clause" << endl;
        cerr << setw(30) << left << "-a --authoriseAs <certificate DN>"<<"ask the server to authorise the request as if coming from the specified DN (neeeds hlrAdmin privileges for the certificate being effectively used.)" << endl;
        cerr << setw(30) << left <<"-h --help"<<"This help" << endl;
        cerr << endl;
        cerr << endl;
        cerr << "The parameter <freq> can be one of the following:" << endl;
        cerr << endl;
        cerr << setw(22) << left<< "\"day\":"<<"daily aggregate usage records " << endl;
        cerr << setw(22) << left<< "\"week\":"<<"weekly aggregate usage records " << endl;
        cerr << setw(22) << left<< "\"month\":"<<"monthly aggregate usage records " << endl;
        cerr << setw(22) << left<< "\"hour\":"<<"hourly aggregate usage records " << endl;
	
        cerr << endl;
	}
	return 0;	
}

int parseXml(string &xml)
{
	node nodeBuff;
	int i =0;
	while ( nodeBuff.status == 0 )
	{
		node nodeBuff = parse(&xml, "Error");
		string errorMessageBuff=nodeBuff.text;
		nodeBuff.release();
		nodeBuff = parse(&xml, "STATUS");
		string errorMessageStatus=nodeBuff.text;
		nodeBuff.release();
		if ( errorMessageStatus != "0" && errorMessageStatus != "" && !isListOk )
		{
			cerr << "Got Error from server:" <<  errorMessageBuff;
			cerr << ",Exit status:" << errorMessageStatus << endl;
			exit(atoi(errorMessageStatus.c_str()));
		}
		
		nodeBuff = parse(&xml, "queryResult");
		if ( nodeBuff.status != 0 )
			break;
		node lineString;
		while ( lineString.status == 0 )
		{
			lineString = parse(&nodeBuff.text, "lineString");
			if (lineString.status == 0 ) 	
			{
				cout << lineString.text << endl;
				i++;
			}
			lineString.release();
			isListOk = true;
		}
		nodeBuff.release();
		nodeBuff = parse(&xml, "lastTid");
		lastTidBuff=nodeBuff.text;
		nodeBuff.release();
	}
		return i;
}


string struct2xml(inputData &input)
{
	string xml;
	xml  = "<inputData>\n";
	if (input.jobId != "" )
 	{
		xml += tagAdd("jobId",input.jobId);
  	}
	if (startTidBuff != "")
	{
		 xml += tagAdd("startTid",startTidBuff);
	}
	if (input.userCertBuffer != "")
	{
		xml += tagAdd("USERCERTBUFFER",input.userCertBuffer);
	}
	
	if (input.resourceIDBuffer != "")
	{
		xml += tagAdd("RESOURCEIDBUFFER",input.resourceIDBuffer);
	}
	if (input.timeBuffer != "")
	{
		xml += tagAdd("TIMEBUFFER",input.timeBuffer);
	}
	if (input.groupIDBuffer != "")
	{
		xml += tagAdd("GROUPIDBUFFER",input.groupIDBuffer);
	}
	if (input.voIDBuffer != "")
	{
		xml += tagAdd("VOIDBUFFER",input.voIDBuffer);
	}
	if (input.vomsRoleBuffer != "")
	{
		xml += tagAdd("VOMSROLE",input.vomsRoleBuffer);
	}
	if (input.frequencyBuffer != "")
	{
		xml += tagAdd("FREQUENCYBUFFER",input.frequencyBuffer);
	}
	if (input.aggregateStringBuff != "")
	{
		xml += tagAdd("AGGREGATESTRINGBUFFER",input.aggregateStringBuff);
	}
	if (input.groupBy != "")
	{
		xml += tagAdd("groupBy",input.groupBy);
	}
	if (input.SiteName != "")
	{
		xml += tagAdd("siteName",input.SiteName);
	}
	if (input.urOrigin != "")
	{
		xml += tagAdd("urOrigin",input.urOrigin);
	}
	if (input.authoriseAs != "")
	{
		xml += tagAdd("authoriseAs",input.authoriseAs);
	}
	if (input.orderBy != "")
	{
		xml += tagAdd("orderBy",input.orderBy);
	}
	if (input.timeIndexBuff != "")
	{
		xml += tagAdd("TIMEINDEXBUFF",input.timeIndexBuff);
	}
	xml += tagAdd("DEBUG",int2string(input.debug));
	xml += tagAdd("LISTFLAG",int2string(input.listFlag));
	if (input.queryTypeBuffer != "")
	{
		xml += tagAdd("QUERYTYPEBUFFER",input.queryTypeBuffer);
	}
	if (input.queryTypeBuffer == "fieldList")
	{
		xml += tagAdd("itsFieldList","1");
	}
	xml += tagAdd("ITSHEADING",int2string(input.itsHeading));
  xml += "</inputData>\n";
  return xml;
}

int composeXml(inputData &input, string &xml)
{
	xml = "<HLR type=\"advancedQuery\">\n";
	xml += "<BODY>\n";
	xml += struct2xml(input);
	xml += "\n</BODY>\n"; 
	xml += "\n</HLR>"; 
	return 0;	
}

int main (int argc, char **argv)
{
	options ( argc, argv );
	if (needs_help)
	{
		if ( wrongOpt )
		{
			cerr << "Error: Wrong option on the command line." << endl;
		}
		help(argv[0]);
		return 1;
	}
	if ( inputStruct.queryTypeBuffer == "" )
	{
		cerr << "You should specify the query Type." << endl;
		help(argv[0]);
		return 1;
	}
	while ( 1 )
	{	
		string xmlOut;
		composeXml(inputStruct, xmlOut);
		if ( inputStruct.debug )
		{
			cerr << xmlOut << endl;
		}
		string HlrHostname = "";
		int HlrPort = 56568;//default value
		string HlrContact = "";
		vector<string> urlBuff;
		Split(':',hlrLocation, &urlBuff);
		if ( urlBuff.size() > 0 )
        	{
			if ( urlBuff.size() > 0 ) HlrHostname = urlBuff[0];
			if ( urlBuff.size() > 1 ) HlrPort = atoi((urlBuff[1]).c_str());
			if ( urlBuff.size() > 2 ) HlrContact = urlBuff[2];
		}
		else 
		{
			cerr << "Bad hlrLocation :" << hlrLocation << endl;
			return 11;
		}
		int res = 0;
		if ( HlrHostname == "" )
		{
			if ( inputStruct.debug ) cout << "Contacting localhost by default" << endl;
			char buff[256];
			res = gethostname( buff,sizeof(buff));
			HlrHostname = buff;
		}
		GSISocketClient *theClient = new GSISocketClient(HlrHostname, HlrPort);
		theClient -> ServerContact(HlrContact);
		theClient -> SetTimeout(120);
		if ( !(theClient -> Open()) )
		{
			cerr << "ERROR: can't open connection"<< endl;
               		exit (atoi(E_NO_CONNECTION));
		}
		if ( inputStruct.debug )
		{
			cerr << "Sending message..." << endl;
		}
		if ( !(theClient -> Send(xmlOut) ))
	        {
	       	        cerr << "Error sending message" << endl;
	       	}
		string xml_answer;
		if ( !(theClient -> Receive(xml_answer) ))
		{
			cerr << "Error receiving message" << endl;
			theClient -> Close();
		}	
		else
	        {
	       	        theClient -> Close();
	        }
	       	delete theClient;
		if ( inputStruct.debug )
		{
			cerr << "Answer from the server:" <<endl;
			cerr << xml_answer << endl;
		}
		res = parseXml(xml_answer);
		if (res == 0) break;
		if (  inputStruct.queryTypeBuffer != "userJobList" &&
		inputStruct.queryTypeBuffer != "resourceJobList") break;
		startTidBuff = lastTidBuff;
		sleep(1);
	}
	return 0;
}

