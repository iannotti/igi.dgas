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
// --

#ifndef urForwardFactory
#define urForwardFactory

#include <fstream>
#include <string>
#include <vector>
#include "glite/dgas/common/tls/GSISocketClient.h"
#include "glite/dgas/common/tls/GSISocketAgent.h"
#include "glite/dgas/hlr-service/base/hlrGenericQuery.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/base/int2string.h"
#include "../base/dbWaitress.h"

#define E_NO_SERVERSFILE 1
#define E_ERROR_IN_RESET 3

using namespace std;
extern ofstream logStream;
extern volatile sig_atomic_t keep_going;

struct serverParameters
{
	string serverVersion;
	string acceptRecordsStartDate;
	string acceptRecordsEndDate;
	string recordsPerConnection;
	string urSourceServer;
	string urSourceServerDN;
	string remoteRecordId;
	string recordDate;
	string recordInsertDate;
	string lastInsertedUniqueChecksum;
};

struct confParams
{
	string sendRecordsStartDate;
	string sendRecordsEndDate;
	string recordsPerConnection;
	string serversFile;
	int defConnTimeout;
};

struct effectiveParams
{
	string serverVersion;
	int recordsPerConnection;
	string recordsStartDate;
	string recordsEndDate;
	string lastForwardedRecord;
	string recordDate;
	string recordInsertDate;
	string lastInsertedUniqueChecksum;
};

class hlrLocation
{
	//The format is:
	//[{vo1,vo2,...}]host[:port[:remoteHosDN]]
public:
	hlrLocation(string &s)
	{
		host = "";
		p = 56568;
		dn = "";
		vector<string> urlBuff;
		Split(':',s, &urlBuff);
		if ( urlBuff.size() > 0 )
		{
			if ( urlBuff.size() > 0 )
			{
				size_t posS = urlBuff[0].find_first_of("{");
				if ( posS != string::npos )
				{
					size_t posE = urlBuff[0].find_first_of("}");
					if ( posE != string::npos )
					{
						host = urlBuff[0].substr(posE+1);
						string voListString = urlBuff[0].substr(posS+1,posE-posS-1);
						Split(',',voListString,&voList);
					}
				}
				else
				{
					host = urlBuff[0];
				}
			}
			if ( urlBuff.size() > 1 ) p = atoi((urlBuff[1]).c_str());
			if ( urlBuff.size() > 2 ) dn = urlBuff[2];
		}
	};
	string host;
	int p;
	string dn;
	vector<string> voList;//CSV list of pertinetn VOs
};

class urForward {
public:
	urForward( confParams& _conf, database& _dBase)
	{
		conf = _conf;
		dBase = _dBase;
	};

	int run();
	int reset();

private:
	confParams conf;
	effectiveParams usedParameters;
	database dBase;
	string additionalMessageBuffer;

	//methods for run();
	int getServers(vector<string>& serverList);
	int contactServer(hlrLocation& s, string& message, string& answer);
	int getInfo(hlrLocation& s, serverParameters& serverParms);
	string getInfo2XML();
	int XML2serverParams(string &xml, serverParameters& serverParms);

	int sendUsageRecords(hlrLocation& hlr, serverParameters& serverParms);
	int getFirstAndLastFromTid (string& firstTid,string& lastTid );
	int getFirstAndLastFromUniqueChecksum (string& uniqueChecksum, string& firstTid,string& lastTid );
	int sendBurst (hlrLocation& h, long startTid, long endTid, string& lastTid);
	int urBurst2XML(hlrGenericQuery& q, string& answer);
	int getStatus(string& answer);
	string getMaxId();
	//methods for reset().
	string reset2XML();
	int sendReset(hlrLocation& s);
};


#endif
