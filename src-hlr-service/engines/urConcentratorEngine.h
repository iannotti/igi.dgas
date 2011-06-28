// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: urConcentratorEngine.h,v 1.1.2.1.4.8 2011/06/28 15:28:57 aguarise Exp $
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
#ifndef urConcentratorEngine_h
#define urConcentratorEngine_h

#include <string>
#include <vector>
#include <iterator>
#include <iostream>
#include <sstream>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/hlr-service/base/hlrGenericQuery.h"
#include "glite/dgas/hlr-service/base/hlrAdmin.h"
#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "serviceCommonUtl.h"
#include "glite/dgas/hlr-service/engines/engineCmnUtl.h"
#include "dbWaitress.h"

using namespace std;

extern ofstream logStream;
extern string acceptRecordsStartDate;
extern string acceptRecordsEndDate;
extern string recordsPerConnection;
extern int recordsPerBulkInsert;
extern bool useBulkInsert;
extern bool checkVo;
extern bool useMergeTables;

#define E_URCONCENTRATOR_NOT_IMPLEMENTED 1
#define E_URCONCENTRATOR_WRONG_REQUEST 2
#define E_URCONCENTRATOR_INDEX_DB_ERROR 3
#define E_URCONCENTRATOR_INSERT_RECORDS 4
#define E_URCONCENTRATOR_AUTH 4
#define E_URCONCENTRATOR_DELETE_RECORDS 5

struct jobTransSummary {
	string dgJobId;
	string date;
	string transType;
	string thisGridId;
	string remoteGridId;
	string userFqan;
	string userVo;
	string remoteHlr;
	string cpuTime;//int(11) in the database
	string wallTime;//int(11) in the database
	string pmem;//int(11) in the database
	string vmem;//int(11) in the database
	string amount;//int(11) in the database
	string start;//int(11) in the database
	string end;//int(11) in the database
	string iBench;
	string iBenchType;
	string fBench;
	string fBenchType;
	string acl;
	string id;//int(11) in the database
	string lrmsId;
	string localUserId;
	string hlrGroup;
	string localGroup;
	string endDate;
	string siteName;
	string urSourceServer;
	string hlrTid;
	string accountingProcedure;
	string voOrigin;
	string glueCEInfoTotalCPUs;
	string executingNodes;
	string numNodes;
	string uniqueChecksum;
};

struct urConcentratorIndex
{
	string urSourceServer;
	string urSourceServerDN;
	string remoteRecordId;
	string recordDate;
	string recordInsertDate;
	string uniqueChecksum;
};

class urConcentrator {
public:
	connInfo *c;
	string *input;
	string *output;
	urConcentrator(
			string *_input,
			connInfo *_c,
			string *_output
	)
	{
		input = _input;
		c = _c;
		output = _output;
	};
	int run();
private:
	string lastInsertedId;
	string lastInsertedRecordDate;
	string lastInsertedUniqueChecksum;
	vector<string> insertValuesBuffer;//TODO buffer to store values for bulk insert
	int insertedRecords; //counter for inserted records in bulk iteration;
	int xmlParser(	string& requestType,
			vector<jobTransSummary>& r);

	int infoRequestSubEngine();
	int insertRequestSubEngine(vector<jobTransSummary>& r);
	int resetRequestSubEngine();

	int infoRequestComposeXml(urConcentratorIndex& indexEntry);
	int insertRequestComposeXml();
	int resetRequestComposeXml();

	bool authorize();
	int insertRecord(db& hlrDb, jobTransSummary& r);
	int insertRecords(vector<jobTransSummary>& r);
	int bulkInsertRecord(db& hlrDb, jobTransSummary& r);
	int bulkInsertRecords(vector<jobTransSummary>& r);
	int updateIndex(urConcentratorIndex& indexEntry);
	int getIndex(urConcentratorIndex& indexEntry);
	int removeServerRecords();

	int errorComposeXml(int errorCode);
	int checkExistsOutOfBand(jobTransSummary& r, string& recordId);
	int removeRecord(string &recordId);
	bool isDuplicateEntry(string& dgJobId, string& hostName, string& transType );

};

int urConcentratorEngine( string &input, connInfo &connectionInfo, string *output );

#endif


