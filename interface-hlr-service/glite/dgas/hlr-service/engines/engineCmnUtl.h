// DGAS (DataGrid Accounting System) 
// Client APIs.
// 
// $Id: engineCmnUtl.h,v 1.1.2.1 2010/10/22 12:13:51 aguarise Exp $
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
#ifndef ENGINE_CMNUTL_H
#define ENGINE_CMNUTL_H

#include <string.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <sstream>
#include <fstream>

#include "glite/dgas/common/base/dgasVersion.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/comm_struct.h"

#include "glite/dgas/hlr-service/base/hlrTransaction.h"

using namespace std;
//parse XML and return value of STATUS tag
struct cmnLogRecords {
        int wallTime;
        int cpuTime;
        int mem;
        int vMem;
        int cePriceTime;
        string userVo;
        string localUserId;
        string userFqan;
	string ceId;
	string priceAuthority;
	string accountingProcedure;
	string forceResourceHlrOnly;
	string economicAccounting;
	string lrmsId;//added for duplicate check.
	string jobName;//added for duplicate check.
	string start;//added for duplicate check.
	string end;//added for duplicate check.
};

struct ATM_job_record
{
	string 	dgJobId;
	int	time;
	string	res_acct_PA_id;
	string	res_acct_bank_id;
	string	usr_acct_bank_id;
	string	user_CertSubject;
	string	res_grid_id;
	string  economicAccountingFlag;
	string  localUserId;
	string  userVo;
	string  urCreation;
	string  lrmsId;
	string  lrmsQueue;
	string  jobName;
	string  localGroup;
	string  execHost;
	string  userFqan;
	string  iBench;
	string  iBenchType;
	string  fBench;
	string  fBenchType;
	string  tz;
	string  forceResourceHlrOnly;
        string  accountingProcedure;
        string  atmClientVersion;
        string  ceHostName;
        string  siteName;
	string  execCe;
	string  submitHost;
	string  lrmsServer;
	string  voOrigin;
	int cpu_time;
	int wall_time;
	string mem;
	string vmem;
	string processors;
	string start;
	string end;
	string ctime;
	string qtime;
	string etime;
	int exitStatus;
	string glueCEInfoTotalCPUs;
	string uniqueChecksum;
	string ceCertificateSubject;
};

int cmnParseLog(string logString, cmnLogRecords& records);
int cmnParseLog(string logString, map<string,string>& recordsMap);

void makeUniqueChecksum(ATM_job_record&, std::string&);
void makeUniqueChecksum(map<string,string>&, std::string&);

int checkDuplicate(ATM_job_record& usage_info,
	bool& success,
	bool& possibleResubmission, 
	string& previousJobId,
	string& uniqueChecksum);

int checkDuplicate(map<string,string>& usageMap,
	bool& success,
	bool& possibleResubmission, 
	string& previousJobId,
	string& uniqueChecksum);

#endif
