// $Id: hlrTransaction.h,v 1.1.2.1.4.1 2010/10/19 09:03:34 aguarise Exp $
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

#ifndef HLR_TRANSACTION_H
#define HLR_TRANSACTION_H


#include<string>
#include<iostream>
#include<sstream>
#include<vector>

using namespace std;

//using namespace glite::workload::dgas::common;


class hlrTransaction {
	public:

		int tid;
		string id;
		string gridUser;
		string gridResource;
		string urSource;
		int amount;
		string timeStamp;
		string logData;
		string uniqueChecksum;
		string accountingProcedure;

		connInfo connectionInfo;

		hlrTransaction ( int _tid = 0,
				string _id = "",
				string _gridUser = "",
				string _gridResource = "",
				string _urSource = "",
				int _amount=0,
				string _timeStamp="",
				string _logData="",
				string _uniqueChecksum="",
				string _accountingProcedure=""
			       );


		int get();	//plain get method
//		int getKeys(vector<int>& keys); //ket in and out keys matching with proper objects
		int put();
		int del();
		int del(int& start, int& end); //delete range of entries
		bool exists(); //report if the given transaction exists or not.

		int process();// process the transaction and credit/debit the 
			      // account.

friend ostream& operator << ( ostream& os, const hlrTransaction& tr );

	private:
		//int getIn();	//plain get method
		//int getKeysIn(vector<int>& keys); //ket in and out keys matching with proper objects
		//int putIn();
		//int delIn();
		bool existsIn();
};
		
		int getJobId(
				    string &uniqueChecksum,
				    string &jobIdResult,
				    string &accountingProcedureResult);

		int makeObsolete(
					string &jobId);
#endif
