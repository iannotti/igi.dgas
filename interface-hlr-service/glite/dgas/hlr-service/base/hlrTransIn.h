// $Id: hlrTransIn.h,v 1.1.2.1.4.1 2010/10/19 09:03:34 aguarise Exp $
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

#ifndef HLR_TRANSIN_H
#define HLR_TRANSIN_H


#include<string>
#include<iostream>
#include<sstream>

using namespace std;

class hlrTransIn {
	public:
		int	 tid;//db primary key AUtoIncrementing!
		string rid;
		string gid;
		string fromDn;
		int amount;
		string timeStamp;
		string dgJobId;
		string uniqueChecksum;
		string accountingProcedure;
		
		hlrTransIn (int _tid = 0, 
				string _rid= "",
				string _gid= "",
				string _fromDn= "",
				int _amount = 0,
				string _timeStamp = "",
				string _dgJobId = "",
				string _uniqueChecksum = "",
				string _accountingProcedure = "");

		int get (); //gets user info from gid || vo_id
		//int get (vector<hlrTransIn>& tv);
		int put (); //dumb put, doesn't do any check replace info if present!
		int del (); //dumb delete
		bool exists ();
	//	int getKeys (vector<int>& keys);
};
		int makeTransInObsolete(string &jobId);

#endif
