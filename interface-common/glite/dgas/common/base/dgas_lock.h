// $Id: dgas_lock.h,v 1.1.2.1.4.2 2011/11/08 09:14:11 aguarise Exp $
// // -------------------------------------------------------------------------
// // Copyright (c) 2001-2002, The DataGrid project, INFN, 
// // All rights reserved. See LICENSE file for details.
// // -------------------------------------------------------------------------
// // Author: Andrea Guarise <andrea.guarise@to.infn.it>
// /***************************************************************************
//  * Code borrowed from:
//  *  authors   : 
//  *             
//  *  
//  ***************************************************************************/
#ifndef DGAS_LOCK_H
#define DGAS_LOCK_H
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <unistd.h>
#include <cstdlib>
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"

using namespace std;

class dgasLock {
	public:
	
	dgasLock ( string _fileName = "" ):       //constructor
			fileName(_fileName){;};
	
	string fileName;  	//lock file name

	int put(); 	//put the lock file 
	int remove();	//removes the lock file
	int appendString(string content);  //append a string in the lock file
	bool exists();  //check if the lock file exists
	int contents(map<string,string> &lockMap);  //retirves the contents of t						    //lock file.

};

class dgasHeartBeat {
	public:
	dgasHeartBeat ( string _processName = "",
			string _hbDir = ""
			):
		processName(_processName),
		hbDir(_hbDir){;};
	string processName;
	string hbDir;
	time_t timestamp;
	pid_t pid;

	int put();
	int get();

};

#endif
