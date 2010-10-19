// $Id: hlrAcctdesc.h,v 1.1.2.1.4.1 2010/10/19 09:03:34 aguarise Exp $
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
#ifndef HLRACCTDESC
#define HLRACCTDESC

#include <iostream>
#include <sstream>
#include <string>


using namespace std;

class hlrAcctdesc {
	public:
		string id;//db primary key
		string type;
		string email;
		string descr;
		string certSubject;
		string acl;

		hlrAcctdesc(string _uid = "", 
				string _type= "",
				string _email= "",
				string _descr= "",
				string _certSubject = "");


		int get (); //gets user info from uid || email || certSubject
		int get (vector<hlrAcctdesc>& av);// gets every matching record
		int put (); //dumb put, doesn't do any check replace info if present!
		int del (); //dumb delete
		bool exists ();
		bool exists (string _type, string& _acl);
		int getKeys (vector<string>& keys);//get matching keys

		friend	ostream& operator << ( ostream& os, const hlrAcctdesc& a );
		
};

#endif



