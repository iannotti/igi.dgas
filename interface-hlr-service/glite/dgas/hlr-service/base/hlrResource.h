// $Id: hlrResource.h,v 1.1.2.1.4.1 2010/10/19 09:03:34 aguarise Exp $
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
#ifndef HLRRESOURCE
#define HLRRESOURCE

#include <string>
#include <iostream>
#include <sstream>
#include "glite/dgas/hlr-service/base/hlrAcctdesc.h"
#include "glite/dgas/hlr-service/base/hlrResourceGroupVO.h"
#include "glite/dgas/hlr-service/base/hlrAdmin.h"
#include "glite/dgas/common/base/comm_struct.h"


using namespace std;

class hlrResource {
	public:
		string rid;//db primary key
		string email;
		string descr;
		string ceId;
		string acl;
		string gid;
		string hostCertSubject;

		connInfo connectionInfo;

		hlrResource(string _rid = "",
				string _email= "",
				string _descr= "",
				string _ceId = "",
				string _acl = "",
			        string _gid= ""	
				);


		int get (); //gets user info from uid || gid || email || certSubject
		int get (vector<hlrResource>& rv);// gets every matching record

		int put (); //dumb put, doesn't do any check, replace info 
			    //if present! Remember to create the user group 
			    //before inserting the user(no check is performed
			    //at this level wether the group description 
			    //exists or not!). (It tries to roll back the DB
			    //if something goes wrong).

		int del (); //dumb delete

		bool exists(string _type, string &_acl);
		bool exists ();//try guessing what it does!

		int getKeys (vector<string>& keys);//get matching keys
		
		friend	ostream& operator<< (ostream& os, const hlrResource& r );

		
};




#endif
