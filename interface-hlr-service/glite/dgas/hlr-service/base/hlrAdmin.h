// $Id: hlrAdmin.h,v 1.1.2.1.4.1 2010/10/19 09:03:34 aguarise Exp $
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

#ifndef HLRADMIN_H
#define HLRADMIN_H

#include <iostream>
#include <sstream>
#include <string>
#include "glite/dgas/common/base/comm_struct.h"


using namespace std;

class hlrAdmin {
	public:
		string acl;

		hlrAdmin(string _acl = "");

		int get (); //gets user info from gid
		int get (vector<hlrAdmin> &gv);
		int put (); //dumb put, doesn't do any check replace info if present!
		int del (); //dumb delete
		bool exists ();
		
};


class hlrVoAcl {
	public:
		int seqNumber;
		string voId;
		string acl;

		hlrVoAcl( string _voId = "",
			string _acl = "");
		
		int get (vector<hlrVoAcl> &);
		int put();
		int del();
		int del(int );
		bool exists();
};

class roles {
	public:
		int seqNumber;
		string role;
		string dn;
		string permission;
		string queryType;
		string queryAdd;
		
		roles( string _dn = "",
			string _role = "");
		int get (vector<roles> &);
		int put();
		int del();
		int del(int );
		bool exists();
};


class hlrVomsAuthMap {
        public:
                int seqNumber;
                string voId;
                string voRole;
                string hlrRole;

                hlrVomsAuthMap ( string _voId = "",
                        string _voRole = "",
                        string _hlrRole = "" );
                int get ();
                int get (vector<hlrVomsAuthMap>& vv);
                int put ();
                int del ();
                bool exists();
};

string hlrRoleGet(connInfo& c);
#endif
