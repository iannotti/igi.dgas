// $Id: hlrResourceGroupVO.h,v 1.1.2.1.4.1 2010/10/19 09:03:34 aguarise Exp $
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
#ifndef HLRRGVO_H
#define HLRRGVO_H


#include<string>
#include<iostream>
#include<sstream>

using namespace std;

class hlrRgVO {
	public:
		string rid;//db primary key
		string gid;
		string vo_id;
		
		hlrRgVO (string _rid = "", 
				string _gid= "",
				string _vo_id= "");

		int get (); //gets user info from ris & gid & vo_id
		int get (vector<hlrRgVO>& rgVO_v);
		int put (); //dumb put, doesn't do any check replace info if present!
		int del (); //dumb delete
		bool exists ();
		int getKeys (vector<string>& keys);
};

#endif
