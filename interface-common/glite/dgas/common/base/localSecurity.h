// DGAS (DataGrid Accounting System) 
// Local security enforcement utilities
// 
// $Id: localSecurity.h,v 1.1.2.1.4.1 2010/10/19 09:00:16 aguarise Exp $
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
 /***************************************************************************
 * Code borrowed from:
 *  authors   : 
 *  copyright : 
 ***************************************************************************/
//
//    


#ifndef localSecurity_h
#define localSecurity_h

#include <iostream>
#include <fstream>
#include <string>

using namespace std;


extern ofstream logStream;

struct securityStruct{
        string hostProxyFile;
        string gridmapFile;
};

int setSecurityEnvironment(securityStruct s);

int changeUser (string& newUser);

#endif
