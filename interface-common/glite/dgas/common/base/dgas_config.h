// $Id: dgas_config.h,v 1.1.2.1.4.3 2011/11/08 09:14:12 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
/***************************************************************************
 * Code borrowed from:
 *  authors   : 
 *             
 *  
 ***************************************************************************/




#ifndef DGAS_CONFIG_H
#define DGAS_CONFIG_H
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <cstdlib>

using namespace std;

int
getEnvValue(string envValue, string& gliteLocation );

int
dgas_conf_read( string confFileName, map<string,string> *confMap);

string dgasLocation();

#endif

