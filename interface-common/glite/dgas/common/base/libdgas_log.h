// $Id: libdgas_log.h,v 1.1.2.1.4.2 2011/11/08 09:14:12 aguarise Exp $
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


/*This function library provides some functions used by the API libraries, 
 * the server and the tools
 */


#ifndef HLR_CT_H
#define HLR_CT_H

//#include <stdio.h>
#include <ctime>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>


using namespace std;

/*definitions */

//log levels:

#define LL_DEB 0
#define LL_SERVER 1
#define LL_APP_W 2
#define LL_APP_R 3
#define LL_API_H 4
#define LL_API_L 5

//structures:

//struct hlrd_conf {
//	string conf_file; 
//};

//struct hlr_conf {
//	string conf_file;
//};

/* Error and log printing functions*/

/* These function are used to manage the error and loggin communication
 * coming from the other parts of the software
 */

int bootstrapLog(string logFileName, ofstream *logfile);

int hlr_error ( string error_string );

int hlr_log( string log_string ,int log_level = 0 );

int hlr_log( string log_string, ofstream *logFile ,int log_level = 0 );





/* System configuration fuctions */

/* These functions will read the configuration files and populate the
 * structures containing the information needed by the various parts of
 * the software
 */

#endif
