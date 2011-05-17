// $Id: libdgas_log.cpp,v 1.1.2.1.4.2 2011/05/17 13:08:47 aguarise Exp $
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


#ifndef LIB_HLR_CT_H
#define LIB_HLR_CT_H


#include <iostream>
#include <fstream>
#include <sstream>
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/int2string.h"
#include <syslog.h>

#ifndef SYSTEM_LOG_LEVEL
#define SYSTEM_LOG_LEVEL 8
#endif

extern int system_log_level;

/* Function definitions*/

	

/* Error and Log printing functions 
 *
 */
  

int hlr_error ( string error_string )
{
	cerr << "ERROR: "<< error_string << endl;
	return 0;
}


int hlr_log ( string log_string , int log_level )
{
	if ( log_level <= SYSTEM_LOG_LEVEL )
	{
		cerr << "LOG (" << log_level << "): " << log_string << endl;
	}
	return 0;
}

int hlr_log ( string log_string, ofstream *logFile , int log_level )
{
	if ( !(*logFile) )
	{
		//SYSYLOG
		int levelBuff = LOG_NOTICE;
		switch ( log_level )
		{
			case 0 : levelBuff = LOG_CRIT; break;	
			case 1 : levelBuff = LOG_ERR; break;	
			case 2 : levelBuff = LOG_WARNING; break;	
			case 3 : levelBuff = LOG_WARNING; break;	
			case 4 : levelBuff = LOG_NOTICE; break;	
			case 5 : levelBuff = LOG_NOTICE; break;	
			case 6 : levelBuff = LOG_INFO; break;	
			case 7 : levelBuff = LOG_INFO; break;	
			case 8 : levelBuff = LOG_DEBUG; break;	
			case 9 : levelBuff = LOG_DEBUG; break;	
			default : break;
		}
		if ( log_level <= system_log_level )
		{
			syslog (levelBuff, "(%d):%s", log_level, log_string.c_str());
		}
		return 0;
	}
	else
	{
		//DGAS log
		time_t curtime;
		struct tm *timeLog;
		curtime = time(NULL);
		timeLog = localtime(&curtime);
		char timeBuff[22];
		strftime(timeBuff,sizeof(timeBuff),"%Y %b %d %T",timeLog); 
		//strip end of line from log_string;
		if ( log_level <= system_log_level )
		{
			*logFile << timeBuff << " ("<< log_level <<"):" << log_string << endl << flush;
		}
		return 0;
	}
}


/* Function definitions*/

int bootstrapLog(string logFileName, ofstream *logfile)
{
	if ( logFileName.substr(0,6) != "SYSLOG" )
	{
		time_t curtime;
		struct tm *timeLog;
		curtime = time(NULL);
		timeLog = localtime(&curtime);
	
		logfile->open( logFileName.c_str() , ios::app);
		if ( !(*logfile) )
		{
			return 1;
		}
		else
		{
			*logfile << "Log start up on:" << asctime(timeLog) << endl;
			return 0;
		}
	}
	else
	{
		setlogmask (LOG_UPTO (LOG_DEBUG));
		int lev = atoi((logFileName.substr(6,1)).c_str());
		switch ( lev )
		{
			case 1: openlog ("DGAS", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1);
					break;
			case 2: openlog ("DGAS", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL2);
					break;
			case 3: openlog ("DGAS", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL3);
					break;
			case 4: openlog ("DGAS", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL4);
					break;
			case 5: openlog ("DGAS", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL5);
					break;
			case 6: openlog ("DGAS", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL6);
					break;
			case 7: openlog ("DGAS", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL7);
					break;
			default : openlog ("DGAS", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1);
                                        break;
		}
		string msg = "Log start up:"+ int2string(lev);
		syslog (LOG_NOTICE, msg.c_str());
		return 0;
	}
	

}

/* Error and Log printing functions 
 *
 */
  


#endif
