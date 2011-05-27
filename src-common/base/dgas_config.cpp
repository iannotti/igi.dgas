// $Id: dgas_config.cpp,v 1.1.2.1.4.6 2011/05/27 09:04:04 aguarise Exp $
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

#include <iostream>
#include <fstream>
#include <string>
#include "glite/dgas/common/base/dgas_config.h"

inline string stripBeginBlanks (string textLine)
{
	string::size_type pos = 0;
	if ( textLine.length() != 0 )
	{
	while ( textLine[pos] == ' ' || textLine[pos] == '\t' || textLine[pos] == '\n')
	{
		textLine.erase( pos, 1);
	}
	}
	return textLine;
}

int getEnvValue(string envName, string &envValue)
{
        char * buff;
        buff = (char *)malloc(4096);
        if ( buff == NULL)
        {
                cerr << "Ooops! out of Mem ?!?" << endl;
                exit(1);
        }
        char buff2[4096];
        buff = getenv(envName.c_str());
	if ( buff == NULL )
	{
		return 2;
	}
        strcpy(buff2,buff);
        envValue = buff2;
/*
        if ( buff != NULL )
        {
                free(buff);
        }
*/
        return 0;
}

int setEnvValue(string envName, string envValue)
{
	return setenv(envName.c_str(),envValue.c_str(),0);
}


string dgasLocation()
{
	//Try to find the value of DGAS_LOCATION first, GLITE_LOCATION later. If none is found
	// use /usr/ value to ensure backward compatibility.
	string dgasLocation;
	getEnvValue("DGAS_LOCATION", dgasLocation);
	if ( dgasLocation == "" )
	{
		getEnvValue("GLITE_LOCATION", dgasLocation);
	}
	if ( dgasLocation == "" )
	{
		dgasLocation = "/usr/"; 
	}
	return dgasLocation;
}

int
dgas_conf_read( string confFileName, map<string,string> *confMap)
{
	//check for DGAS_LOCATION ENV first: Set it to default /usr if not found.
	string dgasLocation;
	getEnvValue("DGAS_LOCATION", dgasLocation);
	if ( dgasLocation == "" )
	{
		setEnvValue("DGAS_LOCATION", "/usr" );
	}
	ifstream inFile(confFileName.c_str());
	if ( ! inFile )
	{
		 return 1;
	}
	string textLine;
	while ( getline (inFile, textLine, '\n'))
	{
		textLine = stripBeginBlanks(textLine);
		if (textLine[0] == '#')
		{
			continue;
		}
		if (textLine.find("=") != string::npos )
		{
			string::size_type pos = 0;
			pos = textLine.find_first_of("=");
			string param = textLine.substr(0,pos);
			string value = textLine.substr(pos);
			pos =0;
			while ((pos = param.find_first_of( " \t", pos)) 
					!= string::npos)
			{
				param.erase ( pos, 1);
			}
			string::size_type endpos = 0;
			pos = value.find_first_of("\"");
			endpos = value.find_first_of("\"", pos+1);
			if ( pos == string::npos || endpos == string::npos)
				return 2;
			value = value.substr(pos+1, endpos-pos-1);
			//if value contains a ${ENV_NAME} part, substitute it
			//with the right value.
			bool goOn = true;
			while ( goOn)
			{
				pos = value.find("${");
				if ( pos != string::npos)
				{
					string::size_type pos1=0;
					pos1 = value.find("}");
					if ( pos1 != string::npos )
					{
						string envValue;
						string envName;
						envName = value.substr(pos+2, (pos1-pos)-2);
						if ( getEnvValue(envName, envValue) != 0)
						{
							return 4;
						}
						else
						{
							value.erase(pos, (pos1-pos)+1);
							value.insert(pos, envValue); 
						}
					}
					else
					{
						return 3;
					}
				}
				else
				{
					goOn = false;
				}
			}
			
			confMap->insert(
					map<string,string>::
					value_type( param, value)
					);
			
		}
			
	}
	inFile.close();
	return 0;
}//dgas_conf_read()


