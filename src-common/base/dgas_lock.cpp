#include "glite/dgas/common/base/dgas_lock.h"
#include <unistd.h>
#include <sys/types.h>


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

int dgasLock::put()
{
	fstream lockStream(fileName.c_str(), ios::out);
	if ( !lockStream )
	{
	#ifdef DEBUG
		cerr << "Error opening lock file" << endl;
	#endif
		return atoi(E_LOCK_OPEN); 
	}
	else
	{
		pid_t processPid = getpid ();
		lockStream << processPid << endl;
		lockStream.close();
		return 0;
	}
}

int dgasLock::remove()
{
	int res = unlink ( fileName.c_str());
	if ( res != 0 )
	{
		return atoi(E_LOCK_REMOVE);
	}
	else
	{
		return 0;
	}
}

int dgasLock::appendString(string content)
{
	fstream lockStream(fileName.c_str(), ios::app);
	if ( !lockStream )
	{
	#ifdef DEBUG
		cerr << "DEBUG: dgasLock::appendString(string content): Error opening lock file" << endl;
	#endif
		return atoi(E_LOCK_OPEN); 
	}
	else
	{
		lockStream << content << endl;
		lockStream.close();
		return 0;
	}
}

bool dgasLock::exists()
{
	ifstream lockStream (fileName.c_str(), ios::in);
	if ( !lockStream )
	{
		return false;
	}
	else
	{
		return true;
	}
}

int dgasLock::contents(map<string,string> &lockMap)
{
	ifstream inFile(fileName.c_str());
	if ( ! inFile )
	{
		#ifdef DEBUG
		 cerr << "Error opening lock file: " << fileName << endl;
		#endif
		return atoi(E_LOCK_OPEN);
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
			lockMap.insert(
					map<string,string>::
					value_type( param, value)
				       );
		}
	}
	inFile.close();
	return 0;
}


int dgasHeartBeat::put()
{
	string fileName = hbDir + "/" + processName;
	fstream hbStream(fileName.c_str(), ios::out);
	if ( !hbStream )
	{
		return atoi(E_HB_OPEN); 
	}
	else
	{
		//writes in the file 
		pid_t processPid = getpid ();
		hbStream << processPid << ":";
		time_t timestampBuff = time(NULL);
		if ( timestampBuff != -1 ) 
		{
			hbStream << timestampBuff << endl;
		}
		else
		{
			return atoi(E_HB_GETTIME);
		}
		hbStream.close();
		return 0;
	}
}


int dgasHeartBeat::get()
{
	string fileName = hbDir + "/" + processName;
	ifstream inFile(fileName.c_str());
	if ( ! inFile )
	{
		return atoi(E_HB_OPEN);
	}
	string textLine;
	while ( getline (inFile, textLine, '\n'))
	{
		textLine = stripBeginBlanks(textLine);
		if (textLine[0] == '#')
		{
			continue;
		}
		if (textLine.find(":") != string::npos )
		{
			string::size_type pos = 0;
			pos = textLine.find_first_of(":");
			string pidBuff = textLine.substr(0,pos);
			string timestampBuff = textLine.substr(pos+1);
			if ( pidBuff != "" )
				pid = atoi(pidBuff.c_str());
			if ( timestampBuff != "" )
				timestamp = atol(timestampBuff.c_str());
			return 0;
		}
	}
	return atoi(E_HB_GET);
}
