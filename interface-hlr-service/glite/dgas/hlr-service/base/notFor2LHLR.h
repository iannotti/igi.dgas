#ifndef NOTFOR2LHLR_H
#define NOTFOR2LHLR_H
#include<iostream>
#include<string>

inline void notfor2lHlr (string& is2ndLeveHlr)
{
	if ( is2ndLeveHlr != "true" )
	{
		return;
	}
	else
	{
		cerr << "Sorry this feature is useless on Second Level HLRs!" << endl;
		exit(0);
	}
}

#endif
