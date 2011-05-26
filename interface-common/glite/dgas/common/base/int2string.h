#ifndef INT2STRING_H
#define INT2STRING_H

#include <sstream>
#include <string>

using namespace std;

inline string int2string(int i)
{
	ostringstream ost;
	ost << i;
	return ost.str();
}
	
inline string int2string(long l)
{
	ostringstream ost;
	ost << l;
	return ost.str();
}

inline string int2string(float f)
{
	ostringstream ost;
	ost << f;
	return ost.str();
}

#endif
