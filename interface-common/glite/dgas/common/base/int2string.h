#ifndef INT2STRING_H
#define INT2STRING_H

#include <sstream>
#include <string>

using namespace std;

template <class Type>
	inline string int2string(Type i)
{
	ostringstream ost;
	ost << i;
	return ost.str();
}

	/*
inline string long2string(long l)
{
	ostringstream ost;
	ost << l;
	return ost.str();
}

inline string float2string(float f)
{
	ostringstream ost;
	ost << f;
	return ost.str();
}
*/
#endif
