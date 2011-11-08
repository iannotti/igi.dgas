#ifndef INT2STRING_H
#define INT2STRING_H

#include <sstream>
#include <string>
#include <cstdlib>

using namespace std;

template <class Type>
	inline string int2string(Type i)
{
	ostringstream ost;
	ost << i;
	return ost.str();
}

#endif
