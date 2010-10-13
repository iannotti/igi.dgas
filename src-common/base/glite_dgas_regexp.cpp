/*
 *
 * Copyright (c) 2004
 * John Maddock
 *
 * Use, modification and distribution are subject to the 
 * Boost Software License, Version 1.0. (See accompanying file 
 * LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
 *
 */

#include <iostream>
#include <string>
#include <vector>
#include "glite/dgas/common/base/glite_dgas_regexp.h"

using namespace std;

bool matchRE (string s, string regexp)
{
	boost::regex re;
	try
	{
		re.assign(regexp);
	}
	catch (...)
	{
		cout << regexp << " is not a valid regular expression: \"" << "\"" << endl;
		return 1;
	}
	return boost::regex_match(s,re);
}

bool matchRE (string s, string regexp, vector<string>& matches)
{
	boost::regex re;
	try
	{
		re.assign(regexp);
	}
	catch (...)
	{
		cout << regexp << " is not a valid regular expression: \"" << "\"" << endl;
		return 1;
	}
	boost::cmatch m;
	bool success = boost::regex_match(s.c_str(), m, re);
	if ( success )
	{
		for (int i = 1; i < m.size(); i++)
		{
			string match(m[i].first, m[i].second);
			matches.push_back(match);
		}
	}
	return success;
}

