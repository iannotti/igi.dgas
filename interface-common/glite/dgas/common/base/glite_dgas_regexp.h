#include <vector>
#include <string>
#include <boost/regex.hpp>

using namespace std;

bool matchRE (string s, string regexp);

bool matchRE (string s, string regexp, vector<string>& matches);

