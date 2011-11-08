#ifndef XMLUTIL_H
#define XMLUTIL_H
#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <cstdlib>
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"

using namespace std;


inline string float2string(float f)
{
	ostringstream ost;
	ost << f;
	return ost.str();
}

namespace glite
{
namespace workload
{
namespace dgas
{
namespace common
{
typedef map<string,string> attrType;

struct attribute
{
	string key;
	string value;
};

}
}
}
}
string timeStamp2ISO8601 (time_t t);
string timeStamp2ISO8601 (time_t t,long int gmtOff); //gmtoff in the form 
//HH*MM*SS
time_t ISO86012timeStamp (string &t);
string seconds2ISO8601  (int s);
int ISO86012seconds (string s);


using namespace glite::workload::dgas::common;
class node {
public:

	node ( string *_mainDoc = NULL, int _status = 0, string _tag = "", string _text = "", int _startPos = 0, int _endPos = 0, bool _valid = false ):
		mainDoc(_mainDoc),
		status(_status),
		tag(_tag),
		text(_text),
		startPos(_startPos),
		endPos(_endPos),
		valid(_valid){;};



	//remove the node from the xml;
	int release();
	attrType getAttributes();

public:
	string * mainDoc;
	int status;

private:
	string tag;

public:
	string text;

private:
	int startPos;
	int endPos;	
	bool valid;




};


//retrieve the info in the node delimited by "tag"
node parse(string *xmlInput, string _tag);
//retrieve the info in the node delimited by "tag" OR
// "space:tag"
node parse(string *xmlInput, string _tag, string space);
//release node if successful parsing.
node parseAndRelease(node &inputNode, string _tag);
string parseAndReleaseS(node &inputNode, string _tag);

node parseImpl(string *xmlInput, string& _tag, size_t pos, size_t pos2);
int parseImpl(string *xmlInput, string& output , string& _tag, size_t pos, size_t pos2);
string tagAdd(string tag, string content);
string tagAdd(string tag, string content, vector<attribute> );
string tagAdd(string tag, int content);
string parseAttribute ( string a, map<string,string>& m);
#endif
