//#include<map>
#include <vector>

#ifndef TRANSINLOG_H
#define TRANSINLOG_H

using namespace std;

class transInLog {

public:

	transInLog (    string _dgJobId="", 
			string _log = "");
	
	
	int put();
	// int addToLog(string &additionalLog);
	int get(string _dgJobId); //0 on success (1 row retrieved)
	// int get(map<string,string> &logEntries); //0 on success (1 row)
//	int get(vector<string> &logEntries); ONSOLETE
	int remove();
	


	string dgJobId;
	string log;

private:
	transInLog (const transInLog&);
	transInLog& operator=(const transInLog&); 

	// int changeLog(string &newLog);
	
	
};
#endif
