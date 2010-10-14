
#ifndef QTRANSACTION_H
#define QTRANSACTION_H

#include <sstream>

using namespace std;
class qTransaction {

	
public:

	string transactionId;
	string gridUser;
	string gridResource;
	string urSource;
	int amount;
	string timestamp;
	string logData;
	int priority;
	int statusTime;
	string uniqueChecksum;
	string accountingProcedure;

	qTransaction ( 
			string _transactionId="",
			string _gridUser="",
			string _gridResource="",
			string _urSource="",
			string _timestamp="",
			string _logData="",
			int _priority=0,
			int _statusTime=0,
			string _uniqueChecksum="",
			string _accountingProcedure=""
		     );
	int put();
	int get(int _priority); //return the first entry with a given priority
	int get(string _transactionId); //return the entry with the given Id
	int update();//CAVEAT: it just updates priority and statusTime.
	int remove();
	
	

private:
	
	
};

namespace qTrans {

int removeGreaterThan(int pri);
int archiveGreaterThan(int pri,string file);
int remove(string _transactionId);	
int get(int _priority, vector<string> &keys);

}
#endif
