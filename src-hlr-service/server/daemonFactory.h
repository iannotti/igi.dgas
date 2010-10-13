#ifndef daemonFactory_h
#define daemonFactory_h

#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/tls/GSISocketServer.h"
#include "glite/dgas/common/tls/GSISocketAgent.h"

using namespace std;
using namespace glite::wmsutils::tls::socket_pp;

//the structure passed to the thread
struct threadStruct{
	GSISocketAgent* a;
	GSISocketServer* s;
	int tN;
	bool isIn;
};

//the loop implementing the thread communication with the remote peer
void* thrLoop(void *param);

int putLock(string lockFile);

int removeLock( string lockFile);

#endif
