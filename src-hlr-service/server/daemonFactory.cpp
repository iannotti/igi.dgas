#include <signal.h>
#include "daemonFactory.h"
#include "glite/dgas/common/base/dgas_lock.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "xmlHlrHelper.h"
#include "pthread.h"


extern ofstream logStream;
extern pthread_mutex_t auth_mutex;
extern volatile sig_atomic_t keep_going;
extern int activeThreads;
extern bool restart;
extern int defConnTimeOut;
extern int authErrors;
extern int threadUsecDelay;
extern int messageReservedMemory;
extern listenerStatus lStatus;



void* thrLoop(void *param)
{
	activeThreads++;
	threadStruct *tstruct = (threadStruct*) param;
	string xml_input ="";
	xml_input.reserve(messageReservedMemory);
	string xml_output ="";
	string logString = "";
	pid_t processPid = getpid ();
	string tNString = int2string(tstruct->tN)+"("+int2string(processPid)+")";
	logString = "within thrloop:" + tNString;
	hlr_log( logString, &logStream,8);
	bool goOn = true;
	if (goOn && (tstruct->a) != NULL )
	{
		logString = "Authenticating Agent:" + tNString;
		hlr_log( logString, &logStream,8);
		connInfo connectionInfo;
	 	logString ="in "+ tNString+ " Authenticating: " +(tstruct->a)->PeerName();
		hlr_log( logString, &logStream,7);
		if ( tstruct->a ) (tstruct->a)-> SetTimeout( defConnTimeOut );
		if ( tstruct->a ) (tstruct->s)->set_auth_timeout( 10*defConnTimeOut );
		pthread_mutex_lock (&auth_mutex);
        if ( (tstruct->s)->AuthenticateAgent((tstruct->a)) )
		{
        	pthread_mutex_unlock (&auth_mutex);
			connectionInfo.hostName = (tstruct->a)->PeerName();
			connectionInfo.contactString = (tstruct->a)->CertificateSubject();
			logString = "Authenticated Agent:" + tNString;
			hlr_log( logString, &logStream,8);
		}
		else
	 	{	
                   if ( (tstruct->a) != NULL )
                   {
	                (tstruct->s) -> KillAgent( (tstruct->a) );
	                (tstruct->a) = NULL;
                   }
                   pthread_mutex_unlock (&auth_mutex);
		   logString = "Error while Authenticating agent: " + tNString +":" + (tstruct->s)->getErrMsg();
		   hlr_log( logString, &logStream,2);
		   authErrors++;
		   activeThreads--; 
		   pthread_exit((void *) 0);
      	}
	 	logString = tNString+ " Connection from : " +connectionInfo.hostName;
		logString += ",DN:" + connectionInfo.contactString;
		hlr_log (logString, &logStream, 5);
		logString = "I/O in:" + tNString;
		hlr_log (logString, &logStream, 9);
		bool receiveOk = true;
			if ( (tstruct->a) != NULL )
			{
				(tstruct->a)-> SetRcvTimeout( defConnTimeOut );
				if ( !((tstruct->a)-> Receive( xml_input )) )
				{
					string logString = tNString +" receive error:";
					hlr_log (logString, &logStream, 3);
					if ( (tstruct->a) != NULL )
					{
						logString = "In "+ tNString +" on err, killing the agent";
						hlr_log (logString, &logStream, 8);
				
						if ( (tstruct->a) != NULL )
						{	
							(tstruct->s)->KillAgent((tstruct->a));
							(tstruct->a) = NULL;
						}
					}
					logString = tNString +" killed, continue...";
					hlr_log (logString, &logStream, 8);
					authErrors++;
					activeThreads--;
					pthread_exit((void *) 0);
				}
			}
			else
			{
				receiveOk = false;
			}
			if ( !receiveOk )
			{
				logString += "Error receiving message from client.";
			}
			else
			{
		
				logString = "message received:"+tNString;
				logString += xml_input;
				hlr_log (logString, &logStream, 7);
				int res =-1;
				res = parse_xml (xml_input, connectionInfo, lStatus ,&xml_output);
				logString = "Parser returned with:" + int2string(res);
				hlr_log (logString, &logStream, 6);
				if (res == 0)
				{
					logString = "Sending Message in thread:" + tNString;
					logString += xml_output;
					hlr_log (logString, &logStream, 8);
					if ( ((tstruct->a)) != NULL )
					{
						(tstruct->a)-> SetSndTimeout( defConnTimeOut );
						if ( !( (tstruct->a) -> Send( xml_output) ) )
						{
							string logString = "In thread "+ tNString +" Send,Error.";
							hlr_log (logString, &logStream, 3);
						}
					}
					logString = "Message sent in thread:"+tNString;
					hlr_log (logString, &logStream, 5);
					logString = "sent answer :\n";
					logString += xml_output + "\n";
					hlr_log (logString, &logStream, 7);
				}
			}
		logString = "Killing:" + tNString;
		hlr_log (logString, &logStream, 8);
		if ( ( (tstruct->a) != NULL ) )
		{
			(tstruct->s)->KillAgent((tstruct->a));
			tstruct->a = NULL;
			logString = "Killed:" + tNString;
			hlr_log (logString, &logStream, 8);
		}
	}
  logString = "Exiting from thread:" + int2string(tstruct->tN);
  hlr_log (logString, &logStream, 7);
  activeThreads--;
  usleep(threadUsecDelay); 
  pthread_exit((void *) 0);
}

int putLock(string lockFile)
{
	dgasLock Lock(lockFile);
	if ( Lock.exists() )
	{
		//the lock file already exists, return an error.
		return 1;
	}
	else
	{
		//the lock file doesn't exists, therefore creates it
		//and insert some important information inside the file.
		if ( Lock.put() != 0 )
		{
			//there was an error creating the lock file.
			//exit with an error.
			return 2;
		}
		else
		{
			return 0;
		}
	}
}

int removeLock( string lockFile)
{
	dgasLock Lock(lockFile);
	if ( Lock.exists() )
	{
		//the lock file exits, remove it.
		if ( Lock.remove() != 0 )
		{
			//error removing the lock
			return 2;
		}
		else
		{
			return 0;
		}
	}
	else 
	{
		return 1;
	}
}
