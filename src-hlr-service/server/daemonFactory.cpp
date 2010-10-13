#include <signal.h>
#include "daemonFactory.h"
#include "glite/dgas/common/base/dgas_lock.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "xmlHlrHelper.h"
#include "pthread.h"

#ifdef WITH_VOMS
  #include "glite/security/voms/voms_api.h"
  extern "C" {
        #include <openssl/pem.h>
        #include <openssl/x509.h>
        }
#endif

extern ofstream logStream;
extern pthread_mutex_t auth_mutex;
extern pthread_mutex_t listen_mutex;
//extern pthread_mutex_t receive_mutex;
extern volatile sig_atomic_t keep_going;
extern int activeThreads;
extern bool restart;
extern int defConnTimeOut;
extern int authErrors;
extern int threadUsecDelay;
extern listenerStatus lStatus;

#ifdef WITH_VOMS

static STACK_OF(X509) *load_chain(const char *certfile)
{
  STACK_OF(X509_INFO) *sk=NULL;
  STACK_OF(X509) *stack=NULL, *ret=NULL;
  BIO *in=NULL;
  X509_INFO *xi;
  int first = 1;

  if(!(stack = sk_X509_new_null())) {
    printf("memory allocation failure\n");
    goto end;
  }

  if(!(in=BIO_new_file(certfile, "r"))) {
    printf("error opening the file, %s\n",certfile);
    sk_X509_pop_free(stack,X509_free);
    goto end;
  }

  /* This loads from a file, a stack of x509/crl/pkey sets */
  if(!(sk=PEM_X509_INFO_read_bio(in,NULL,NULL,NULL))) {
    printf("error reading the file, %s\n",certfile);
    sk_X509_pop_free(stack,X509_free);
    goto end;
  }

  /* scan over it and pull out the certs */
  while (sk_X509_INFO_num(sk)) {
    /* skip first cert */
    if (first) {
      first = 0;
      continue;
    }
    xi=sk_X509_INFO_shift(sk);
    if (xi->x509 != NULL) {
      sk_X509_push(stack,xi->x509);
      xi->x509=NULL;
    }
    X509_INFO_free(xi);
  }
  if(!sk_X509_num(stack)) {
    printf("no certificates in file, %s\n",certfile);
    sk_X509_pop_free(stack,X509_free);
    goto end;
  }
  ret=stack;
end:
  if (in) BIO_free_all(in);
  if (sk) sk_X509_INFO_pop_free(sk,X509_INFO_free);
  return(ret);
}

int getAC(string fileName, connInfo& c)
{
        string logBuff;
        int returnCode = 0;;
        int res;
        vomsdata d;
        BIO *in = NULL;
        X509 *x = NULL;
        in = BIO_new(BIO_s_file());
        if (in)
        {
                if (BIO_read_filename(in, fileName.c_str()) > 0)
                {
                        x = PEM_read_bio_X509(in, NULL, 0, NULL);
                        STACK_OF(X509) *chain = load_chain(fileName.c_str());
                        res = d.Retrieve(x, chain, RECURSE_CHAIN);
                        if (res)
                        {
                                std::vector<voms> v = d.data;
                                for (std::vector<voms>::iterator i=v.begin(); i != v.end(); i++)
                                {
                                        if ( (*i).type == TYPE_STD )
                                        {
                                                string fqanBuff;
                                                vomsAC acBuff;
                                                if ( (*i).voname != "" )
                                                {
                                                        c.voname = (*i).voname;
                                                }
                                                for (std::vector<data>::iterator j = i->std.begin(); j != i->std.end(); j++)
                                                {
                                                        fqanBuff = (*j).group;
                                                        fqanBuff += "/Role=" +(*j).role;
                                                        fqanBuff += "/Capability=" +(*j).cap;
                                                        acBuff.group = (*j).group;
                                                        acBuff.role = (*j).role;                                                        acBuff.cap = (*j).cap;
                                                        (c.fqan).push_back(fqanBuff);
                                                        (c.vomsData).push_back(acBuff);
                                                        logBuff = "FQAN:" +fqanBuff;
                                                        hlr_log(logBuff, &logStream,6);
                                                }
                                        }
                                }
                                //ok
                        }
                        else
                        {
                                hlr_log("could not load voms cert chain!",&logStream,4);
                                returnCode =1;
                        }
                        if (chain)
                        {
                                sk_X509_pop_free(chain,X509_free);
                                hlr_log("Executing: sk_X509_pop_free(chain,X509_free)",&logStream,9);
                        }

                }
                else
                {
                                hlr_log("error setting up BIO!",&logStream,3);
                                returnCode=2;
                }
        }
        if (!res)
        {
                //ko!
                returnCode=3;
        }
        if (in)
        {
                BIO_free_all(in);
                hlr_log("Executing: BIO_free_all(in)",&logStream,9);
        }
        if (x)
        {
                X509_free(x);
                hlr_log("Executing: X509_free(x)",&logStream,9);
        }
        return returnCode;
}
#endif


void* thrLoop(void *param)
{
	activeThreads++;
	threadStruct *tstruct = (threadStruct*) param;
	string xml_input ="";
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
		hlr_log( logString, &logStream,6);
		connInfo connectionInfo;
	 	logString ="in "+ tNString+ " Authenticating connection received from : " +(tstruct->a)->PeerName();
		hlr_log( logString, &logStream,4);
		//pthread_mutex_lock (&auth_mutex);
		if ( tstruct->a ) (tstruct->a)-> SetTimeout( defConnTimeOut );
		if ( tstruct->a ) (tstruct->s)->set_auth_timeout( 10*defConnTimeOut );
                if ( (tstruct->s)->AuthenticateAgent((tstruct->a)) )
		{
			connectionInfo.hostName = (tstruct->a)->PeerName();
			connectionInfo.contactString = (tstruct->a)->CertificateSubject();
			string certFile = ( tstruct->a)->CredentialsFile();
			logString = "Cert File:" + certFile;
			hlr_log( logString, &logStream,8);
			#ifdef WITH_VOMS
			if ( certFile != "" )
			{
				hlr_log( "Retrieving voms AC...",&logStream,7);
				getAC(certFile, connectionInfo);
				hlr_log( "...Done.", &logStream,7);
			}
			#endif
			//pthread_mutex_unlock (&auth_mutex);
			logString = "Authenticated Agent:" + tNString;
			hlr_log( logString, &logStream,4);
		}
		else
	 	{	
                   if ( (tstruct->a) != NULL )
                   {
	                (tstruct->s) -> KillAgent( (tstruct->a) );
			(tstruct->a) = NULL;
                   }
		   //pthread_mutex_unlock (&auth_mutex);
		   logString = "Error while Authenticating agent: " + tNString;
		   hlr_log( logString, &logStream,2);
		   authErrors++;
		   activeThreads--; 
		   pthread_exit((void *) 0);
      		}
	 	logString = "in "+ tNString+ " Connection received from : " +connectionInfo.hostName + " ";
		logString += "with cert subject: " + connectionInfo.contactString;
		hlr_log (logString, &logStream, 4);
		logString = "Begin I/O in thread:" + tNString;
		hlr_log (logString, &logStream, 6);	
		bool receiveOk = true;
			if ( (tstruct->a) != NULL )
			{
				(tstruct->a)-> SetRcvTimeout( defConnTimeOut );
				if ( !((tstruct->a)-> Receive( xml_input )) )
				{
					string logString = "In thread "+ tNString +" receive, Error.:";
					hlr_log (logString, &logStream, 3);
					if ( (tstruct->a) != NULL )
					{
						logString = "In thread "+ tNString +" killing the agent";
						hlr_log (logString, &logStream, 6);
				
						if ( (tstruct->a) != NULL )
						{	
							(tstruct->s)->KillAgent((tstruct->a));
							(tstruct->a) = NULL;
						}
					}
					logString = "In thread "+ tNString +" killed, continue...";
					hlr_log (logString, &logStream, 6);
					authErrors++;
					activeThreads--;
					pthread_exit((void *) 0);
				}
			}
			if ( !receiveOk )
			{
				logString += "Error receiving message from client.\n";
			}
			else
			{
		
				logString = "message received:"+tNString;
				logString += xml_input + "\n";
				hlr_log (logString, &logStream, 7);
				int res =-1;
				res = parse_xml (xml_input, connectionInfo, lStatus ,&xml_output);
				logString = "Parser returned with:" + int2string(res);
				hlr_log (logString, &logStream, 6);
				if (res == 0)
				{
					logString = "Sending Message in thread:" + tNString;
					logString += xml_output;
					hlr_log (logString, &logStream, 6);
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
		hlr_log (logString, &logStream, 7);
		if ( ( (tstruct->a) != NULL ) )
		{
			(tstruct->s)->KillAgent((tstruct->a));
			tstruct->a = NULL;
			logString = "Killed:" + tNString;
			hlr_log (logString, &logStream, 6);
		}
	}
  logString = "Exiting from thread:" + int2string(tstruct->tN);
  hlr_log (logString, &logStream, 6);
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
