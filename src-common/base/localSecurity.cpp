#include "glite/dgas/common/base/localSecurity.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/int2string.h"
#include <string>
#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>



int setSecurityEnvironment(securityStruct s)
{
        int res = 0;
        uid_t dgasUid;
        dgasUid = getuid();
        if ( dgasUid != 0 )
        {
                string logBuff = "DGAS is running under UID=" + int2string(dgasUid);
                hlr_log(logBuff, &logStream,2);
                string envName= "X509_USER_PROXY";
                if ( setenv(envName.c_str(),(s.hostProxyFile).c_str(),1) != 0 )
                {
                        string logBuff = "Error setting variable X509_USER_PROXY=" + s.hostProxyFile;
                        res += 1;
                }
        }
        return res;
}


int changeUser (string& newUser)
{
        int res = 0;
        string logBuff;
        uid_t dgasUid;
        dgasUid = getuid();
        if ( newUser != "root" )
        {
                if ( dgasUid != 0 )
                {
                        logBuff = "DGAS is running under UID=" + int2string(dgasUid) + " not changing user.";
                         hlr_log(logBuff, &logStream,2);
                         res+=3;
                }
                else
                {
                        struct passwd *myPasswd;
                        myPasswd = getpwnam(newUser.c_str());
                        if ( !myPasswd )
                        {
                                logBuff = "Could not find user" + newUser;
                                hlr_log(logBuff, &logStream,2);
                                res += 1;
                        }
                        else
                        {
                                if ( setuid(myPasswd->pw_uid) != 0 )
                                {
                                        logBuff = "Could not switch to user:" + newUser;
                                        hlr_log(logBuff, &logStream,2);
                                        res += 2;
                                }
                        }
                }
        }
        else
        {
                hlr_log("You asked for being 'root', choose: you already are, or you can't be...", &logStream,2);
                res+=4;
        }
        return res;
}
