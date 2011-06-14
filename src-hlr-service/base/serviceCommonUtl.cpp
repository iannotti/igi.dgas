#include "serviceCommonUtl.h"
#include "glite/dgas/common/base/stringSplit.h"
#include <vector>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <fstream>


void makeUniqueString(string &logEntries, string& uniqueString)
{
        uniqueString = md5sum(logEntries);
        return;
}
/*FIXME remove
string checkUserVo (string& currentUserVo, string& userFqan ,string& localUserId, string& voSource)
{
        //used to fix records with userVohaving well
        //known problem.
        string voBuff = currentUserVo;
        if ( currentUserVo == "Role=NULL" || currentUserVo == "" )
        {
                if ( userFqan != "" )
                {
                        //try with FQAN
                        vector<string> buffV;
                        Split ('/',userFqan, &buffV );
                        voBuff = buffV.front();
                        if ( voBuff != "" )
                        {
                                size_t pos = voBuff.find("VO=");
                                if ( pos != string::npos)
                                {
                                        voBuff = voBuff.substr(3,voBuff.size()-3);
                                }
                                voSource = "Found user vo from FQAN:";
                                voSource += voBuff;
                                if ( voBuff != "" )
                                {
                                        return voBuff;
                                }
                        }
                }
                if ( localUserId != "" )
                {
                        //try from pool account.
                        size_t pos = (localUserId).find("sgm");
                        if ( pos  == string::npos )
                        {
                                //it is not an *sgm user, try if it is and *prd user.
                                pos = (localUserId).find("prd");
                                if ( pos  == string::npos )
                                {
                                        pos = (localUserId).find("gold");
                                        if ( pos  == string::npos )
                                        {
                                                pos = (localUserId).find("silver");
                                                if ( pos  == string::npos )
                                                {
                                                        pos = (localUserId).find("bronze");
                                                }
                                        }
					}
                        }
                        if (  pos != string::npos )
                        {
				if ( pos == 0 )
				{
					//it is {sgm,prd}userVo[ddd]
					string buff = 
						(localUserId).substr(3);
					//buff now is userVo[ddd]
					bool isPool = true;
					string suffixBuff =  localUserId.substr(localUserId.size()-3);
					string::const_iterator it = suffixBuff.begin();
					while ( it != suffixBuff.end() )
					{                                
						if (!isdigit(*it) )
                                		{
							isPool = false;
						}
                       	        	 	it++;
                        		}
					if ( isPool )
					{
						//it is userVoddd
                                        	voBuff = (buff).substr(0,
                                                	(buff).size()-3);
                              	          	voSource = "Found user vo from {prd,sgm}userId(ddd) pool account:";
                                	        voSource += voBuff;
                                        	if ( voBuff != "" )
                          	              	{
                                	                return voBuff;
                                       	 	}
					}
					else
					{
						//it is userVo
						voBuff = buff;
                              	          	voSource = "Found user vo from {prd,sgm}userId account:";
                                	        voSource += voBuff;
                                        	if ( voBuff != "" )
                          	              	{
                                	                return voBuff;
                                       	 	}
					}
					
				}
				else
				{
                     	           //it is either an *sgm or *prd user.
                        	        voBuff =
                                	        (localUserId).substr(0,pos);
                   	             voSource = "dgasBankClient: Found user vo for prd,sgm,gold,silver,bronze) user:";
                        	        voSource += voBuff;
                                	if ( voBuff != "" )
                                	{
                                        	return voBuff;
                                	}
				}
                        }
                        else
                        {
                                //check pool account.
				bool isPool = true;
				string suffixBuff =  localUserId.substr(localUserId.size()-3);
				string::const_iterator it = suffixBuff.begin();
				while ( it != suffixBuff.end() )
				{                                
					if (!isdigit(*it) )
                                	{
						isPool = false;
					}
                                	it++;
                        	}
                        	if ( isPool )
				{
                                        voBuff = (localUserId).substr(0,
                                                (localUserId).size()-3);
                                        voSource = "Found user vo from pool account:";
                                        voSource += voBuff;
                                        if ( voBuff != "" )
                                        {
                                                return voBuff;
                                        }
				}
                        }
                }
	voBuff = "NULL";
	return voBuff;
        }
	if ( currentUserVo.size() > 3 )
	{
		if ( currentUserVo.substr(currentUserVo.size()-3,3) == "sgm" 
			|| currentUserVo.substr(currentUserVo.size()-3,3) == "prd" )
		{
			//userVO derived from poolaccount of type:
			//atlassgm001
			voBuff = currentUserVo.substr(0,currentUserVo.size()-3);
        		voSource = "VO retrieved from an userVo{sgm,prd} account: " + currentUserVo; 
			return voBuff;
		} 
	}
        //didn't match any fix criteria, should use what we have.
        voSource = "We use the VO received from the caller:" + currentUserVo; 
        return currentUserVo;
}
*/
