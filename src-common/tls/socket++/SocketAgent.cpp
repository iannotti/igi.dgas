/*
 * SocketAgent.cpp
 * 
 * Copyright (C) 2002 EU Datagrid
 * For license conditions see http://www.eu-datagrid.org/license.html
 */

/**
 * @file SocketAgent.cpp
 * @brief The file for Socket Agent Object.
 * This file contains implementations for Socket Agent used for message
 * exchange between Client and Server.
 * @author Salvatore Monforte salvatore.monforte@ct.infn.it
 * @author Marco Pappalardo marco.pappalardo@ct.infn.it
 * @author comments by Marco Pappalardo marco.pappalardo@ct.infn.it and Salvatore Monforte
 */

#include <cstdio>
#include <unistd.h>
#include <memory.h>
#include <sys/time.h>
#include <errno.h>
#include <arpa/inet.h>
/** This class header file. */
#include "glite/dgas/common/tls/SocketAgent.h"
#include "../../../interface-common/glite/dgas/common/base/int2string.h"

namespace glite { 
namespace wmsutils { 
namespace tls {
namespace socket_pp {

/**
 * Constructor.
 */
SocketAgent::SocketAgent()
{
  	memset ((char *)&peeraddr_in, 0, sizeof(struct sockaddr_in));
	m_recv_timeout = m_send_timeout = -1;
}

/**
 * Destructor.
 */
SocketAgent::~SocketAgent()
{
	close(sck);
}

/*
bool SocketAgent::is_recv_pending()
{
	fd_set readfs;
	struct timeval timeout;
	timeout.tv_sec=m_recv_timeout;
	timeout.tv_usec=0;
	FD_ZERO(&readfs);
	FD_SET(sck,&readfs);
 	int result(
		select(sck+1,&readfs,0,0,m_recv_timeout<0?0:&timeout)
	);
	return result==1;
}
*/

/*
bool SocketAgent::is_send_pending()
{
	fd_set sendfs;
	struct timeval timeout;
	timeout.tv_sec=m_send_timeout;
	timeout.tv_usec=0;
	FD_ZERO(&sendfs);
	FD_SET(sck,&sendfs);
	int result(
		select(sck+1,0,&sendfs,0,m_send_timeout<0?0:&timeout)
	);
	return result==1;
}
*/

/**
 * Set the connection timeout.
 * @param secs a size_t representing the timeout in seconds.
 * @return tru on success, false otherwise.
 */
bool SocketAgent::SetTimeout( int secs )
{
	m_send_timeout = m_recv_timeout = secs;
	return true;
}

bool SocketAgent::SetSndTimeout( int secs )
{  
	m_send_timeout = secs;
	return true;
}

bool SocketAgent::SetRcvTimeout( int secs )
{
	m_recv_timeout = secs;
	return true;  
}



/**
 * Returns the host name.
 * @param the string to fill with host name.
 */
std::string SocketAgent::PeerName()
{
	struct hostent*
		hp = gethostbyaddr ((char *) &peeraddr_in.sin_addr,
			sizeof (struct in_addr),
			peeraddr_in.sin_family);
	if(hp) 
	{
		return std::string(hp->h_name);
	}
	else 
	{
		return PeerAddr();
	}
}

/**                                                                                               
 * Returns peer entity ip address.                                                                
 * @return peer entity ip address.                                                                
 */ 
std::string SocketAgent::PeerAddr() 
{ 
	return std::string(inet_ntoa(peeraddr_in.sin_addr));
} 
 
/**                                                                                               
 * Returns peer entity ip port.                                                                   
 * @return peer entity ip port.                                                                   
 */ 
int SocketAgent::PeerPort() 
{
	return peeraddr_in.sin_port; 
} 

} // namespace socket_pp
} // namespace tls
} // namespace wmsutils
} // namespace glite

