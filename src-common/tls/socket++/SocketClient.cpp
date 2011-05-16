/***************************************************************************
 *  filename  : SocketClient.cpp
 *  authors   : Salvatore Monforte <salvatore.monforte@ct.infn.it>
 *  copyright : (C) 2001 by INFN
 ***************************************************************************/

// $Id: SocketClient.cpp,v 1.1.2.1.4.5 2011/05/16 12:29:14 aguarise Exp $

/**
 * @file SocketClient.cpp
 * @brief The file for Socket Client Object.
 * This file contains implementations for Socket Client used in
 * order to communicate with the Resource Broker.
 * @author Salvatore Monforte salvatore.monforte@ct.infn.it
 * @author comments by Marco Pappalardo marco.pappalardo@ct.infn.it and Salvatore Monforte
 */

#include <unistd.h>
#include <memory.h>
#include <stdio.h>
#include <iostream>

/** This class header file. */
#include "glite/dgas/common/tls/SocketClient.h"
/** The communication agent header file. */
#include "glite/dgas/common/tls/SocketAgent.h"

namespace glite { 
namespace wmsutils { 
namespace tls {
namespace socket_pp {

/** 
 * Constructor.
 * @param h the host name.
 * @param p the host port.
 */
SocketClient::SocketClient(const std::string& h, int p) : host(h), port(p)
{
	AttachAgent(new SocketAgent());
}

/**
 * Set the connection timeout.
 * @param secs a size_t representing the timeout in seconds.
 * @return tru on success, false otherwise.
 */
bool SocketClient::SetTimeout(size_t secs)
{
	return ( agent -> SetTimeout( secs ) );
}

/**
 * Attach an agent to this client.
 * This method also connects the agent to the proper server.
 * @param a the Socket Agent to attach.
 * @return true for a successful attachment, false otherwise.
 */
bool SocketClient::AttachAgent(SocketAgent* a)
{
	bool result = false;
	if((agent = a)) 
	{
		agent -> peeraddr_in.sin_family = AF_INET;
		struct hostent *hp; 
		if(!(hp = gethostbyname(host.c_str()))) 
		{
			//std::cerr << "Not found in /etc/hosts" << std::endl;
		}
		else 
		{
			agent -> peeraddr_in.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
			agent -> peeraddr_in.sin_port = htons(port);
			result = true;
		}
	}	
	return result;
}

/**
 * Destructor.
 * This method must be also implemented by object subclassing server socket.
 */
SocketClient::~SocketClient()
{
	close(agent -> sck);
}

/**
 * Open a connection to the Server.
 * @return true on success, false otherwise.
 */
bool SocketClient::Open()
{
	bool result = true;
  	if( (agent -> sck = socket(AF_INET, SOCK_STREAM, 0)) == -1 ) 
	{
		result = false;
	}
	else 
	{
		char value = 1;
		setsockopt( agent -> sck, SOL_SOCKET, SO_REUSEADDR, (void *) &value, sizeof(char) );
		if( connect(agent -> sck, (struct sockaddr*)& agent -> peeraddr_in, sizeof(struct sockaddr_in)) == -1) 
		{
			result = false;
			//error Unable to connect to remote Host():port.
		}
		else 
		{
			socklen_t addrlen = sizeof(struct sockaddr_in);
			struct sockaddr_in myaddr_in;
			memset ((char *)&myaddr_in, 0, sizeof(struct sockaddr_in));
			if (getsockname(agent -> sck, (struct sockaddr*)&myaddr_in, &addrlen) == -1) 
			{
				result = false;
				//error in getsockname()
			}
		}
	}
	return result;
}

/**
 * Close the connection.
 * @return true on success, false otherwise.
 */
bool SocketClient::Close()
{
	return (close(agent -> sck) == 0);
}


SocketAgent* SocketClient::getAgent() const
{
	return agent;
}

} // namespace socket
} // namespace tls
} // namespace wmsutils
} // namespace glite

