#ifndef dgasClient_h
#define dgasClient_h

/*
 * dgasClientImpl.h
 * 
 * Copyright (C) 2002 by EU DataGrid.
 * For license coinditions see http://www.eu-datagrid.org/license.html
 */

#include "glite/dgas/hlr-clients/base/dgasClientInterface.h"
#include <string>

namespace glite {
namespace wmsutils {
namespace tls {
namespace socket_pp {
	class GSISocketClient;
}
}
}
}

namespace socket_pp = glite::wmsutils::tls::socket_pp;

namespace glite{
namespace workload {
namespace dgas {
namespace clients {
class dgasClient : public virtual dgasClientInterface
{
public:
	/**
	 * Constructor
	 * @param h hostname of the server
	 * @param p portnumber of the server
	 * @param c contact string fro the server, optional.
	 */
	dgasClient(const std::string& h, int p, const std::string& c = "");
	/**
	 * Destructor
	 */
	~dgasClient();
	
	/**
	 * Connects to the DGAS Server.
	 * @return if the connection has been estabilished or not
	 */
	bool connect();
	/**
	 * close the connection with the DGAS Server.
	 * @return if the connection has been estabilished or not
	 */
	bool disconnect();

	/** Send an xml request to the remote peer
	 * @param xml the xml request to be sent.
	 * @return wether the message has been correctly sent or not.
	 */
	bool send(const std::string& xml);
	
	/** receive the xml answer from the server.
	 * @param xml the xml answer received.
	 * @return wether the message has been correctly received or not.
	 */
	bool receive(std::string& xml);

protected:
	
	/** The GSI Client Socket Reference. */
	socket_pp::GSISocketClient* connection;
	
		
};

} // namespace clients
} // namespace dgas
} // namespace workload
} // namespace edg


#endif
