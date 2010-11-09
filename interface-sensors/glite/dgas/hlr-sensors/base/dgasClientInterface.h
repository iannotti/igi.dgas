#ifndef dgasClientInterface_h
#define dgasClientInterface_h

/*
 *  * dgasClientInterface.h
 *  * 
 *  * Copyright (C) 2002 by EU DataGrid.
 *  * For license coinditions see http://www.eu-datagrid.org/license.html
 *  */

/*
 * Author: Andrea Guarise <andrea.guarise@to.infn.it>
 */


#include <string>

namespace glite {
namespace workload {
namespace dgas {
namespace clients {

class dgasClientInterface
{
public:
	/**
	 * empty Constructor.
	 */
	dgasClientInterface() {}
	/**
	 * Destructor.
	 */
	virtual ~dgasClientInterface() {};		

	/**
	 * Initialize a connection to the remote DGAS server
	 * @return if the connection has been estabilished or not
	 */
	virtual bool connect() = 0;
	/**
	 * close the connection to the remote DGAS server
	 * @return if the connection has been closed or not
	 */
	virtual bool disconnect() = 0;
	/**
	 * Send an xml dgas request to the remote peer
	 * @param xml containing the request
	 * @return whether the request has been correctly sent to server.
	 */
	virtual bool send(const std::string& xml) = 0;
	/**
	 * Receive an xml containing the answer from the remote peer
	 * @param xml containing the answer
	 * @return whether the answer has been correctly received.
	 */
	virtual bool receive(std::string& xml) = 0;
	
};
	
} // namespace clients
} // namespace dgas
} // namespace workload
} // namespace edg

#endif
