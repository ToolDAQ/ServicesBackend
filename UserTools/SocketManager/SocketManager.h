#ifndef SocketManager_H
#define SocketManager_H

#include <iostream>

#include "Tool.h"
#include "DataModel.h"

/**
* \class SocketManager
*
* This Tool uses the DAQUtils class to periodically find new clients advertising relevant services and make new connections to their respective zmq sockets.
*
* $Author: Marcus O'Flaherty $
* $Date: 2025/12/11 $
* Contact: marcus.o-flaherty@warwick.ac.uk
*/

struct SocketManager_args : public Thread_args {
	
	DataModel* m_data;
	std::map<std::string,std::string> clientsmap;
	
	std::map<std::string, std::chrono::time_point<std::chrono::steady_clock>> last_update;
	std::chrono::milliseconds update_period_ms;
	
};

class SocketManager: public Tool {
	
	public:
	SocketManager(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Execute function used to perform Tool purpose.
	bool Finalise(); ///< Finalise function used to clean up resources.
	
	private:
	static void Thread(Thread_args* args);
	SocketManager_args thread_args;
	
};


#endif
