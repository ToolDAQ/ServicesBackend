#ifndef WriteQueryReceiver_H
#define WriteQueryReceiver_H

#include <iostream>
#include <chrono>

#include "Tool.h"
#include "DataModel.h"
#include "WriteReceiveMonitoring.h"

/**
* \class WriteQueryReceiver
*
* This Tool receives Write queries from clients over a ZMQ_SUB socket and pushes them to the DataModel
*
* $Author: M. O'Flaherty $
* $Date: 2025/11/26 $
* Contact: marcus.o-flaherty@warwick.ac.uk
*/


struct WriteQueryReceiver_args : public Thread_args {
	
	std::string m_tool_name;
	DataModel* m_data;
	WriteReceiveMonitoring* monitoring_vars;
	zmq::socket_t* socket=nullptr;
	std::mutex* socket_mtx; // for sharing the socket with ServicesManager Tool for finding clients
	
	int poll_timeout_ms;
	zmq::pollitem_t poll;
	zmq::message_t msg_discard;
	bool make_new;
	int msg_parts;
	int get_ok;
	QueryBatch* in_local_queue;
	
	std::chrono::time_point<std::chrono::steady_clock> last_transfer;
	std::chrono::milliseconds transfer_period_ms;
	size_t local_buffer_size;
	
};

class WriteQueryReceiver: public Tool {
	
	public:
	WriteQueryReceiver(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Executre function used to perform Tool perpose. 
	bool Finalise(); ///< Finalise funciton used to clean up resorces
	
	private:
	static void Thread(Thread_args* args);
	WriteQueryReceiver_args thread_args;
	WriteReceiveMonitoring monitoring_vars;
	
	std::string port_name; // name by which clients advertise sockets for sending write queries to the DB
	
	bool am_master;
	//bool Promote(); ///< Connect to clients to start receiving messages, if we became master
	//bool Demote();  ///< Disconnect from clients to stop receiving & processing messages, if we are no longer master
	
	
};

#endif
