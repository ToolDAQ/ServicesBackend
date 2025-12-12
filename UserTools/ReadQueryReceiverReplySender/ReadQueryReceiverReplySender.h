#ifndef ReadReceiverReplySender_H
#define ReadReceiverReplySender_H

#include <iostream>

#include "Tool.h"
#include "DataModel.h"


/**
 * \class ReadReceiverReplySender
 *
 * This Tool gets read queries from a ZMQ ROUTER socket and send replies as well as write query acknowledgements.
 *
 * $Author: Marcus O'Flaherty $
 * $Date: 2025/11/27 $
 * Contact: marcus.o-flaherty@warwick.ac.uk
*/

struct ReadReceiverReplySender_args : public Thread_args {
	
	DataModel* m_data;
	zmq::socket_t* socket=nullptr;
	std::mutex* socket_mtx; // for sharing the socket with ServicesManager Tool for finding clients
	
	int poll_timeout_ms;
	std::vector<zmq::pollitem_t> polls;
	zmq::message_t msg_discard;
	bool make_new;
	int msg_parts;
	int get_ok;
	QueryBatch* in_local_queue;
	QueryBatch* out_local_queue;
	size_t out_i; ///< which query in the batch is next to sent
	
	// for received buffer transfers
	// FIXME we don't track last time of outgoing buffer transfer?
	std::chrono::time_point<std::chrono::steady_clock> last_transfer;
	std::chrono::milliseconds transfer_period_ms;
	size_t local_buffer_size;
	
};

class ReadReceiverReplySender: public Tool {
	
	public:
	ReadReceiverReplySender(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Executre function used to perform Tool perpose. 
	bool Finalise(); ///< Finalise funciton used to clean up resorces.
	
	private:
	static void Thread(Thread_args* args);
	ReadReceiverReplySender_args thread_args;
	
	std::string port_name; // name by which clients advertise sockets for sending read queries to the DB
	
};




#endif
