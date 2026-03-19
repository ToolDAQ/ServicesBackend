#ifndef MulticastReceiverSender_H
#define MulticastReceiverSender_H

#include <iostream>
#include <chrono>
#include <vector>
// multicast
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>

#include "Tool.h"
#include "DataModel.h"
#include "MulticastReceiveMonitoring.h"

/**
 * \class MulticastReceiverSender
 *
 * This Tool receives and sends logging or monitoring (multicast) messages via a thread, pushing them to/pulling them from the DataModel.
 *
 * $Author: M. O'Flaherty $
 * $Date: 2025/11/26 $
 * Contact: marcus.o-flaherty@warwick.ac.uk
*/

// class for things passed to multicast listener thread
struct MulticastReceive_args : public Thread_args {
	
	std::string m_tool_name;
	DataModel* m_data;
	MulticastReceiveMonitoring* monitoring_vars;
	socklen_t addrlen;
	struct sockaddr_in addr;
	int socket;
	int poll_timeout_ms;
	zmq::pollitem_t poll;
	char message[655355]; // theoretical maximum UDP buffer size - size also hard-coded in thread
	int get_ok;
	size_t local_buffer_size;
	std::vector<std::string>* in_local_queue;
	std::vector<std::string> out_local_queue;
	size_t out_i=0;
	
	std::vector<std::vector<std::string>*>* in_queue;
	std::mutex* in_queue_mtx;
	std::vector<std::string>* out_queue;
	std::mutex* out_queue_mtx;
	
	std::chrono::time_point<std::chrono::steady_clock> last_transfer;
	std::chrono::milliseconds transfer_period_ms;
	
};

class MulticastReceiverSender: public Tool {
	
	public:
	MulticastReceiverSender(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Executre function used to perform Tool perpose.
	bool Finalise(); ///< Finalise funciton used to clean up resorces.
	
	private:
	static void Thread(Thread_args* args);
	MulticastReceive_args thread_args;
	MulticastReceiveMonitoring monitoring_vars;
	
	std::string type_str;  // "logging" or "monitoring"
	int socket_handle;
	int get_ok;
	std::atomic<int>* thread_crashes;
	
	int64_t last_bytes_in;
	int64_t last_bytes_out;
	std::chrono::time_point<std::chrono::steady_clock> last_bytes_time;
	
};

#endif
