#ifndef WriteReceiveMonitoring_H
#define WriteReceiveMonitoring_H

#include "MonitoringVariables.h"

class WriteReceiveMonitoring : public MonitoringVariables {
	public:
	WriteReceiveMonitoring(){};
	~WriteReceiveMonitoring(){};
	
	std::atomic<int> polls_failed; // error polling socket
	std::atomic<int> rcv_fails;  // error in recv_from
	std::atomic<int> msgs_rcvd; // messages successfully received
	std::atomic<int> bad_msgs; // messages with the wrong number of zmq parts
	std::atomic<int> in_buffer_transfers; // transfers of thread-local message vector to datamodel
	std::atomic<int> thread_crashes; // restarts of tool worker thread (main thread found reader thread 'running' was false)
	
	std::string toJSON(){
		
		std::string s="{\"polls_failed\":"+std::to_string(polls_failed.load())
		             +",\"rcv_fails\":"+std::to_string(rcv_fails.load())
		             +",\"msgs_rcvd\":"+std::to_string(msgs_rcvd.load())
		             +",\"bad_msgs\":"+std::to_string(bad_msgs.load())
		             +",\"in_buffer_transfers\":"+std::to_string(in_buffer_transfers.load())
		             +",\"thread_crashes\":"+std::to_string(thread_crashes.load())
		             +"}";
		
		return s;
	}
};

#endif
