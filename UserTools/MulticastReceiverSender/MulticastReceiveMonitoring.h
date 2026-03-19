#ifndef MulticastReceiveMonitoring_H
#define MulticastReceiveMonitoring_H

#include "MonitoringVariables.h"

class MulticastReceiveMonitoring : public MonitoringVariables {
	public:
	MulticastReceiveMonitoring(){};
	~MulticastReceiveMonitoring(){};
	
	std::atomic<int> polls_failed; // error polling socket
	std::atomic<int> rcv_fails;  // error in recv_from
	std::atomic<int> send_fails;  // error in send_to
	std::atomic<int> msgs_rcvd; // messages successfully received
	std::atomic<int> msgs_sent; // messages successfully received
	std::atomic<int> bytes_received;
	std::atomic<int> bytes_sent;
	std::atomic<int> in_buffer_transfers; // transfers of thread-local message vector to datamodel
	std::atomic<int> out_buffer_transfers; // transfers of thread-local message vector to datamodel
	std::atomic<int> thread_crashes; // restarts of tool worker thread (main thread found reader thread 'running' was false)
	
	std::string toJSON(){
		
		std::string s="{\"polls_failed\":"+std::to_string(polls_failed.load())
		             +",\"rcv_fails\":"+std::to_string(rcv_fails.load())
		             +",\"send_fails\":"+std::to_string(send_fails.load())
		             +",\"msgs_rcvd\":"+std::to_string(msgs_rcvd.load())
		             +",\"msgs_sent\":"+std::to_string(msgs_sent.load())
		             +",\"bytes_received\":"+std::to_string(bytes_received.load())
		             +",\"bytes_sent\":"+std::to_string(bytes_sent.load())
		             +",\"in_buffer_transfers\":"+std::to_string(in_buffer_transfers.load())
		             +",\"out_buffer_transfers\":"+std::to_string(out_buffer_transfers.load())
		             +",\"thread_crashes\":"+std::to_string(thread_crashes.load())
		             +"}";
		
		return s;
	}
};

#endif
