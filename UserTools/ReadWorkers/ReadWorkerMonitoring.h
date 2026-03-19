#ifndef ReadWorkerMonitoring_H
#define ReadWorkerMonitoring_H

#include "MonitoringVariables.h"

class ReadWorkerMonitoring : public MonitoringVariables {
	public:
	ReadWorkerMonitoring(){};
	~ReadWorkerMonitoring(){};
	
	std::atomic<int> jobs_failed;
	std::atomic<int> jobs_completed;
	std::atomic<int> msgs_processed; // each job concatenates a batch of messages; this sums all batches
	std::atomic<int> thread_crashes; // restarts of tool worker thread (main thread found reader thread 'running' was false)
	
	std::string toJSON(){
		
		std::string s="{\"jobs_failed\":"+std::to_string(jobs_failed.load())
		             +",\"jobs_completed\":"+std::to_string(jobs_completed.load())
		             +",\"msgs_processed\":"+std::to_string(msgs_processed.load())
		             +",\"thread_crashes\":"+std::to_string(thread_crashes.load())
		             +"}";
		
		return s;
	}
};

#endif
