#ifndef MulticastWorkerMonitoring_H
#define MulticastWorkerMonitoring_H

#include "MonitoringVariables.h"

class MulticastWorkerMonitoring : public MonitoringVariables {
	public:
	MulticastWorkerMonitoring(){};
	~MulticastWorkerMonitoring(){};
	
	std::atomic<int> jobs_failed;
	std::atomic<int> jobs_completed;
	std::atomic<int> msgs_processed; // each job concatenates a batch of messages; this sums all batches
	std::atomic<int> logs_processed;
	std::atomic<int> mons_processed;
	std::atomic<int> logging_bytes_processed;
	std::atomic<int> monitoring_bytes_processed;
	std::atomic<int> bytes_processed;
	std::atomic<int> thread_crashes; // restarts of tool worker thread (main thread found reader thread 'running' was false)
	
	std::string toJSON(){
		
		std::string s="{\"jobs_failed\":"+std::to_string(jobs_failed.load())
		             +",\"jobs_completed\":"+std::to_string(jobs_completed.load())
		             +",\"msgs_processed\":"+std::to_string(msgs_processed.load())
		             +",\"bytes_processed\":"+std::to_string(bytes_processed.load())
		             +",\"logs_processed\":"+std::to_string(logs_processed.load())
		             +",\"logging_bytes_processed\":"+std::to_string(logging_bytes_processed.load())
		             +",\"mons_processed\":"+std::to_string(mons_processed.load())
		             +",\"monitoring_bytes_processed\":"+std::to_string(monitoring_bytes_processed.load())
		             +",\"thread_crashes\":"+std::to_string(thread_crashes.load())
		             +"}";
		
		return s;
	}
};

#endif
