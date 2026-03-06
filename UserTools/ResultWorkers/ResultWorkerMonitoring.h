#ifndef ResultWorkerMonitoring_H
#define ResultWorkerMonitoring_H

#include "MonitoringVariables.h"

class ResultWorkerMonitoring : public MonitoringVariables {
	public:
	ResultWorkerMonitoring(){};
	~ResultWorkerMonitoring(){};
	
	std::atomic<int> read_batches_processed;
	std::atomic<int> write_batches_processed;
	std::atomic<int> jobs_failed;
	std::atomic<int> jobs_completed;
	std::atomic<int> result_access_errors;
	
	std::atomic<int> thread_crashes; // restarts of tool worker thread (main thread found reader thread 'running' was false)
	
	std::string toJSON(){
		
		std::string s="{\"read_batches_processed\":"+std::to_string(read_batches_processed.load())
		             +",\"write_batches_processed\":"+std::to_string(write_batches_processed.load())
		             +",\"result_access_errors\":"+std::to_string(result_access_errors.load())
		             +",\"jobs_completed\":"+std::to_string(jobs_completed.load())
		             +",\"jobs_failed\":"+std::to_string(jobs_failed.load())
		             +",\"thread_crashes\":"+std::to_string(thread_crashes.load())
		             +"}";
		
		return s;
	}
};

#endif
