#ifndef ReadWorkers_H
#define ReadWorkers_H

#include <iostream>

#include "Tool.h"
#include "DataModel.h"
#include "ReadWorkerMonitoring.h"

/**
* \class ReadWorkers
*
* This Tool uses a worker pool to process read queries, converting received messages (structs encapsulating batches of zmq::message_t) into a format suitable for the DatabaseWorkers (array of JSONs).
*
* $Author: M. O'Flaherty $
* $Date: 2025/12/04 $
* Contact: marcus.o-flaherty@warwick.ac.uk
*/

// class for things passed to multicast worker threads
struct ReadJobStruct {
	
	ReadJobStruct(Pool<ReadJobStruct>* pool, DataModel* data, ReadWorkerMonitoring* mon) : m_pool(pool), m_data(data), monitoring_vars(mon){};
	DataModel* m_data;
	ReadWorkerMonitoring* monitoring_vars;
	Pool<ReadJobStruct>* m_pool;
	std::string m_job_name;
	QueryBatch* local_msg_queue;
	std::string* out_buffer;
	size_t decompressed_bytes;
	std::string decompress_buffer;
	std::string_view the_msg;
	
};

struct ReadJobDistributor_args : Thread_args {
	
	DataModel* m_data;
	ReadWorkerMonitoring* monitoring_vars;
	std::string m_job_name;
	std::vector<QueryBatch*> local_msg_queue;       // swap with datamodel and then pass out to jobs
	// maybe we can use shared_ptr<void> instead of a job args pool? - only useful for jobs retaining their args,
	// i.e. job queues of a single type of job.
	Pool<ReadJobStruct> job_struct_pool{true, 1000, 100}; ///< pool for job args structs // FIXME default args
	
};

class ReadWorkers: public Tool {
	
	public:
	ReadWorkers(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Execute function used to perform Tool purpose.
	bool Finalise(); ///< Finalise function used to clean up resources.
	
	private:
	static void Thread(Thread_args* args);
	ReadJobDistributor_args thread_args; ///< args for the child thread that makes jobs for the job queue
	ReadWorkerMonitoring monitoring_vars;
	
	static bool ReadMessageJob(void*& arg);
	static void ReadMessageFail(void*& arg);
	
};

#endif
