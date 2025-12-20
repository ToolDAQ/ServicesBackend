#ifndef WriteWorkers_H
#define WriteWorkers_H

#include <iostream>

#include "Tool.h"
#include "DataModel.h"
#include "WriteWorkerMonitoring.h"

/**
* \class WriteWorkers
*
* This Tool uses a worker pool to process write queries, converting received messages (structs encapsulating batches of zmq::message_t) into a format suitable for the DatabaseWorkers (array of JSONs).
*
* $Author: M. O'Flaherty $
* $Date: 2025/12/04 $
* Contact: marcus.o-flaherty@warwick.ac.uk
*/

// class for things passed to multicast worker threads
struct WriteJobStruct {
	
	WriteJobStruct(Pool<WriteJobStruct>* pool, DataModel* data, WriteWorkerMonitoring* mon) : m_pool(pool), m_data(data), monitoring_vars(mon){};
	DataModel* m_data;
	WriteWorkerMonitoring* monitoring_vars;
	Pool<WriteJobStruct>* m_pool;
	std::string m_job_name;
	QueryBatch* local_msg_queue;
	std::string* out_buffer;
	
};

struct WriteJobDistributor_args : Thread_args {
	
	DataModel* m_data;
	WriteWorkerMonitoring* monitoring_vars;
	std::string m_job_name;
	std::vector<QueryBatch*> local_msg_queue;       // swap with datamodel and then pass out to jobs
	// maybe we can use shared_ptr<void> instead of a job args pool? - only useful for jobs retaining their args,
	// i.e. job queues of a single type of job.
	Pool<WriteJobStruct> job_struct_pool{true, 1000, 100}; ///< pool for job args structs // FIXME default args
	
};

class WriteWorkers: public Tool {
	
	public:
	WriteWorkers(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Execute function used to perform Tool purpose.
	bool Finalise(); ///< Finalise function used to clean up resources.
	
	private:
	static void Thread(Thread_args* args);
	WriteJobDistributor_args thread_args; ///< args for the child thread that makes jobs for the job queue
	WriteWorkerMonitoring monitoring_vars;
	
	static bool WriteMessageJob(void*& arg);
	static void WriteMessageFail(void*& arg);
	
};

#endif
