#ifndef ResultWorkers_H
#define ResultWorkers_H

#include <iostream>
#include <sstream>

#include "Tool.h"
#include "DataModel.h"
#include "ResultWorkerMonitoring.h"

/**
* \class ResultWorkers
*
* This Tool spawns jobs that convert pqxx::result objects from read queries into zmq::message_t objects ready for sending back to clients
*
* $Author: Marcus O'Flaherty $
* $Date: 2025/12/10 $
* Contact: marcus.o-flaherty@warwick.ac.uk
*/

// class for things passed to result worker threads
struct ResultJobStruct {
	
	ResultJobStruct(Pool<ResultJobStruct>* pool, DataModel* data, ResultWorkerMonitoring* mon) : m_pool(pool), m_data(data), monitoring_vars(mon){};
	DataModel* m_data;
	ResultWorkerMonitoring* monitoring_vars;
	Pool<ResultJobStruct>* m_pool;
	std::string m_job_name;
	QueryBatch* batch;
	std::stringstream ss;
	std::string tmpval;
	
};

struct ResultJobDistributor_args : Thread_args {
	
	DataModel* m_data;
	ResultWorkerMonitoring* monitoring_vars;
	std::vector<QueryBatch*> local_msg_queue;       // swap with datamodel and then pass out to jobs
	Pool<ResultJobStruct> job_struct_pool{true, 1000, 100}; ///< pool for job args structs // FIXME default args
	
};

class ResultWorkers: public Tool {
	
	public:
	ResultWorkers(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Execute function used to perform Tool purpose.
	bool Finalise(); ///< Finalise funciton used to clean up resources.
	
	private:
	static void Thread(Thread_args* args);
	ResultJobDistributor_args thread_args; ///< args for the child thread that makes jobs for the job queue
	ResultWorkerMonitoring monitoring_vars;
	
	static bool ResultJob(void*& arg);
	static void ResultJobFail(void*& args);
	
};

#endif
