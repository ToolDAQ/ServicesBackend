#ifndef MulticastWorkers_H
#define MulticastWorkers_H

#include <iostream>
#include <zstd.h>

#include "Tool.h"
#include "DataModel.h"
#include "MulticastWorkerMonitoring.h"

/**
* \class MulticastWorkers
*
* This Tool uses a worker pool to process batches of multicast messages (received in JSON format), separates them based on their topic (i.e. destination table) and prepares them for insertion into the database by database workers. This preparation may include batching messages, decoding the JSON into SQL, extraction of JSON variables into parameter packs, etc. Presently, it batches the JSON for use with postgres json_to_recordset.
*
* $Author: M. O'Flaherty $
* $Date: 2025/12/04 $
* Contact: marcus.o-flaherty@warwick.ac.uk
*/

// class for things passed to multicast worker threads
struct MulticastJobStruct {
	
	MulticastJobStruct(Pool<MulticastJobStruct>* pool, DataModel* data, MulticastWorkerMonitoring* mon) : m_pool(pool), m_data(data), monitoring_vars(mon){};
	DataModel* m_data;
	MulticastWorkerMonitoring* monitoring_vars;
	Pool<MulticastJobStruct>* m_pool;
	std::string m_job_name;
	std::vector<std::string>* msg_buffer;
	std::string* logging_buffer;
	std::string* monitoring_buffer;
	std::string* rootplot_buffer;
	std::string* plotlyplot_buffer;
	std::string* out_buffer;
	int n_log_msgs;
	int n_mon_msgs;
	size_t decompressed_bytes;
	std::string decompress_buffer;
	std::string_view the_msg;
	
};

struct MulticastJobDistributor_args : Thread_args {
	
	DataModel* m_data;
	MulticastWorkerMonitoring* monitoring_vars;
	std::vector<std::vector<std::string>*> local_msg_queue; // swap with datamodel and then pass out to jobs
	Pool<MulticastJobStruct> job_struct_pool{true, 1000, 100}; ///< pool for job objects used by worker threads
	
};

class MulticastWorkers: public Tool {
	
	public:
	MulticastWorkers(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Executre function used to perform Tool perpose. 
	bool Finalise(); ///< Finalise funciton used to clean up resorces.
	
	private:
	MLogger* logger;
	static void Thread(Thread_args* args); ///< job distributor thread function that pulls batches of multicast messages from upstream and passes them to the job queue
	MulticastJobDistributor_args thread_args; ///< args for the child thread that produces and distributes jobs to the worker farm
	MulticastWorkerMonitoring monitoring_vars;
	
	static bool MulticastMessageJob(void*& arg); ///< job function that prepares a batch of multicast messages for DB entry
	static void MulticastMessageFail(void*& arg); ///< job fail function, perform cleanup to return multicast buffer and job args struct to their respective Pools
	std::chrono::time_point<std::chrono::steady_clock> last_exec;
	
	// for now use shared ones in datamodel
	//WorkerPoolManager* job_manager=nullptr; ///< manager for worker farm, has internal background thread that spawns new jobs and or prunes them, along with tracking statistics
	//JobQueue multicast_jobs; ///< job queue for worker farm
	
};



#endif
