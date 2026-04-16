#ifndef DatabaseWorkers_H
#define DatabaseWorkers_H

#include <iostream>
#include <set>
#include <chrono>

#include "Tool.h"
#include "DataModel.h"
#include "WorkerPoolManager.h"
#include "DatabaseWorkerMonitoring.h"

/**
* \class DatabaseWorkers
*
* This Tool manages a pool of workers, each with a connection to the backend database, to run the queries.
*
* $Author: M. O'Flaherty $
* $Date: 2025/12/08 $
* Contact: marcus.o-flaherty@warwick.ac.uk
*/

enum class DatabaseJobStep { generics, logging, monitoring, rootplots, plotlyplots, writes, finish };

struct DatabaseJobStruct {
	
	DatabaseJobStruct(Pool<DatabaseJobStruct>* pool, DataModel* data, DatabaseWorkerMonitoring* mon) : m_pool(pool), m_data(data), monitoring_vars(mon){};
	DataModel* m_data;
	DatabaseWorkerMonitoring* monitoring_vars;
	Pool<DatabaseJobStruct>* m_pool;
	std::string m_job_name;
	
	std::vector<QueryBatch*> read_queue;
	std::vector<QueryBatch*> write_queue;
	std::vector<std::string*> logging_queue;
	std::vector<std::string*> monitoring_queue;
	std::vector<std::string*> rootplot_queue;
	std::vector<std::string*> plotlyplot_queue;
	
	std::set<int16_t> bad_logs;
	std::set<int16_t> bad_mons;
	std::set<int16_t> bad_rootplots;
	std::set<int16_t> bad_plotlyplots;
	
	uint16_t last_i;
	
	std::vector<pqxx::pipeline::query_id> ids;
	bool pipeline_error;
	
	bool had_error;
	DatabaseJobStep checkpoint;
	DatabaseJobStep endpoint;
	size_t checkpoint_i;
	size_t checkpoint_j;
	size_t endpoint_i;
	size_t endpoint_j;
	
	void clear(){
		read_queue.clear();
		write_queue.clear();
		logging_queue.clear();
		monitoring_queue.clear();
		rootplot_queue.clear();
		plotlyplot_queue.clear();
		
		bad_logs.clear();
		bad_mons.clear();
		bad_rootplots.clear();
		bad_plotlyplots.clear();
		
		had_error=false;
		endpoint=DatabaseJobStep::finish;
		
	}
	
};

struct DatabaseJobDistributor_args : Thread_args {
	DataModel* m_data;
	DatabaseWorkerMonitoring* monitoring_vars;
	Pool<DatabaseJobStruct> job_struct_pool;
	JobQueue* job_queue;
	Job* the_job = nullptr;
	
};

class DatabaseWorkers: public Tool {
	
	public:
	DatabaseWorkers(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Execute function used to perform Tool purpose.
	bool Finalise(); ///< Finalise function used to clean up resources.
	
	private:
	static void Thread(Thread_args* args);
	bool CacheConfigs(const char* alertname, const char* payload); ///< alert to cache configs for upcoming config change
	std::string CacheConfigs(const char* arg); ///< slowcontrol to cache configs for upcoming config change
	std::string GetCachedConfigs(const char* arg=nullptr); ///< get cached config numbers
	DatabaseJobDistributor_args thread_args;
	DatabaseWorkerMonitoring monitoring_vars;
	
	WorkerPoolManager* job_manager=nullptr; ///< manager for worker farm, has internal background thread that spawns new jobs and or prunes them, along with tracking statistics
	JobQueue database_jobqueue; ///< job queue for worker farm
	
	unsigned int max_workers; // for some reason workerpoolmanager only takes a pointer to this, not a copy
	static std::string connection_string;
	
	static bool DatabaseJob(void*& arg);
	static void DatabaseJobFail(void*& args);
	
	std::chrono::time_point<std::chrono::steady_clock> last_exec;
	
	int m_base_config_id;
	int m_runmode_config_id;
	
};

#endif
