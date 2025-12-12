#ifndef DatabaseWorkers_H
#define DatabaseWorkers_H

#include <iostream>

#include "Tool.h"
#include "DataModel.h"
#include "WorkerPoolManager.h"


/**
* \class DatabaseWorkers
*
* This Tool manages a pool of workers, each with a connection to the backend database, to run the queries.
*
* $Author: M. O'Flaherty $
* $Date: 2025/12/08 $
* Contact: marcus.o-flaherty@warwick.ac.uk
*/

struct DatabaseJobDistributor_args {
	DataModel* m_data;
	Postgres* m_database;
	Pool<DatabaseJobStruct> job_struct_pool;
	
};

struct DatabaseJobStruct {
	
	DatabaseJobStruct(Pool<DatabaseJobStruct>* pool, DataModel* data) : m_pool(pool) m_data(data){};
	DataModel* m_data;
	Pool<DatabaseJobStruct>* m_pool;
	
	std::string connection_string;
	std::vector<std::vector<std::string>*> local_multicast_queue;
	
	std::vector<QueryBatch*> read_queue;
	std::vector<QueryBatch*> write_queue;
	std::vector<std::string> logging_queue;
	std::vector<std::string> monitoring_queue;
	std::vector<std::string> rootplot_queue;
	std::vector<std::string> plotlyplot_queue;
	
};

class DatabaseWorkers: public Tool {
	
	public:
	DatabaseWorkers(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Execute function used to perform Tool purpose.
	bool Finalise(); ///< Finalise function used to clean up resources.
	
	private:
	static void Thread(Thread_args* args);
	DatabaseJobDistributor_args thread_args;
	
	WorkerPoolManager* job_manager=nullptr; ///< manager for worker farm, has internal background thread that spawns new jobs and or prunes them, along with tracking statistics
	JobQueue database_jobqueue; ///< job queue for worker farm
	
	static void DatabaseJob(void*& arg);
	static void DatabaseJobFail(void*& args);
	
};


#endif
