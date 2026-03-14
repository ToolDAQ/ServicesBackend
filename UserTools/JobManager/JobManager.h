#ifndef JobManager_H
#define JobManager_H

#include <iostream>
#include <chrono>

#include "Tool.h"
#include "DataModel.h"
#include "WorkerPoolManager.h"
#include "JobManagerMonitoring.h"

/**
* \class JobManager
*
* This Tool instantiates a WorkerPoolManager to manage the numer of worker threads for processing multicast messages, write queries and responses.
*
* $Author: Marcus O'Flaherty $
* $Date: 2025/12/10 $
* Contact: marcus.o-flaherty@warwick.ac.uk
*/

class JobManager: public Tool {
	
	public:
	JobManager(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Execute function used to purform Tool purpose.
	bool Finalise(); ///< Finalise funciton used to clean up resources.
	
	private:
	bool self_serving;
	unsigned int m_thread_cap;
	WorkerPoolManager* worker_pool_manager;
	JobManagerMonitoring monitoring_vars;
	std::chrono::time_point<std::chrono::steady_clock> last_exec;
	
	std::string m_configfile;
	void LoadConfig();
	
};


#endif
