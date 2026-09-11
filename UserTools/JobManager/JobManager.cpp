#include "JobManager.h"

JobManager::JobManager():Tool(){}


bool JobManager::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	logger = m_data->logger;
	//m_variables.Print();
	
	// FIXME add to other Tools
	LoadConfig();
	
	m_data->num_threads=0; // tracker
	worker_pool_manager= new WorkerPoolManager(m_data->job_queue, &m_thread_cap, &(m_data->thread_cap), &(m_data->num_threads), nullptr, self_serving);
	
	ExportConfiguration();
	
	// monitoring struct to encapsulate tracking info
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.emplace(m_tool_name, &monitoring_vars);
	
	last_exec = std::chrono::steady_clock::now();
	
	return true;
}


bool JobManager::Execute(){
	
	// TODO add this to other Tools?
	if(m_data->change_config){
		InitialiseConfiguration(m_configfile);
		LoadConfig();
		ExportConfiguration();
	}
	
	auto time_now = std::chrono::steady_clock::now();
	double ms_since_last = std::chrono::duration_cast<std::chrono::milliseconds>(time_now-last_exec).count();
	if(ms_since_last < 1000) return true;
	last_exec = time_now;
	
	monitoring_vars.Set("pool_threads",worker_pool_manager->NumThreads());
	monitoring_vars.Set("queued_jobs",m_data->job_queue.size());
	worker_pool_manager->GetStats(monitoring_vars.vars);
	
	if(worker_pool_manager->NumThreads()==m_thread_cap) std::cerr<<"Warning: Worker Pool Threads Maxed"<<std::endl;
	
//	printf("%-20s\tqueued jobs:%d\tactive threads: %d\n",m_tool_name.c_str(), m_data->job_queue.size(), worker_pool_manager->NumThreads());
	//worker_pool_manager->PrintStats();  // print queued jobs, total workers, per job breakdown etc. trailing blank line...
	//printf("printing stats\n");
	//m_data->job_queue.Print();
	//printf("done printing stats\n");
	
	return true;
}


bool JobManager::Finalise(){
	
	delete worker_pool_manager;
	worker_pool_manager=nullptr;
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	return true;
}


// FIXME add to other Tools
void JobManager::LoadConfig(){
	if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;
	if(!m_variables.Get("thread_cap",m_thread_cap)) m_thread_cap = double(std::thread::hardware_concurrency())*0.8;
	if(!m_variables.Get("global_thread_cap",m_data->thread_cap)) m_data->thread_cap = m_thread_cap;
	if(!m_variables.Get("self_serving", self_serving)) self_serving = true;
	return;
}

