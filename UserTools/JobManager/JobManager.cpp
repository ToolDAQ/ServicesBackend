#include "JobManager.h"

JobManager::JobManager():Tool(){}


bool JobManager::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	//m_variables.Print();
	
	// FIXME add to other Tools
	LoadConfig();
	
	m_data->num_threads=0; // tracker
	worker_pool_manager= new WorkerPoolManager(m_data->job_queue, &m_thread_cap, &(m_data->thread_cap), &(m_data->num_threads), nullptr, self_serving);
	
	// FIXME add to other Tools
	ExportConfiguration();
	
	return true;
}


bool JobManager::Execute(){
	
	// TODO add this to other Tools?
	if(m_data->change_config){
		InitialiseConfiguration(m_configfile);
		LoadConfig();
		ExportConfiguration();
	}
	
	/* TODO
	m_data->monitoring_store_mtx.lock();
	m_data->monitoring_store.Set("pool_threads",worker_pool_manager->NumThreads());
	m_data->monitoring_store.Set("queued_jobs",m_data->job_queue.size());
	m_data->monitoring_store_mtx.unlock();
	// printf("jobmanager q:t = %d:%d\n", m_data->job_queue.size(), worker_pool_manager->NumThreads());
	usleep(1000);
	sleep(5);
	worker_pool_manager->PrintStats();
	printf("buffersize %u\n", m_data->aggrigation_buffer.size());
	if(worker_pool_manager->NumThreads()==m_thread_cap)  m_data->services->SendLog("Warning: Worker Pool Threads Maxed" , 0); //make this a warning
	std::cout<<"globalThreads="<<m_data->num_threads<<std::endl;
	*/
	
	return true;
}


bool JobManager::Finalise(){
	
	delete worker_pool_manager;
	worker_pool_manager=nullptr;
	m_data->num_threads--;
	
	return true;
}


// FIXME add to other Tools
void JobManager::LoadConfig(){
	if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;
	if(!m_variables.Get("thread_cap",m_thread_cap)) m_thread_cap = double(std::thread::hardware_concurrency())*0.8;
	if(!m_variables.Get("global_thread_cap",m_data->thread_cap)) m_data->thread_cap = m_thread_cap;
	if(!m_variables.Get("self_serving", self_serving) self_serving = true;
	return;
}

