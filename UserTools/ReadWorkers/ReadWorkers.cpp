#include "ReadWorkers.h"

ReadWorkers::ReadWorkers():Tool(){}


bool ReadWorkers::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	//m_variables.Print();
	
	if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;
	
	ExportConfiguration();
	
	// monitoring struct to encapsulate tracking info
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.emplace(m_tool_name, &monitoring_vars);
	
	thread_args.m_data = m_data;
	thread_args.monitoring_vars = &monitoring_vars;
	if(!m_data->utils.CreateThread("read_job_distributor", &Thread, &thread_args)){
		Log("Failed to spawn background thread",v_error,m_verbose);
		return false;
	}
	m_data->num_threads++;
	
	return true;
}


bool ReadWorkers::Execute(){
	
	// FIXME ok but actually this kills all our jobs, not just our job distributor
	// so we don't want to do that.
	if(!thread_args.running){
		Log("Execute found thread not running!",v_error);
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++(monitoring_vars.thread_crashes);
	}
	
	return true;
}


bool ReadWorkers::Finalise(){
	
	// signal job distributor thread to stop
	Log("Joining job distributor thread",v_warning);
	m_data->utils.KillThread(&thread_args);
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	Log("Finished",v_warning);
	return true;
}


void ReadWorkers::Thread(Thread_args* args){
	
	ReadJobDistributor_args* m_args = dynamic_cast<ReadJobDistributor_args*>(args);
	m_args->local_msg_queue.clear();
	
	// grab a batch of read queries
	std::unique_lock<std::mutex> locker(m_args->m_data->read_msg_queue_mtx);
	if(!m_args->m_data->read_msg_queue.empty()){
		std::swap(m_args->m_data->read_msg_queue, m_args->local_msg_queue);
	} else {
		locker.unlock();
		usleep(100);
		return;
	}
	locker.unlock();
	
	// add a job for each batch to the queue
	for(int i=0; i<m_args->local_msg_queue.size(); ++i){
		
		// add a new Job to the job queue to process this data
		Job* the_job = m_args->m_data->job_pool.GetNew("read_worker");
		the_job->out_pool = &m_args->m_data->job_pool;
		if(the_job->data == nullptr){
			// on first creation of the job, make it a JobStruct to encapsulate its data
			// N.B. Pool::GetNew will only invoke the constructor if this is a new instance,
			// (not if it's been used before and then returned to the pool)
			// so don't pass job-specific variables to the constructor
			the_job->data = m_args->job_struct_pool.GetNew(&m_args->job_struct_pool, m_args->m_data, m_args->monitoring_vars);
		} else {
			// this should never happen as jobs should return their args to the pool
			std::cerr<<"ReadWorker Job with non-null data pointer!"<<std::endl;
			// FIXME ... do we assume this job args object is valid, and use it?
			// this could lead to a segfault (if the args got returned to the pool and deleted)
			// or corruption (if the args got returned to the pool and given to another job)
			// alternatively do we just over-Read the job pointer with new args (potentially leaking it)
		}
		ReadJobStruct* job_data = static_cast<ReadJobStruct*>(the_job->data);
		job_data->local_msg_queue = m_args->local_msg_queue[i];
		job_data->m_job_name = "read_worker";
		
		//printf("spawning %s job\n", job_data->m_job_name.c_str());
		the_job->func = ReadMessageJob;
		the_job->fail_func = ReadMessageFail;
		
		m_args->m_data->job_queue.AddJob(the_job);
		//job_data->local_msg_queue->push_time("read_job_push");
		
	}
	
	return;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

void ReadWorkers::ReadMessageFail(void*& arg){
	
	// safety check in case the job somehow fails after returning its args to the pool
	if(arg==nullptr){
		std::cerr<<"multicast worker fail with no args"<<std::endl;
		return; // FIXME log this occurrence?
	}
	
	// FIXME do something here
	// if there were preceding messages that were succesfully added
	// we could try to insert the current buffers so that those get processed.
	// but we don't know where we failed, so that could be risky if the buffer is corrupt?
	// we could keep track of where we were in m_args and:
	// 1. log the specific message we were trying to process when the job failed
	// 2. submit the data we already have
	// 3. make a new job for the remaining data
	// this probably seems better, but be careful not to get stuck in a fail loop
	// if the problem isn't the query
	
	// at minimum we need to pass our vector<ZmqQuery> back somewhere for the failures
	// to be reported to the clients
	//m_args->m_data->query_buffer_pool.Add(m_args->msg_buffer);  << FIXME not back to the pool but reply queue
	
	ReadJobStruct* m_args=static_cast<ReadJobStruct*>(arg);
	std::cerr<<m_args->m_job_name<<" failure"<<std::endl;
	++(m_args->monitoring_vars->jobs_failed);
	
	// return our job args to the pool
	m_args->m_pool->Add(m_args);
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	return;
}

bool ReadWorkers::ReadMessageJob(void*& arg){
	
	ReadJobStruct* m_args = static_cast<ReadJobStruct*>(arg);
	
	thread_local std::unique_ptr<ZSTD_DCtx,long unsigned int(*)(ZSTD_DCtx*)> zstd_ctx(ZSTD_createDCtx(), ZSTD_freeDCtx);
	
	//m_args->local_msg_queue->push_time("ReadWorker_start");
	
	//printf("%s job processing %d queries\n", m_args->m_job_name.c_str(), m_args->local_msg_queue->queries.size());
	
	// pull next query from batch
	for(size_t i=0; i<m_args->local_msg_queue->queries.size(); ++i){
		
		ZmqQuery& query = m_args->local_msg_queue->queries[i];
		
		if(query.compressed()){
		        // compressed - decompress it
		        m_args->decompressed_bytes = ZSTD_getFrameContentSize(query.msg_raw().data(), query.msg_raw().size());
		        if(m_args->decompressed_bytes==ZSTD_CONTENTSIZE_UNKNOWN || m_args->decompressed_bytes==ZSTD_CONTENTSIZE_ERROR){
		                // bad message, discard // FIXME log it
		                printf("%s ignoring zstd bad read message '%.*s'\n",m_args->m_job_name.c_str(), query.msg_raw().size(), query.msg_raw().data());
		                query.err="bad zstd size";
		                continue;
		        }
		        if(m_args->decompressed_bytes > MAX_DECOMPRESSED_MSG_SIZE){
		                printf("%s ignoring zstd message requesting excessive '%lu' byte decompression buffer\n",
		                       m_args->m_job_name.c_str(), m_args->decompressed_bytes);
		                query.err = "zstd too large decompressed size";
		                continue;
		        }
		        query.decompress_buffer.resize(m_args->decompressed_bytes);
		        m_args->decompressed_bytes = ZSTD_decompressDCtx(zstd_ctx.get(),(void*)query.decompress_buffer.data(),m_args->decompressed_bytes, query.msg_raw().data(), query.msg_raw().size());
		         if(ZSTD_isError(m_args->decompressed_bytes)){
		                printf("%s error decompressing zstd message from %.*s: %s\n", // FIXME log these
		                       m_args->m_job_name.c_str(), query.client_id().size(), query.client_id().data(), ZSTD_getErrorName(m_args->decompressed_bytes));
		                query.err = "zstd decompression error";
		                continue;
		         }
		}
		// XXX 
		//printf("ReadWorker processing %.*s query '%.*s'\n",query.topic().size(),query.topic().data(),query.msg().size(), query.msg().data());
		
		++(m_args->monitoring_vars->msgs_processed);
		
	}
	
	//m_args->local_msg_queue->push_time("ReadWorker_done");
	
	// pass the batch onto the next stage of the pipeline for the DatabaseWorkers
	std::unique_lock<std::mutex> locker(m_args->m_data->read_query_queue_mtx);
	m_args->m_data->read_query_queue.push_back(m_args->local_msg_queue);
	locker.unlock();
	
	//printf("%s queueing processed querybatch\n",m_args->m_job_name.c_str());
	++(m_args->monitoring_vars->jobs_completed);
	
	// return our job args to the pool
	m_args->m_pool->Add(m_args);  // return our job args to the job args struct pool
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	return true;
}


