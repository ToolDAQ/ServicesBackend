#include "WriteWorkers.h"

WriteWorkers::WriteWorkers():Tool(){}


bool WriteWorkers::Initialise(std::string configfile, DataModel &data){
	
	if(configfile!="")  m_variables.Initialise(configfile);
	//m_variables.Print();
	
	m_data= &data;
	m_log= m_data->Log;
	
	if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;
	
	thread_args.m_data = m_data;
	m_data->utils.CreateThread("write_job_distributor", &Thread, &thread_args);
	m_data->num_threads++;
	
	return true;
}


bool WriteWorkers::Execute(){
	
	// FIXME ok but actually this kills all our jobs, not just our job distributor
	// so we don't want to do that.
	if(!thread_args.running){
		Log(m_tool_name+" Execute found thread not running!",v_error);
		Finalise();
		Initialise(); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++m_data->write_job_distributor_thread_crashes;
	}
	
	return true;
}


bool WriteWorkers::Finalise(){
	
	// signal job distributor thread to stop
	Log(m_tool_name+": Joining job distributor thread",v_warning);
	m_data->utils.KillThread(&thread_args);
	Log(m_tool_name+": Finished",v_warning);
	m_data->num_threads--;
	
	return true;
}


void WriteWorkers::Thread(Thread_args* args){
	
	WriteJobDistributor_args* m_args = reinterpret_cast<WriteJobDistributor_args*>(args);
	
	// grab a batch of write queries
	std::unique_lock<std::mutex> locker(m_args->m_data->write_msg_queue_mtx);
	if(!m_args->m_data->write_msg_queue.empty()){
		std::swap(m_args->m_data->write_msg_queue, m_args->local_msg_queue);
	}
	locker.unlock();
	
	// add a job for each batch to the queue
	for(int i=0; i<m_args->local_msg_queue.size(); ++i){
		
		// add a new Job to the job queue to process this data
		Job* the_job = m_args->m_data->job_pool.GetNew(&m_args->m_data->job_pool, "write_worker");
		if(the_job->data == nullptr){
			// on first creation of the job, make it a JobStruct to encapsulate its data
			// N.B. Pool::GetNew will only invoke the constructor if this is a new instance,
			// (not if it's been used before and then returned to the pool)
			// so don't pass job-specific variables to the constructor
			the_job->data = m_args->job_struct_pool.GetNew(&m_args->job_struct_pool, m_args->m_data);
		} else {
			// this should never happen as jobs should return their args to the pool
			std::cerr<<"WriteWorker Job with non-null data pointer!"<<std::endl;
			// FIXME ... do we assume this job args object is valid, and use it?
			// this could lead to a segfault (if the args got returned to the pool and deleted)
			// or corruption (if the args got returned to the pool and given to another job)
			// alternatively do we just over-write the job pointer with new args (potentially leaking it)
		}
		WriteJobStruct* job_data = dynamic_cast<WriteJobStruct*>(the_job->data);
		job_data->local_msg_queue = m_args->local_msg_queue[i];
		
		the_job->func = WriteMessageJob;
		the_job->fail_func = WriteMessageFail;
		
		/*ok =*/ m_args->m_data->job_queue.AddJob(the_job); // just checks if you've defined func and first_vals = true;
		
	}
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

void WriteWorkers::WriteMessageFail(void*& arg){
	
	// safety check in case the job somehow fails after returning its args to the pool
	if(arg==nullptr){
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
	
	WriteJobStruct* m_args=reinterpret_cast<WriteJobStruct*>(arg);
	++(*m_args->m_data->write_worker_job_fails);
	
	// return our job args to the pool
	m_args->m_pool.Add(m_args);
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
}

void WriteWorkers::WriteMessageJob(void*& arg){
	
	WriteJobStruct* m_args = reinterpret_cast<WriteJobStruct*>(arg);
	
	m_args->local_msg_queue.reset();
	
	// pull next query from batch
	for(size_t i=0; i<m_args->local_msg_queue->queries.size(); ++i){
		
		ZmqQuery& query = m_args->local_msg_queue->queries[i];
		
		// we can only batch queries destined for the same table,
		// so we need to split our messages up into different queues
		// (this also means we can prioritise high priority queries such as alarms)
		// we can do batch insertions with a 'returning version' statement to obtain
		// a multi-record response with all the corresponding version numbers: e.g.
		// INSERT INTO rootplots ( time, name, data ) SELECT * FROM jsonb_to_recordset
		// ('[ {"time":"2025-12-05 23:31", "name":"dev1", "data":{"message":"blah"} },
		//     {"time":"2025-12-05 23:25", "name":"dev2", "data":{"message":"argg"} } ]')
		// as t(time timestamptz, name text, data jsonb) returning version;"
		// as before, such batches need to be grouped according to destination table
		switch(query_topic{query.topic()[2]}){
			// FIXME switch letter to query_type enum class
			case query_topic::alarm: // ALARM
				// alarm insertions require no return value,
				// but we still need to send back an acknowledgement once the alarm is inserted
				m_args->out_buffer = m_args->local_msg_queue->alarm_buffer;
				break;
			case query_topic::dev_config: // DEVCONFIG
				m_args->out_buffer = m_args->local_msg_queue->devconfig_buffer;
				break;
			case query_topic::run_config: // RUNCONFIG
				m_args->out_buffer = m_args->local_msg_queue->runconfig_buffer;
				break;
			case query_topic::calibration: // CALIBRATION
				m_args->out_buffer = m_args->local_msg_queue->calibration_buffer;
				break;
			case query_topic::plotlyplot: // PLOTLYPLOT
				m_args->out_buffer = m_args->local_msg_queue->plotlyplot_buffer;
				break;
			case query_topic::rootplot: // TROOTPLOT (yeah the T is just so it's unique...)  FIXME maybe topic can just be a unique char
				m_args->out_buffer = m_args->local_msg_queue->rooplot_buffer;
				break;
			default:
				// FIXME unrecognised topic log it.
		}
		
		if(i!=0) m_args->out_buffer += ", ";
		m_args->out_buffer += query.msg();
		
	}
	
	// pass the batch onto the next stage of the pipeline for the DatabaseWorkers
	std::unique_lock<std::mutex> locker(m_args->m_data->write_query_queue_mtx);
	m_args->m_data->alarm_batch_queue.push_back(m_args->write_query_queue);
	locker.unlock();
	
	++(*m_args->m_data->write_worker_job_successes);
	
	// return our job args to the pool
	m_args->m_pool.Add(m_args);  // return our job args to the job args struct pool
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	return;
}


