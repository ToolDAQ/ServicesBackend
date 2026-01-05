#include "ResultWorkers.h"

ResultWorkers::ResultWorkers():Tool(){}


bool ResultWorkers::Initialise(std::string configfile, DataModel &data){
	
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
	if(!m_data->utils.CreateThread("result_job_distributor", &Thread, &thread_args)){
		Log(m_tool_name+": Failed to spawn background thread",v_error,m_verbose);
		return false;
	}
	m_data->num_threads++;
	
	return true;
}


bool ResultWorkers::Execute(){
	
	// FIXME ok but actually this kills all our jobs, not just our job distributor
	// so we don't want to do that.
	if(!thread_args.running){
		Log(m_tool_name+" Execute found thread not running!",v_error);
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++(monitoring_vars.thread_crashes);
	}
	
	return true;
}


bool ResultWorkers::Finalise(){
	
	// signal job distributor thread to stop
	Log(m_tool_name+": Joining receiver thread",v_warning);
	m_data->utils.KillThread(&thread_args);
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	Log(m_tool_name+": Finished",v_warning);
	return true;
}


void ResultWorkers::Thread(Thread_args* args){
	
	ResultJobDistributor_args* m_args = reinterpret_cast<ResultJobDistributor_args*>(args);
	
	// grab a batch of read queries, with results awaiting conversion
	std::unique_lock<std::mutex> locker(m_args->m_data->query_results_mtx);
	if(m_args->m_data->query_results.empty()) return;
	std::swap(m_args->m_data->query_results, m_args->local_msg_queue);
	locker.unlock();
	
	// add a job for each batch to the queue
	for(int i=0; i<m_args->local_msg_queue.size(); ++i){
		
		// add a new Job to the job queue to process this data
		Job* the_job = m_args->m_data->job_pool.GetNew("result_worker");
		the_job->out_pool = &m_args->m_data->job_pool;
		if(the_job->data == nullptr){
			// on first creation of the job, make it a JobStruct to encapsulate its data
			// N.B. Pool::GetNew will only invoke the constructor if this is a new instance,
			// (not if it's been used before and then returned to the pool)
			// so don't pass job-specific variables to the constructor
			the_job->data = m_args->job_struct_pool.GetNew(&m_args->job_struct_pool, m_args->m_data, m_args->monitoring_vars);
		} else {
			// FIXME error
			std::cerr<<"result_worker Job with non-null data pointer!"<<std::endl;
		}
		
		the_job->func = ResultJob;
		the_job->fail_func = ResultJobFail;
		
		ResultJobStruct* job_data = static_cast<ResultJobStruct*>(the_job->data);
		job_data->batch = m_args->local_msg_queue[i];
		job_data->m_job_name = "result_worker";
		
		m_args->m_data->job_queue.AddJob(the_job);
		
	}
	m_args->local_msg_queue.clear();
	
	// TODO add workers that also call setstatus  /setversion on batch jobs and then pass them to send thread?
	// maybe we can generalise to setreply if needed, depending on reply format & batching of read queries
	// or do we just do this in the connection / reply sender thread(s)?
	
	return;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

void ResultWorkers::ResultJobFail(void*& arg){
	
	// safety check in case the job somehow fails after returning its args to the pool
	if(arg==nullptr){
		std::cerr<<"multicast worker fail with no args"<<std::endl;
		return; // FIXME log this occurrence?
	}
	
	// FIXME hmm, well, i guess we say the query failed
	// - we had the results, but then lost them before sending
	
	ResultJobStruct* m_args=reinterpret_cast<ResultJobStruct*>(arg);
	std::cerr<<m_args->m_job_name<<" failure"<<std::endl;
	++(m_args->monitoring_vars->jobs_failed);
	
	// return our job args to the pool
	m_args->m_pool->Add(m_args);
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	return;
}

bool ResultWorkers::ResultJob(void*& arg){
	
	ResultJobStruct* m_args = reinterpret_cast<ResultJobStruct*>(arg);
	
	// for now each job processes a batch, not a set of batches
	//for(QueryBatch* batch : m_args->local_msg_queue){
		
		// read queries need to have their results interpreted,
		// write queries only need to have their insertion success status returned.
		// each batch should only contain messages that are either all read or all write.
		if(m_args->batch->queries.front().topic()[0]=='R'){
			
			// process batch of read queries
			
			for(ZmqQuery& query : m_args->batch->queries){
				
				// set whether the query succeeded or threw an exception
				if(query.result.query().empty()){  // FIXME not sure if this is a good check necessarily, esp w/pipelining?
					query.setsuccess(0);
					query.setresponserows(1);
					query.setresponse(0, query.err);
					
				} else {
					query.setsuccess(1);
					
					// returned rows are sent back formatted as JSON, with each row a new zmq::message_t
					// resize zmq vector in preparation
					query.setresponserows(std::size(query.result));
					
					if(query_topic{query.topic()[2]}!=query_topic::generic){
						
						// just for good measure, when we try to access the pqxx result,
						// enclose within try just in case it throws something
						try {
							// standard queries generated by the libDAQInterface use `row_to_json`
							// to request results already packaged up into one JSON per row
							// so all we need to do is copy that into the zmq message
							for(size_t i=0; i<std::size(query.result); ++i){
								query.setresponse(i, query.result[i][0].c_str());
							}
						} catch (std::exception& e){
							// just for good measure, when we try to access the pqxx result,
							// enclose within try just in case it throws something
							std::cerr<<"caught "<<e.what()<<" trying to access query result!"<<std::endl;
							query.setsuccess(0);
							query.setresponserows(0);
							++(m_args->monitoring_vars->result_access_errors);
						}
						
					} else {
						
						// just for good measure, when we try to access the pqxx result,
						// enclose within try just in case it throws something
						try {
							// TODO if we can safely shoehorn in a wrapping call to `row_to_json`
							// around a user's generic sql, we can combine this with the above.
							// But, given the arbitrary complexity of statements, this may not be possible.
							// in which case, we need to loop over rows and convert them to JSON manually
							for(size_t i=0; i<std::size(query.result); ++i){
								
								// build a json from fields in this row
								m_args->tmpval = "{";
								for (pqxx::row::iterator it=query.result[i].begin(); it<query.result[i].end(); ++it){
									if(it!=query.result[i].begin()) m_args->tmpval += ", ";
									m_args->tmpval += "\"" + std::string{it->name()} + "\":";
									// Field values are returned bare: i.e. '3' or 'cat' or '{"iam":"ajson"}'
									// but to convert this into JSON, strings need to be quoted:
									// i.e. { "field1":3, "field2":"cat", "field3":{"iam":"ajson"} }
									// this means we need to add enclosing quotes *only* for string fields
									if((it->type()==18) || (it->type()==25) || (it->type()==1042) || (it->type()==1043)){
										m_args->tmpval += "\""+std::string{it->c_str()}+"\"";
									} else {
										m_args->tmpval += it->c_str();
									}
								}
								m_args->tmpval += "}";
								
								query.setresponse(i, m_args->tmpval);
							}
							
						} catch (std::exception& e){
							std::cerr<<"caught "<<e.what()<<" trying to access query result!"<<std::endl;
							query.setsuccess(0);
							query.setresponserows(0);
							++(m_args->monitoring_vars->result_access_errors);
						}
						
					} // generic query, manual json formation rom fields
					
					// release pqxx::result and clear error
					query.Clear();
					
				} // if we had a result object
			} // loop over queries in this batch
			
			++(m_args->monitoring_vars->read_batches_processed);
			
		} else {
			
			// process batch of write queries
			// these are interleaved but results are grouped by type
			size_t devconfig_i = 0;
			size_t runconfig_i = 0;
			size_t calibration_i = 0;
			size_t plotlyplot_i = 0;
			size_t rootplot_i = 0;
			bool devconfigs_ok = !m_args->batch->devconfig_version_nums.empty();
			bool runconfigs_ok = !m_args->batch->runconfig_version_nums.empty();
			bool calibrations_ok = !m_args->batch->calibration_version_nums.empty();
			bool plotlyplots_ok = !m_args->batch->plotlyplot_version_nums.empty();
			bool rootplots_ok = !m_args->batch->rootplot_version_nums.empty();
			
			for(ZmqQuery& query : m_args->batch->queries){
				
				switch(query_topic{query.topic()[2]}){
					// alarms return just the success status
					case query_topic::alarm:
						query.setsuccess(m_args->batch->alarm_batch_err.empty());
						query.setresponserows(0);
						break;
						
					// everything else returns a version number
					case query_topic::dev_config:
						query.setsuccess(devconfigs_ok);
						query.setresponserows(1);
						if(devconfigs_ok){
							query.setresponse(0, m_args->batch->devconfig_version_nums[devconfig_i++]);
						} else {
							// FIXME is it worth propagating the error back to the user?
							// since it's a batch insert, the error may have nothing to do with their query...
							query.setresponse(0, m_args->batch->devconfig_batch_err);
						}
						break;
						
					case query_topic::run_config:
						query.setsuccess(runconfigs_ok);
						query.setresponserows(1);
						if(runconfigs_ok){
							query.setresponse(0, m_args->batch->runconfig_version_nums[runconfig_i++]);
						} else {
							query.setresponse(0, m_args->batch->runconfig_batch_err);
						}
						break;
						
					case query_topic::calibration:
						query.setsuccess(calibrations_ok);
						query.setresponserows(1);
						if(calibrations_ok){
							query.setresponse(0, m_args->batch->calibration_version_nums[calibration_i++]);
						} else {
							query.setresponse(0, m_args->batch->calibration_batch_err);
						}
						break;
						
					case query_topic::plotlyplot:
						query.setsuccess(plotlyplots_ok);
						query.setresponserows(1);
						if(plotlyplots_ok){
							query.setresponse(0, m_args->batch->plotlyplot_version_nums[plotlyplot_i++]);
						} else {
							query.setresponse(0, m_args->batch->plotlyplot_batch_err);
						}
						break;
						
					case query_topic::rootplot:
						query.setsuccess(rootplots_ok);
						query.setresponserows(1);
						if(rootplots_ok){
							query.setresponse(0, m_args->batch->rootplot_version_nums[rootplot_i++]);
						} else {
							query.setresponse(0, m_args->batch->rootplot_batch_err);
						}
						break;
						
					case query_topic::generic:
						// just for good measure, when we try to access the pqxx result,
						// enclose within try just in case it throws something
						try {
							// TODO if we can safely shoehorn in a wrapping call to `row_to_json`
							// around a user's generic sql, we can combine this with the above.
							// But, given the arbitrary complexity of statements, this may not be possible.
							// in which case, we need to loop over rows and convert them to JSON manually
							query.setresponserows(std::size(query.result));
							for(size_t i=0; i<std::size(query.result); ++i){
								
								// build a json from fields in this row
								m_args->tmpval = "{";
								for (pqxx::row::iterator it=query.result[i].begin(); it<query.result[i].end(); ++it){
									if(it!=query.result[i].begin()) m_args->tmpval += ", ";
									m_args->tmpval += "\"" + std::string{it->name()} + "\":";
									// Field values are returned bare: i.e. '3' or 'cat' or '{"iam":"ajson"}'
									// but to convert this into JSON, strings need to be quoted:
									// i.e. { "field1":3, "field2":"cat", "field3":{"iam":"ajson"} }
									// this means we need to add enclosing quotes *only* for string fields
									if((it->type()==18) || (it->type()==25) || (it->type()==1042) || (it->type()==1043)){
										m_args->tmpval += "\""+std::string{it->c_str()}+"\"";
									} else {
										m_args->tmpval += it->c_str();
									}
								}
								m_args->tmpval += "}";
								
								query.setresponse(i, m_args->tmpval);
							}
							
						} catch (std::exception& e){
							std::cerr<<"caught "<<e.what()<<" trying to access query result!"<<std::endl;
							query.setsuccess(0);
							query.setresponserows(1);
							query.setresponse(0, query.err);
							++(m_args->monitoring_vars->result_access_errors);
						}
						break;
						
					default:
						// FIXME corrupted topic, log it.
						std::cerr<<m_args->m_job_name<<" unknown topic "<<query.topic()<<std::endl;
						break;
					
				}
				
				// release pqxx::result and clear error
				query.Clear();
				
			} // loop over queries in this batch
			
			++(m_args->monitoring_vars->write_batches_processed);
			
		} // if/else on whether this batch was read/write
		
//	} // loop over query batches
	
	// pass the batch onto the next stage of the pipeline for the DatabaseWorkers
	std::unique_lock<std::mutex> locker(m_args->m_data->query_replies_mtx);
	//m_args->m_data->query_replies.insert(m_args->m_data->query_replies.end(),
	//                                     m_args->local_msg_queue.begin(),m_args->local_msg_queue.end());
	m_args->m_data->query_replies.push_back(m_args->batch);
	locker.unlock();
	
	printf("%s completed\n",m_args->m_job_name.c_str());
	++(m_args->monitoring_vars->jobs_completed);
	
	// return our job args to the pool
	m_args->m_pool->Add(m_args);  // return our job args to the job args struct pool
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	return true;
}


