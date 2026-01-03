#include "DatabaseWorkers.h"
#include <memory>
#include <pqxx/pqxx>
//#include <pqxx/prepared_statement>

DatabaseWorkers::DatabaseWorkers():Tool(){}

std::string DatabaseWorkers::connection_string="";

bool DatabaseWorkers::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	//m_variables.Print();
	
	/* ----------------------------------------- */
	/*               Configuration               */
	/* ----------------------------------------- */
	
	m_verbose=1;
	std::string dbhostname = "/tmp";     // '/tmp' = local unix socket
	std::string dbhostaddr = "";         // fallback if hostname is empty, an ip address
	int dbport = 5432;                   // database port
	std::string dbname = "daq";          // database name
	std::string dbuser = "";             // database user to connect as. defaults to PGUSER env var if empty.
	std::string dbpasswd = "";           // database password. defaults to PGPASS or PGPASSFILE if not given.
	
	// on authentication: we may consider using 'ident', which will permit the
	// user to connect to the database as the postgres user with name matching
	// their OS username, and/or the database user mapped to their username
	// with the pg_ident.conf file in postgres database. in such a case dbuser and dbpasswd
	// should be left empty
	
	m_variables.Get("verbose",m_verbose);
	m_variables.Get("hostname",dbhostname);
	m_variables.Get("hostaddr",dbhostaddr);
	m_variables.Get("dbname",dbname);
	m_variables.Get("port",dbport);
	m_variables.Get("user",dbuser);
	m_variables.Get("passwd",dbpasswd);
	// number of database workers - FIXME needs to match concurrency of postgres backend
	max_workers = 10;
	m_variables.Get("max_workers", max_workers);
	
	ExportConfiguration();
	
	/* ----------------------------------------- */
	/*               Thread Setup                */
	/* ----------------------------------------- */
	
	// monitoring struct to encapsulate tracking info
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.emplace(m_tool_name, &monitoring_vars);
	
	// we *do* need a unique worker pool here because these workers
	// maintain a connection to the database, so are a 'limited resource'
	job_manager = new WorkerPoolManager(database_jobqueue, &max_workers, &(m_data->thread_cap), &(m_data->num_threads), nullptr, true);
	
	thread_args.m_data = m_data;
	thread_args.monitoring_vars = &monitoring_vars;
	thread_args.job_queue = &database_jobqueue;
	if(!m_data->utils.CreateThread("database_job_distributor", &Thread, &thread_args)){
		Log(m_tool_name+": Failed to spawn background thread",v_error,m_verbose);
		return false;
	}
	m_data->num_threads++;
	
	/* ----------------------------------------- */
	/*                  DB Test                  */
	/* ----------------------------------------- */
	
	// pass connection details to the postgres interface class
	std::stringstream tmp;
	if(dbhostname!="") tmp<<" host="<<dbhostname;
	if(dbhostaddr!="") tmp<<" hostaddr="<<dbhostaddr;
	if(dbname!="")     tmp<<" dbname="<<dbname;
	if(dbport!=-1)     tmp<<" port="<<dbport;
	if(dbuser!="")     tmp<<" user="<<dbuser;
	if(dbpasswd!="")   tmp<<" password="<<dbpasswd;
	connection_string = tmp.str();
	// fail early: open a connection just to check we can
	try {
		pqxx::connection test_conn(connection_string);
		// verify we succeeded
		// "don't use is_open(), use the broken_connection exception", they say. Hmm.
		// But will that be thrown now, or only when we try to *use* the connection, for a transaction?
		// may depend on the connection type... let's just check?
		if(!test_conn.is_open()){
			std::cerr<<"pqxx::connection::is_open() returned false after connection attempt"<<std::endl;
			std::cerr<<"Connection string was: '"<<tmp.str()<<"'"<<std::endl;  // FIXME cerr -> Log
			return false;
		}
		// closes connection here on destruction
	} catch (const pqxx::broken_connection &e){
		// as usual the doxygen sucks, but it seems this doesn't provide
		// any further methods to obtain information about the failure mode,
		// so probably not useful to catch this explicitly.
		std::cerr << e.what() << std::endl; // FIXME cerr -> Log
		return false;
	}
	catch (std::exception const &e){
		std::cerr << current_exception_name()<<": "<<e.what() << std::endl; // FIXME cerr -> Log
		return false;
	}
	
	return true;
}


bool DatabaseWorkers::Execute(){
	
	// the main thread is going to lock the datamodel vector of queries
	// grab a bunch of entries, and spin off a job for each batch of queries
	// (possibly doing this several times to spin off multiple jobs)
	
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


bool DatabaseWorkers::Finalise(){
	
	// signal job distributor thread to stop
	Log(m_tool_name+": Joining job distributor thread",v_warning);
	m_data->utils.KillThread(&thread_args);
	Log(m_tool_name+": Finished",v_warning);
	m_data->num_threads--;
	
	// deleting the worker pool manager will kill all the worker threads
	Log(m_tool_name+": Joining database worker thread pool",v_warning);
	delete job_manager;
	job_manager = nullptr;
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	Log(m_tool_name+": Finished",v_warning);
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

void DatabaseWorkers::Thread(Thread_args* args){
	
	DatabaseJobDistributor_args* m_args = dynamic_cast<DatabaseJobDistributor_args*>(args);
	
	// get a new Job to the job queue to process this data
	if(m_args->the_job==nullptr){
		m_args->the_job = m_args->m_data->job_pool.GetNew("database_worker");
		m_args->the_job->out_pool = &m_args->m_data->job_pool;
		
		if(m_args->the_job->data == nullptr){
			// on first creation of the job, make it a JobStruct to encapsulate its data
			// N.B. Pool::GetNew will only invoke the constructor if this is a new instance,
			// (not if it's been used before and then returned to the pool)
			// so don't pass job-specific variables to the constructor
			m_args->the_job->data = m_args->job_struct_pool.GetNew(&m_args->job_struct_pool, m_args->m_data, m_args->monitoring_vars);
		} else {
			// FIXME error
			std::cerr<<"database_worker Job with non-null data pointer!"<<std::endl;
		}
		
		m_args->the_job->func = DatabaseJob;
		m_args->the_job->fail_func = DatabaseJobFail;
		
		// FIXME this could leak the_job if the toolchain ends... gonna ignore that, i dunno how to handle it.
	}
	
	DatabaseJobStruct* job_data = static_cast<DatabaseJobStruct*>(m_args->the_job->data);
	job_data->clear();
	
	// XXX ok we have flexibility here on how much we want each worker to grab
	// the more we do in one transaction (one job) the better throughput...
	// but with possibly greater latency on replies
	
	// grab logging queries
	std::unique_lock<std::mutex> locker(m_args->m_data->log_query_queue_mtx);
	if(!m_args->m_data->log_query_queue.empty()){
		std::swap(m_args->m_data->log_query_queue, job_data->logging_queue);
		printf("DbJobDistributor grabbed %d log batches\n",job_data->logging_queue.size());
	}
	
	// grab monitoring queries
	locker = std::unique_lock<std::mutex>(m_args->m_data->mon_query_queue_mtx);
	if(!m_args->m_data->mon_query_queue.empty()){
		std::swap(m_args->m_data->mon_query_queue, job_data->monitoring_queue);
	}
	
	// if rootplot queries go over multicast, grab those
	locker = std::unique_lock<std::mutex>(m_args->m_data->rootplot_query_queue_mtx);
	if(!m_args->m_data->rootplot_query_queue.empty()){
		std::swap(m_args->m_data->rootplot_query_queue, job_data->rootplot_queue);
	}
	
	// if plotlyplot queries go over multicast, grab those
	locker = std::unique_lock<std::mutex>(m_args->m_data->plotlyplot_query_queue_mtx);
	if(!m_args->m_data->plotlyplot_query_queue.empty()){
		std::swap(m_args->m_data->plotlyplot_query_queue, job_data->plotlyplot_queue);
	}
	
	// grab write queries
	locker = std::unique_lock<std::mutex>(m_args->m_data->write_query_queue_mtx);
	if(!m_args->m_data->write_query_queue.empty()){
		std::swap(m_args->m_data->write_query_queue, job_data->write_queue);
		printf("DbJobDistributor grabbed %d write query batches\n",job_data->write_queue.size());
	}
	
	// grab read queries
	locker = std::unique_lock<std::mutex>(m_args->m_data->read_msg_queue_mtx);
	if(!m_args->m_data->read_msg_queue.empty()){
		std::swap(m_args->m_data->read_msg_queue, job_data->read_queue);
		printf("DbJobDistributor grabbed %d read query batches\n",job_data->read_queue.size());
	}
	
	locker.unlock();
	
	// check if the job had something to do
	if(job_data->logging_queue.empty() &&
	   job_data->monitoring_queue.empty() &&
	   job_data->rootplot_queue.empty() &&
	   job_data->plotlyplot_queue.empty() &&
	   job_data->write_queue.empty() &&
	   job_data->read_queue.empty()) return;
	
	printf("DbJobDistributor making db job!\n");
	job_data->m_job_name = "database_worker";
	
	m_args->job_queue->AddJob(m_args->the_job);
	m_args->the_job = nullptr;
	
	return;
	
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

void DatabaseWorkers::DatabaseJobFail(void*& arg){
	
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
	
	//query.result.clear(); // to clear/release bad results...
	// ideally we want to pass back an error or what happened to the client (set query.err)
	//query.err = ??? but what was the problem?
	
	DatabaseJobStruct* m_args=static_cast<DatabaseJobStruct*>(arg);
	std::cerr<<m_args->m_job_name<<" failure"<<std::endl;
	++(m_args->monitoring_vars->jobs_failed);
	
	// return our job args to the pool
	m_args->m_pool->Add(m_args);
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	return;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool DatabaseWorkers::DatabaseJob(void*& arg){
	
	DatabaseJobStruct* m_args = static_cast<DatabaseJobStruct*>(arg);
	printf("DB worker starting!\n");
	
	// the worker will need a connection to the database
	thread_local std::unique_ptr<pqxx::connection> conn;
	if(conn==nullptr){
		conn.reset(new pqxx::connection(DatabaseWorkers::connection_string));
		if(!conn){
			//Log("Failed to open connection to database for worker thread!",v_error); // FIXME logging
			// FIXME terminate this worker... m_args->running=false?
			return false;
		} else {
			// set up prepared statements. These are, sadly, a property of the connection
			// logging insert
			conn->prepare("logging_insert", "INSERT INTO logging ( time, device, severity, message ) SELECT * FROM jsonb_to_recordset( $1::jsonb ) as t(time timestamptz, device text, severity int, message text)");
			// monitoring insert
			conn->prepare("monitoring_insert", "INSERT INTO monitoring ( time, device, subject, data ) SELECT * FROM jsonb_to_recordset( $1::jsonb ) as t(time timestamptz, device text, subject text, data jsonb)");
			// alarms insert
			conn->prepare("alarms_insert", "INSERT INTO alarms ( time, device, level, alarm ) SELECT * FROM jsonb_to_recordset( $1::jsonb ) as t(time timestamptz, device text, level int, alarm text)");
			// rootplot insert
			conn->prepare("rootplots_insert", "INSERT INTO rootplots ( time, name, data, draw_options, lifetime ) SELECT * FROM jsonb_to_recordset( $1::jsonb ) as t(time timestamptz, name text, data jsonb, draw_options text, lifetime int) returning version");
			// plotlyplot insert
			conn->prepare("plotlyplots_insert", "INSERT INTO plotlyplots ( time, name, data, layout, lifetime ) SELECT * FROM jsonb_to_recordset( $1::jsonb ) as t(time timestamptz, name text, data jsonb, layout jsonb, lifetime int) returning version");
			// calibration insert
			conn->prepare("calibration_insert", "INSERT INTO calibration ( time, name, description, data ) SELECT * FROM jsonb_to_recordset( $1::jsonb ) as t(time timestamptz, name text, description text, data jsonb) returning version");
			// device config insert
			conn->prepare("device_config_insert", "INSERT INTO device_config ( time, device, author, description, data ) SELECT * FROM jsonb_to_recordset( $1::jsonb ) as t(time timestamptz, device text, author text, description text, data jsonb) returning version");
			// run config insert
			conn->prepare("run_config_insert", "INSERT INTO run_config ( time, name, author, description, data ) SELECT * FROM jsonb_to_recordset( $1::jsonb ) as t(time timestamptz, name text, author text, description text, data jsonb) returning config_id");
		}
	}
	
	// FIXME if the DB goes down, implement some sort of pausing(?) or local recording to local disk (SQLite?)
	
	// we also use a single transaction for all queries, so open that now
	pqxx::work* tx = new pqxx::work(*conn.get()); // aka pqxx::transaction<>
//	pqxx::substransaction sub(tx); // aka create savepoint and rollback on error. possibly harmful for performance...
	
	// ok, so the problem with this is any query that fails within a transaction subsequently throws:
	// 'current transaction is aborted, commands ignored until end of transaction block'
	// for any further use, so we need to handle that.
	
	// insert new logging statements
	printf("calling prepped for %d logging batches\n",m_args->logging_queue.size());
	for(std::string& batch : m_args->logging_queue){
		printf("dbworker inserting logging batch: '%s'\n",batch.c_str());
		try {
			tx->exec(pqxx::prepped{"logging_insert"}, pqxx::params{batch});
			++(m_args->monitoring_vars->logging_submissions);
		} catch (std::exception& e){
			std::cerr<<"dbworker log insert failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
			++(m_args->monitoring_vars->logging_submissions_failed);
			// FIXME log the error here
			// FIXME if we catch (pqxx::sql_error const &e) or others can we get better information?
			// after error the transaction becomes unusable, and we must open a new one
			delete tx;
			tx = new pqxx::work(*conn.get());
		}
	}
	
	// insert new monitoring statements
	printf("calling prepped for %d monitoring batches\n",m_args->monitoring_queue.size());
	for(std::string& batch : m_args->monitoring_queue){
		try {
			tx->exec(pqxx::prepped{"monitoring_insert"}, pqxx::params{batch});
			++(m_args->monitoring_vars->monitoring_submissions);
		} catch (std::exception& e){
			++(m_args->monitoring_vars->monitoring_submissions_failed);
			std::cerr<<"dbworker mon insert failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
			// FIXME log the error here
			delete tx;
			tx = new pqxx::work(*conn.get());
		}
	}
	
	// insert new multicast rootplot statements
	printf("calling prepped for %d rootplot batches\n",m_args->rootplot_queue.size());
	for(std::string& batch : m_args->rootplot_queue){
		try {
			tx->exec(pqxx::prepped{"rootplots_insert"}, pqxx::params{batch});
			++(m_args->monitoring_vars->rootplot_submissions);
		} catch (std::exception& e){
			++(m_args->monitoring_vars->rootplot_submissions_failed);
			std::cerr<<"dbworker rootplot insert failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
			// FIXME log the error here
			delete tx;
			tx = new pqxx::work(*conn.get());
		}
	}
	
	// insert new multicast plotlyplot statements
	printf("calling prepped for %d plotlyplot batches\n",m_args->plotlyplot_queue.size());
	for(std::string& batch : m_args->plotlyplot_queue){
		try {
			tx->exec(pqxx::prepped{"plotlyplots_insert"}, pqxx::params{batch});
			++(m_args->monitoring_vars->plotlyplot_submissions);
		} catch (std::exception& e){
			++(m_args->monitoring_vars->plotlyplot_submissions_failed);
			std::cerr<<"dbworker plotlyplot insert failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
			// FIXME log the error here
			delete tx;
			tx = new pqxx::work(*conn.get());
		}
	}
	
	// write queries
	printf("processing %d write batches\n",m_args->write_queue.size());
	for(QueryBatch* batch : m_args->write_queue){
		// the batch gets split up by WriteWorkers into a buffer for each type of write query
		
		// alarm insertions return nothing, just catch errors
		if(batch->got_alarms()){
			printf("calling prepped for alarm buffer '%s'\n",batch->alarm_buffer.c_str());
			try {
				tx->exec(pqxx::prepped{"alarms_insert"}, pqxx::params{batch->alarm_buffer});
				++(m_args->monitoring_vars->alarm_submissions);
			} catch (std::exception& e){
				batch->alarm_batch_err = current_exception_name()+": "+e.what();
				++(m_args->monitoring_vars->alarm_submissions_failed);
				std::cerr<<"dbworker alarm batch '"<<batch->alarm_buffer<<"' insert failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
				delete tx;
				tx = new pqxx::work(*conn.get());
				// FIXME log the error here
			}
		}
		
		// the remaining insertions return the new version number
		// `pqxx::transaction_base::for_query` runs a query and invokes a callable for each result row
		// we use this to collect the returned version numbers into a vector
		// N.B. `pqxx::transaction_base::for_stream` is an alternative that is faster for large results
		// but slower for small results. TODO check whether ours count as 'large' .. probably not.
		
		// device config insertions
		if(batch->got_devconfigs()){
			printf("calling prepped for dev_config buffer '%s'\n",batch->devconfig_buffer.c_str());
			try {
				tx->for_query(pqxx::prepped{"device_config_insert"},
					[&batch](int32_t new_version_num){
						batch->devconfig_version_nums.push_back(new_version_num);
					}, pqxx::params{batch->devconfig_buffer});
				++(m_args->monitoring_vars->devconfig_submissions);
			} catch (std::exception& e){
				++(m_args->monitoring_vars->devconfig_submissions_failed);
				batch->devconfig_batch_err = current_exception_name()+": "+e.what();
				std::cerr<<"dbworker devconfig insert '"<<batch->devconfig_buffer<<"' failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
				// FIXME log the error here
				delete tx;
				tx = new pqxx::work(*conn.get());
			}
		}
		
		// run config insertions
		if(batch->got_runconfigs()){
			printf("calling prepped for run_config buffer '%s'\n",batch->runconfig_buffer.c_str());
			try {
				tx->for_query(pqxx::prepped{"run_config_insert"},
					[&batch](int32_t new_version_num){
						batch->runconfig_version_nums.push_back(new_version_num);
					}, pqxx::params{batch->runconfig_buffer});
				++(m_args->monitoring_vars->runconfig_submissions);
			} catch (std::exception& e){
				++(m_args->monitoring_vars->runconfig_submissions_failed);
				batch->runconfig_batch_err = current_exception_name()+": "+e.what();
				std::cerr<<"dbworker runconfig insert '"<<batch->runconfig_buffer<<"' failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
				// FIXME log the error here
				delete tx;
				tx = new pqxx::work(*conn.get());
			}
		}
		
		// calibration data insertions
		if(batch->got_calibrations()){
			printf("calling prepped for calibration buffer '%s'\n",batch->calibration_buffer.c_str());
			try {
				tx->for_query(pqxx::prepped{"calibration_insert"},
					[&batch](int32_t new_version_num){
						batch->calibration_version_nums.push_back(new_version_num);
					}, pqxx::params{batch->calibration_buffer});
				++(m_args->monitoring_vars->calibration_submissions);
			} catch (std::exception& e){
				++(m_args->monitoring_vars->calibration_submissions_failed);
				batch->calibration_batch_err = current_exception_name()+": "+e.what();
				std::cerr<<"dbworker calibration insert '"<<batch->calibration_buffer<<"' failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
				// FIXME log the error here
				delete tx;
				tx = new pqxx::work(*conn.get());
			}
		}
		
		// rootplot insertions
		if(batch->got_rootplots()){
			printf("calling prepped for rootplots buffer '%s'\n",batch->rootplot_buffer.c_str());
			try {
				tx->for_query(pqxx::prepped{"rootplots_insert"},
					[&batch](int32_t new_version_num){
						batch->rootplot_version_nums.push_back(new_version_num);
					}, pqxx::params{batch->rootplot_buffer});
				++(m_args->monitoring_vars->rootplot_submissions);
			} catch (std::exception& e){
				++(m_args->monitoring_vars->rootplot_submissions_failed);
				batch->rootplot_batch_err = current_exception_name()+": "+e.what();
				std::cerr<<"dbworker rootplot insert '"<<batch->rootplot_buffer<<"' failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
				// FIXME log the error here
				delete tx;
				tx = new pqxx::work(*conn.get());
			}
		}
		
		// plotlyplot insertions
		if(batch->got_plotlyplots()){
			printf("calling prepped for plotlyplots buffer '%s'\n",batch->plotlyplot_buffer.c_str());
			try {
				tx->for_query(pqxx::prepped{"plotlyplots_insert"},
					[&batch](int32_t new_version_num){
						batch->plotlyplot_version_nums.push_back(new_version_num);
					}, pqxx::params{batch->plotlyplot_buffer});
				++(m_args->monitoring_vars->plotlyplot_submissions);
			} catch (std::exception& e){
				++(m_args->monitoring_vars->plotlyplot_submissions_failed);
				batch->plotlyplot_batch_err = current_exception_name()+": "+e.what();
				std::cerr<<"dbworker plotlyplot insert '"<<batch->plotlyplot_buffer<<"' failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
				// FIXME log the error here
				delete tx;
				tx = new pqxx::work(*conn.get());
			}
		}
		
		// generic query insertions
		// we can't batch these as they're just arbitrary SQL from the user,
		// so we need to loop over them.
		// FIXME no performance optimisation here: don't expect there to be many... right?
		// if there's a lot we could use a pipeline as below...but the overhead may not be worth it
		printf("serially executing %d generic queries\n",batch->generic_write_query_indices.size());
		for(size_t i : batch->generic_write_query_indices){
			ZmqQuery& query = batch->queries[i];
			try {
				query.result = tx->exec(query.msg());
				++(m_args->monitoring_vars->generic_submissions);
			} catch (std::exception& e){
				++(m_args->monitoring_vars->generic_submissions_failed);
				query.result.clear();
				query.err = current_exception_name()+": "+e.what();
				std::cerr<<"dbworker generic query '"<<query.msg()<<"' failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
				pqxx::sql_error* sqle = dynamic_cast<pqxx::sql_error*>(&e);
				//if(sqle) std::cerr<<"SQLSTATE is now "<<sqle->sqlstate()<<std::endl;
				// FIXME log the error here
				delete tx;
				tx = new pqxx::work(*conn.get());
			}
		}
	}
	
	// read queries
	// since these don't actually modify the database, if any query or the final 'commit' fails,
	// then preceding queries should already have their results
	// (and FIXME check this, maybe later retrieve calls still work too?)
	// if so, we can:
	// 1. move them to the start of the job, so we can salvage what ran successfully?
	//    the trouble with that is these queries are "less reliable" since they are not necessarily formed by us.
	//    perhaps we could separate out `Query` topic jobs to run at the end after the `commit` call? XXX probably this!
	// 2. move these to a separate worker job?
	
	// each batch contains a vector of queries, but unlike inserts, we can't batch these FIXME i think?
	// for giggles, we'll pipeline them. This may even improve performance.
	pqxx::pipeline px(*tx);
	m_args->ids.clear();
	m_args->pipeline_error = false;
	printf("processing %d read query batches\n",m_args->read_queue.size());
	for(QueryBatch* batch : m_args->read_queue){
		
		printf("pipelining batch of %d read queries\n",batch->queries.size());
		
		// it may be best to set the pipeline to retain ~the number of queries we're going to insert,
		// so that it runs them all in one. TODO or maybe do it in two halves?
		px.retain(batch->queries.size());
		
		// push all the queries to the DB
		for(ZmqQuery& query : batch->queries){
			m_args->ids.push_back(px.insert(query.msg()));
		}
		
		// pull the results
		for(size_t i=0; i<batch->queries.size(); ++i){
			ZmqQuery& query = batch->queries[i];
			try {
				if(px.empty()){
					// we should never find the pipeline empty! ... i think?
					// unless this happens if we check too soon?? (i.e. no results ready *yet*?) FIXME??
					m_args->pipeline_error=true;
					break;
				} else {
					// FIXME note that retrieving a specific id will block until that result is available
					// which means a long-running query will hold up sending replies to faster ones....
					// actually, we'd have to wait on the whole pipeline before returning anyway,
					// but this is an issue with batching...
					query.result = px.retrieve(m_args->ids[i]);
					++(m_args->monitoring_vars->readquery_submissions);
				}
			} catch (std::exception& e){
				++(m_args->monitoring_vars->readquery_submissions_failed);
				query.result.clear(); // this sets `m_query=nullptr` so maybe we can use that as a check...? FIXME
				query.err = current_exception_name()+": "+e.what(); // store info about what failed
				std::cerr<<"dbworker read query '"<<query.msg()<<"' failed with "<<current_exception_name()<<": "<<e.what()<<std::endl;
			}
		}
		
		// synchronization check
		if(m_args->pipeline_error || !px.empty()){
			// either we broke early because the pipeline was empty before we got all results,
			// or pipeline is not empty and we're missing some...
			std::cerr<<"dbworker pipeline error retrieving all results!"<<std::endl;
			// FIXME log error, do something
		}
	}
	// we're done with the pipeline: close it and detach, whatever that means.
	px.complete();
	
	// commit the transaction. Need to do this before it goes out of scope or the whole thing will be rolled back!
	// FIXME we've already copied out vesion numbers and success statuses as we went along, but if this happens
	// those statuses need to be reset!!! FIXME i guess do this in fail func?
	// we therefore need to throw for ANY errors to invoke this!
	try {
		tx->commit();
	} catch(std::exception& e){
		// oh yeaaa, the transaction might have commited, or it might not have. awesome.
		// our consolation prize is a `pqxx::in_doubt_error`.
		// FIXME supposedly, it is up to us to determine whether it committed or not
		// perhaps by attempting to query whether the inserted records are found...
		std::cerr<<"dbworker caught "<<current_exception_name()<<": "<<e.what()<<" committing transaction!"<<std::endl;
	}
	
	// pass the batch onto the next stage of the pipeline for the DatabaseWorkers
	if(!m_args->write_queue.empty()){
		// FIXME FIXME FIXME rename 'read_replies' to 'raw_replies' or something better
		printf("returning %d write acknowledgements to datamodel\n", m_args->write_queue.size());
		std::unique_lock<std::mutex> locker(m_args->m_data->read_replies_mtx);
		m_args->m_data->read_replies.insert(m_args->m_data->read_replies.end(),
		                                     m_args->write_queue.begin(),m_args->write_queue.end());
	}
	
	if(!m_args->read_queue.empty()){
		printf("returning %d read replies to datamodel\n", m_args->read_queue.size());
		std::unique_lock<std::mutex> locker(m_args->m_data->read_replies_mtx);
		m_args->m_data->read_replies.insert(m_args->m_data->read_replies.end(),
		                                    m_args->read_queue.begin(),m_args->read_queue.end());
	}
	
	printf("%s completed\n",m_args->m_job_name.c_str());
	++(m_args->monitoring_vars->jobs_completed);
	
	// return our job args to the pool
	m_args->m_pool->Add(m_args);  // return our job args to the job args struct pool
	m_args = nullptr;  // clear the local m_args variable... not strictly necessary
	arg = nullptr;     // clear the job 'data' member variable
	
	
	return true;
}


