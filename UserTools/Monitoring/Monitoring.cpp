#include "Monitoring.h"

Monitoring::Monitoring():Tool(){}


bool Monitoring::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	//m_variables.Print();
	
	if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;
	
	// how often to write out monitoring stats
	int monitoring_period_ms = 60000;
	m_variables.Get("monitoring_period_ms",monitoring_period_ms);
	
	ExportConfiguration();
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.emplace(m_tool_name, &monitoring_vars);
	
	thread_args.monitoring_period_ms = std::chrono::milliseconds{monitoring_period_ms};
	thread_args.last_send = std::chrono::steady_clock::now();
	thread_args.m_data = m_data;
	thread_args.monitoring_vars = &monitoring_vars;
	m_data->utils.CreateThread("monitoring", &Thread, &thread_args); // thread needs a unique name
	m_data->num_threads++;
	
	//m_data->services->AddService("middleman", 5000); // is this needed? what for??
	
	return true;
}


bool Monitoring::Execute(){
	
	if(!thread_args.running){
		Log(m_tool_name+" Execute found thread not running!",v_error);
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		// FIXME if restarts > X times in last Y mins, alarm (bypass, shove into DB? send to websocket?) and StopLoop.
		++(monitoring_vars.thread_crashes);
	}
	
	return true;
}


bool Monitoring::Finalise(){
	
	// signal job distributor thread to stop
	Log(m_tool_name+": Joining monitoring thread",v_warning);
	m_data->utils.KillThread(&thread_args);
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	Log(m_tool_name+": Finished",v_warning);
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

void Monitoring::Thread(Thread_args* args){
	
	Monitoring_args* m_args = dynamic_cast<Monitoring_args*>(args);
	
	if((m_args->last_send - std::chrono::steady_clock::now()) > m_args->monitoring_period_ms){
		
		std::unique_lock<std::mutex> locker(m_args->m_data->monitoring_variables_mtx);
		
		for(std::pair<const std::string, MonitoringVariables*>& mon : m_args->m_data->monitoring_variables){
			
			std::string s="{\"time\":0, \"device\":\"middleman\",\"subject\":\""+mon.first+"\", \"data\":"+mon.second->toJSON()+"}";
			
			std::unique_lock<std::mutex> locker(m_args->m_data->out_mon_msg_queue_mtx);
			m_args->m_data->out_mon_msg_queue.push_back(s);
			
		}
		
		/*
		// to calculate rates we need to know the difference in number
		// of reads/writes since last time. So get the last values
		unsigned long last_write_query_count;
		unsigned long last_read_query_count;
		unsigned long last_log_count;
		unsigned long last_mon_count;
		MonitoringStore.Get("write_queries_recvd", last_write_query_count);
		MonitoringStore.Get("read_queries_recvd", last_read_query_count);
		MonitoringStore.Get("logs_recvd", last_log_count);
		MonitoringStore.Get("mons_recvd", last_mon_count);
		
		// calculate rates are per minute
		elapsed_time = boost::posix_time::microsec_clock::universal_time() - last_stats_calc;
		
		float read_query_rate = (elapsed_time.total_seconds()==0) ? 0 :
		    ((read_queries_recvd - last_read_query_count) * 60.) / elapsed_time.total_seconds();
		float write_query_rate = (elapsed_time.total_seconds()==0) ? 0 :
		    ((write_queries_recvd - last_write_query_count) * 60.) / elapsed_time.total_seconds();
		float log_rate = (elapsed_time.total_seconds()==0) ? 0 :
		    ((logs_recvd - last_log_count) * 60.) / elapsed_time.total_seconds();
		float mon_rate = (elapsed_time.total_seconds()==0) ? 0 :
		    ((mons_recvd - last_mon_count) * 60.) / elapsed_time.total_seconds();
		
		// dump all stats into a Store.
		MonitoringStore.Set("min_loop_time",min_loop_ms);
		MonitoringStore.Set("max_loop_time",max_loop_ms);
		MonitoringStore.Set("loops",loops);
		MonitoringStore.Set("loop_rate [Hz]",loops/elapsed_time.total_seconds());
		MonitoringStore.Set("write_queries_waiting",wrt_txn_queue.size());
		MonitoringStore.Set("read_queries_waiting",rd_txn_queue.size());
		MonitoringStore.Set("replies_waiting",resp_queue.size());
		MonitoringStore.Set("incoming_logs_waiting",in_log_queue.size());
		MonitoringStore.Set("incoming_mons_waiting",in_mon_queue.size());
		MonitoringStore.Set("out_multicasts_waiting",out_multicast_queue.size());
		MonitoringStore.Set("cached_queries",cache.size());
		MonitoringStore.Set("write_queries_recvd", write_queries_recvd);
		MonitoringStore.Set("write_query_recv_fails", write_query_recv_fails);
		MonitoringStore.Set("read_queries_recvd", read_queries_recvd);
		MonitoringStore.Set("read_query_recv_fails", read_query_recv_fails);
		MonitoringStore.Set("logs_recvd", logs_recvd.load());
		MonitoringStore.Set("mons_recvd", mons_recvd.load());
		MonitoringStore.Set("log_recv_fails", log_recv_fails.load());
		MonitoringStore.Set("mon_recv_fails", mon_recv_fails.load());
		MonitoringStore.Set("mm_broadcasts_recvd", mm_broadcasts_recvd);
		MonitoringStore.Set("mm_broadcast_recv_fails", mm_broadcast_recv_fails);
		MonitoringStore.Set("write_queries_failed", write_queries_failed);
		MonitoringStore.Set("log_queries_failed", log_queries_failed.load());
		MonitoringStore.Set("mon_queries_failed", mon_queries_failed.load());
		MonitoringStore.Set("read_queries_failed", read_queries_failed);
		MonitoringStore.Set("reps_sent", reps_sent);
		MonitoringStore.Set("rep_send_fails", rep_send_fails);
		MonitoringStore.Set("multicasts_sent", multicasts_sent);
		MonitoringStore.Set("multicast_send_fails", multicast_send_fails);
		MonitoringStore.Set("mm_broadcasts_sent", mm_broadcasts_sent);
		MonitoringStore.Set("mm_broadcasts_failed", mm_broadcasts_failed);
		MonitoringStore.Set("master_clashes", master_clashes);
		MonitoringStore.Set("master_clashes_failed", master_clashes_failed);
		MonitoringStore.Set("standby_clashes", standby_clashes);
		MonitoringStore.Set("standby_clashes_failed", standby_clashes_failed);
		MonitoringStore.Set("self_promotions", self_promotions);
		MonitoringStore.Set("self_promotions_failed", self_promotions_failed);
		MonitoringStore.Set("promotions", promotions);
		MonitoringStore.Set("promotions_failed", promotions_failed);
		MonitoringStore.Set("demotions", demotions);
		MonitoringStore.Set("demotions_failed", demotions_failed);
		MonitoringStore.Set("dropped_writes", dropped_writes);
		MonitoringStore.Set("dropped_reads", dropped_reads);
		MonitoringStore.Set("dropped_resps", dropped_resps);
		MonitoringStore.Set("dropped_log_in", dropped_log_in);
		MonitoringStore.Set("dropped_mon_in", dropped_mon_in);
		MonitoringStore.Set("dropped_logs_out", dropped_logs_out);
		MonitoringStore.Set("dropped_monitoring_out", dropped_monitoring_out);
		MonitoringStore.Set("read_query_rate", read_query_rate);
		MonitoringStore.Set("write_query_rate", write_query_rate);
		
		// convert Store into a json
		std::string json_stats;
		MonitoringStore >> json_stats;
		
		// update the web page status
		// actually, this only supports a single word, with no spaces?
		std::stringstream status;
		status << "  read qrys (rcvd/rcv errs/qry errs):["<<read_queries_recvd<<"|"<<read_query_recv_fails<<"|"<<read_queries_failed
		       <<"]; write qrys:["<<write_queries_recvd<<"|"<<write_query_recv_fails<<"|"<<write_queries_failed
		       <<"]; log qrys:["<<logs_recvd<<"|"<<log_recv_fails<<"|"<<log_queries_failed
		       <<"]; monitoring qrys:["<<mons_recvd<<"|"<<mon_recv_fails<<"|"<<mon_queries_failed
		       <<"]; replies (ok/err):["<<reps_sent<<"|"<<rep_send_fails
		       <<"]; dropped (reads/writes/multicasts/resps):["<<dropped_reads<<"|"<<dropped_writes<<"|"<<dropped_log_in<<"|"<<dropped_mon_in<<"|"<<dropped_resps
		       <<"]";
		SC_vars["Status"]->SetValue(status.str());
		
//		// temporarily bypass the database logging level to ensure it gets sent to the monitoring db.
//		int db_verbosity_tmp = db_verbosity;
//		db_verbosity = 10;
//		Log(Concat("Monitoring Stats:",json_stats),15);
//		db_verbosity = db_verbosity_tmp;
		
		//std::string sql_qry = "INSERT INTO monitoring ( time, device, subject, data ) VALUES ( 'now()', '"
		//	                + my_id+"','stats','"+json_stats+"' );";
		
		std::string multicast_msg = "{ \"topic\":\"monitoring\""
			                        ", \"subject\":\"stats\""
			                        ", \"device\":\""+escape_json(my_id)+"\""
			                      + ", \"time\":"+std::to_string(time(nullptr)*1000)  // ms since unix epoch
			                      + ", \"data\":\""+json_stats+"\" }";
		
		if(am_master){
			in_mon_queue_mtx.lock();
			in_mon_queue.push_back(multicast_msg);
			in_mon_queue_mtx.unlock();
		} else {
			out_multicast_queue.push_back(multicast_msg); // FIXME FIXME FIXME needs to go to mon port
		}
		
		// reset counters
		last_send = std::chrono::steady_clocknow();
		
		min_loop_ms=9999999;
		max_loop_ms=0;
		loops=0;
		
		*/
	}
	
	return;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool Monitoring::ResetStats(bool reset){
/*
	if(!reset) return true;
	
	min_loop_ms=0;
	max_loop_ms=0;
	loops=0;
	write_queries_recvd=0;
	write_query_recv_fails=0;
	read_queries_recvd=0;
	read_query_recv_fails=0;
	logs_recvd=0;
	mons_recvd=0;
	log_recv_fails=0;
	mon_recv_fails=0;
	mm_broadcasts_recvd=0;
	mm_broadcast_recv_fails=0;
	write_queries_failed=0;
	log_queries_failed=0;
	mon_queries_failed=0;
	read_queries_failed=0;
	reps_sent=0;
	rep_send_fails=0;
	multicasts_sent=0;
	multicast_send_fails=0;
	mm_broadcasts_sent=0;
	mm_broadcasts_failed=0;
	master_clashes=0;
	master_clashes_failed=0;
	standby_clashes=0;
	standby_clashes_failed=0;
	self_promotions=0;
	self_promotions_failed=0;
	promotions=0;
	promotions_failed=0;
	demotions=0;
	demotions_failed=0;
	dropped_writes=0;
	dropped_reads=0;
	dropped_resps=0;
	dropped_log_in=0;
	dropped_mon_in=0;
	dropped_logs_out=0;
	dropped_monitoring_out=0;
	
	MonitoringStore.Set("write_queries_recvd", 0);
	MonitoringStore.Set("read_queries_recvd", 0);
	
	last_stats_calc = boost::posix_time::microsec_clock::universal_time();
	std::string timestring;
	TimeStringFromUnixSec(0, timestring);
	SC_vars["ResetStats"]->SetValue(false);
*/
	
	return true;
}
