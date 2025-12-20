#include "WriteQueryReceiver.h"

WriteQueryReceiver::WriteQueryReceiver():Tool(){}


bool WriteQueryReceiver::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	//m_variables.Print();
	
	/* ----------------------------------------- */
	/*               Configuration               */
	/* ----------------------------------------- */
	
//	am_master = true; // FIXME not sure being used any more
	m_verbose=1;
	port_name = "db_write";
	// FIXME do these timeouts need to be << transfer_period_ms?
	int poll_timeout_ms = 500;
	int rcv_timeout_ms = 500;
	int transfer_period_ms = 200;
	int local_buffer_size = 200;
	int rcv_hwm=10000; // FIXME sufficient?
	int conns_backlog=1000; // FIXME sufficient?
	
	m_variables.Get("verbose",m_verbose);
	m_variables.Get("port_name", port_name);
	m_variables.Get("rcv_hwm", rcv_hwm); // max num outstanding messages in receive buffer
	m_variables.Get("conns_backlog", conns_backlog); // max num oustanding connection requests
	m_variables.Get("poll_timeout_ms",poll_timeout_ms);
	m_variables.Get("rcv_timeout_ms",rcv_timeout_ms);
	m_variables.Get("local_buffer_size", local_buffer_size);
	m_variables.Get("transfer_period_ms", transfer_period_ms);
//	m_variables.Get("am_master", am_master);
	
	ExportConfiguration();
	
	/* ----------------------------------------- */
	/*               Socket Setup                */
	/* ----------------------------------------- */
	
	// Write queries are received via a SUB socket so they get to both middlemen - only the master runs the query.
	// acknowledgements and any 'returning' results are sent on the ROUTER socket used for receiving read queries
	
	// socket to receive published write queries from clients
	// -------------------------------------------------------
	ManagedSocket* managed_socket = new ManagedSocket;
	managed_socket->service_name=""; // attach to any client type...
	managed_socket->port_name = port_name; // ...that advertises a service on port 'port_name'
	managed_socket->socket = new zmq::socket_t(*m_data->context, ZMQ_SUB);
	// this socket never sends, so a send timeout is irrelevant.
	managed_socket->socket->setsockopt(ZMQ_RCVTIMEO, rcv_timeout_ms);
	// don't linger too long, it looks like the program crashed.
	managed_socket->socket->setsockopt(ZMQ_LINGER, 10);
	managed_socket->socket->setsockopt(ZMQ_SUBSCRIBE,"",0);
	managed_socket->socket->setsockopt(ZMQ_RCVHWM,rcv_hwm);
	managed_socket->socket->setsockopt(ZMQ_BACKLOG,conns_backlog);
	
	// add the socket to the datamodel for the SocketManager, which will handle making new connections to clients
	std::unique_lock<std::mutex> locker(m_data->managed_sockets_mtx);
	m_data->managed_sockets[port_name] = managed_socket;
	
	/* ----------------------------------------- */
	/*               Thread Setup                */
	/* ----------------------------------------- */
	
	// monitoring struct to encapsulate tracking info
	locker = std::unique_lock<std::mutex>(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.emplace(m_tool_name, &monitoring_vars);
	
	thread_args.m_data = m_data;
	thread_args.m_tool_name = m_tool_name;
	thread_args.monitoring_vars = &monitoring_vars;
	thread_args.socket = managed_socket->socket;
	thread_args.socket_mtx = &managed_socket->socket_mtx; // for sharing socket with SocketManager
	thread_args.poll_timeout_ms = poll_timeout_ms;
	thread_args.poll = zmq::pollitem_t{*managed_socket->socket, 0, ZMQ_POLLIN, 0};
	thread_args.in_local_queue = m_data->querybatch_pool.GetNew(local_buffer_size);
	thread_args.local_buffer_size = local_buffer_size;
	thread_args.transfer_period_ms = std::chrono::milliseconds{transfer_period_ms};
	thread_args.make_new = true;
	
	m_data->utils.CreateThread("write_query_receiver", &Thread, &thread_args); // thread needs a unique name
	m_data->num_threads++;
	
	return true;
}

// FIXME renoame to writequeryreceiver
bool WriteQueryReceiver::Execute(){
	
	if(!thread_args.running){
		Log(m_tool_name+" Execute found thread not running!",v_error);
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++(monitoring_vars.thread_crashes);
	}
	
	/*
	FIXME are we doing this
	if(am_master != am_master_last){
		if(m_data->am_master) Promote();
		else Demote();
	}
	*/
	
	return true;
}


bool WriteQueryReceiver::Finalise(){
	
	// signal background receiver thread to stop
	Log(m_tool_name+": Joining receiver thread",v_warning);
	m_data->utils.KillThread(&thread_args);
	std::cerr<<"WriteReceiver thread terminated"<<std::endl;
	m_data->num_threads--;
	
	if(m_data->managed_sockets.count(port_name)){
		std::unique_lock<std::mutex> locker(m_data->managed_sockets_mtx);
		ManagedSocket* sock = m_data->managed_sockets[port_name];
		m_data->managed_sockets.erase(port_name);
		locker.unlock();
		if(sock->socket) delete sock->socket; // destructor closes socket
		delete sock;
	}
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	Log(m_tool_name+": Finished",v_warning);
	return true;
}

void WriteQueryReceiver::Thread(Thread_args* args){
	
	WriteQueryReceiver_args* m_args = reinterpret_cast<WriteQueryReceiver_args*>(args);
	
	// transfer to datamodel
	// =====================
	if(m_args->in_local_queue->queries.size() >= m_args->local_buffer_size ||
	  (m_args->last_transfer - std::chrono::steady_clock::now()) > m_args->transfer_period_ms){
		
		if(!m_args->make_new) m_args->in_local_queue->queries.pop_back();
		if(!m_args->in_local_queue->queries.empty()){
			
			std::clog<<m_args->m_tool_name<<": adding "<<m_args->in_local_queue->queries.size()
			         <<" messages to datamodel"<<std::endl;
			
			std::unique_lock<std::mutex> locker(m_args->m_data->write_msg_queue_mtx);
			m_args->m_data->write_msg_queue.push_back(m_args->in_local_queue);
			locker.unlock();
			
			m_args->in_local_queue = m_args->m_data->querybatch_pool.GetNew(m_args->local_buffer_size);
			
			m_args->last_transfer = std::chrono::steady_clock::now();
			m_args->make_new=true;
			++(m_args->monitoring_vars->in_buffer_transfers);
			
		}
		
	}
	
	// poll
	// ====
	try {
		
		std::unique_lock<std::mutex> locker(*m_args->socket_mtx);
		m_args->get_ok = zmq::poll(&m_args->poll, 1, m_args->poll_timeout_ms);
		
		if(m_args->get_ok<0){
			std::cerr<<m_args->m_tool_name<<" poll failed with "<<zmq_strerror(errno)<<std::endl;
			++(m_args->monitoring_vars->polls_failed);
//			m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
			return;
		}
		
	} catch(zmq::error_t& err){
		// ignore poll aborting due to signals
		if(zmq_errno()==EINTR) return;
		//std::cerr<<m_args->m_tool_name<<" poll caught "<<err.what()<<std::endl; FIXME re-enable
		++(m_args->monitoring_vars->polls_failed);
//		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		return;
	} catch(std::exception& err){
		std::cerr<<m_args->m_tool_name<<" poll caught "<<err.what()<<std::endl;
		++(m_args->monitoring_vars->polls_failed);
//		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		return;
	} catch(...){
		std::cerr<<m_args->m_tool_name<<" poll caught "<<strerror(errno)<<std::endl;
		++(m_args->monitoring_vars->polls_failed);
//		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		return;
	}
	
	// read
	// ====
	if(m_args->poll.revents & ZMQ_POLLIN){
		std::clog<<m_args->m_tool_name<<": receiving message"<<std::endl;
		
		if(m_args->make_new){
			m_args->in_local_queue->queries.emplace_back(); // FIXME we could resize(local_buffer_size) on retreive new
			m_args->make_new = false;               // then resize down to actual size on transfer out
		}
		ZmqQuery& msg_buf = m_args->in_local_queue->queries.back();
		msg_buf.parts.resize(4);
		// received parts are [topic, client, msg_id, query]
		// reorder parts on receipt as client and msg_id will be left untouched and re-used for response
		static constexpr char part_order[4] = {2,0,1,3};
		
		try {
			
			// receive expected 4 parts
			std::unique_lock<std::mutex> locker(*m_args->socket_mtx);
			for(m_args->msg_parts=0; m_args->msg_parts<4; ++m_args->msg_parts){
				
				m_args->get_ok = m_args->socket->recv(&msg_buf[part_order[m_args->msg_parts]]);
				
				if(!m_args->get_ok){
					std::cerr<<m_args->m_tool_name<<" receive failed with "<<zmq_strerror(errno)<<std::endl;
					++(m_args->monitoring_vars->rcv_fails);
//					m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
					break;
				}
				
				if(!msg_buf[part_order[m_args->msg_parts]].more()) break;
				
			}
			
			// if there are more than 4 parts, read the remainder to flush the buffer, but discard the message
			if(m_args->get_ok && msg_buf[3].more()){
				while(true){
					m_args->socket->recv(&m_args->msg_discard);
					++m_args->msg_parts;
				}
				std::cerr<<m_args->m_tool_name<<": Unexpected "<<m_args->msg_parts<<" part message"<<std::endl;
				// FIXME print other info we have (client, message, parts) to help identify culprit
				// FIXME do we do this? for efficiency? here? do we add a flag for bad and do it in the processing?
				// FIXME do we try to make a query out of the first 4 parts? i'm gonna say no, for now
				// pass of as fail job
				++(m_args->monitoring_vars->bad_msgs);
				return;
			}
			
			// if receive failed, discard the message
			if(!m_args->get_ok){
				std::cerr<<m_args->m_tool_name<<": receive failed with "<<zmq_strerror(errno)<<std::endl;
				++(m_args->monitoring_vars->rcv_fails);
				return;
			}
			
			// else success
			m_args->make_new=true;
			++(m_args->monitoring_vars->msgs_rcvd);
			
		} catch(zmq::error_t& err){
			// receive aborted due to signals?
			if(zmq_errno()==EINTR) return; // FIXME is this appropriate here?
			std::cerr<<m_args->m_tool_name<<" receive caught "<<err.what()<<std::endl;
			++(m_args->monitoring_vars->rcv_fails);
//			m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
			return;
		} catch(std::exception& err){
			std::cerr<<m_args->m_tool_name<<" receive caught "<<err.what()<<std::endl;
			++(m_args->monitoring_vars->rcv_fails);
//			m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
			return;
		} catch(...){
			std::cerr<<m_args->m_tool_name<<" receive caught "<<strerror(errno)<<std::endl;
			++(m_args->monitoring_vars->rcv_fails);
//			m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
			return;
		}
		
		
	} // else no messages from clients
	
	return;
}

/*
bool WriteQueryReceiver::Promote(){
	// FIXME TODO if using a standby, need to connect to clients
}

bool WriteQueryReceiver::Demote(){
	// FIXME TODO if using a standby, need to disconnect from clients
	// (to prevent zmq buffering messages, and avoid load of unnecessarily reading them)
}
*/
