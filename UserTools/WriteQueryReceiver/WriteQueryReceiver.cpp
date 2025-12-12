#include "WriteQueryReceiver.h"

WriteQueryReceiver::WriteQueryReceiver():Tool(){}


bool WriteQueryReceiver::Initialise(std::string configfile, DataModel &data){
	
	if(configfile!="")  m_variables.Initialise(configfile);
	//m_variables.Print();
	
	m_data= &data;
	m_log= m_data->Log;
	
	/* ----------------------------------------- */
	/*               Configuration               */
	/* ----------------------------------------- */
	
	bool am_master = true; // FIXME not sure being used any more
	m_verbose=1;
	port_name = "db_write";
	// FIXME do these timeouts need to be << transfer_period_ms?
	int rcv_timeout_ms = 500;
	int poll_timeout_ms = 500;
	int transfer_period_ms = 200;
	int local_buffer_size = 200;
	
	m_variables.Get("verbose",m_verbose);
	m_variables.Get("rcv_timeout_ms",rcv_timeout_ms);
	m_variables.Get("poll_timeout_ms",poll_timeout_ms);
	m_variables.Get("port_name", port_name);
	m_variables.Get("am_master", am_master);
	m_variables.Get("local_buffer_size", local_buffer_size);
	m_variables.Get("transfer_period_ms", transfer_period_ms);
	
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
	managed_socket->socket->setsockopt(ZMQ_RCVHWM,10000); // TODO are these sufficient?
	managed_socket->socket->setsockopt(ZMQ_BACKLOG,1000); // TODO any other options?
	
	// add the socket to the datamodel for the SocketManager, which will handle making new connections to clients
	std::unique_lock<std::mutex> locker(m_data->managed_sockets_mtx);
	m_data->managed_sockets[port_name] = managed_socket;
	locker.unlock();
	
	/* ----------------------------------------- */
	/*               Thread Setup                */
	/* ----------------------------------------- */
	
	thread_args.m_data = m_data;
	thread_args.socket = managed_socket->socket;
	thread_args.socket_mtx = managed_socket->socket_mtx; // for sharing socket with SocketManager
	thread_args.poll_timeout_ms = poll_timeout_ms;
	thread_args.poll = zmq::pollitem_t{NULL, socket, ZMQ_POLLIN, 0};
	thread_args.in_local_queue = m_data->querybatch_pool.GetNew();
	thread_args.in_local_queue.reserve(local_buffer_size);
	thread_args.local_buffer_size = local_buffer_size;
	thread_args.transfer_period_ms = transfer_period_ms;
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
		Initialise(); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++m_data->pub_rcv_thread_crashes;
	}
	
	/*
	FIXME are we doing this
	if(m_data->am_master != am_master_last){
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
	Log(m_tool_name+": Finished",v_warning);
	m_data->num_threads--;
	
	if(m_data->managed_sockets.count(port_name)){
		std::unique_lock<std::mutex> lock(m_data->managed_sockets_mtx);
		ManagedSocket* sock = m_data->managed_sockets[port_name];
		m_data->managed_sockets.erase(port_name);
		locker.unlock();
		if(sock->socket) delete sock->socket; // destructor closes socket
		delete sock;
	}
	
	return true;
}

void WriteQueryReceiver::Thread(Thread_args* args){
	
	WriteQueryReceiver_args* m_args = reinterpret_cast<WriteQueryReceiver_args*>(args);
	
	// transfer to datamodel
	// =====================
	if(m_args->in_local_queue->size() >= m_args->local_buffer_size ||
	  (m_args->last_transfer - std::chrono<steady_clock>now()) > transfer_period_ms){
		
		if(!make_new) pop_back();
		if(!m_args->in_local_queue->empty()){
			
			std::unique_lock<std::mutex> locker(m_args->m_data->write_msg_queue_mtx);
			m_args->m_data->write_msg_queue.push_back(m_args->in_local_queue);
			locker.unlock();
			
			m_args->in_local_queue = m_args->m_data->querybatch_pool.GetNew();
			m_args->in_local_queue.reserve(m_args->local_buffer_size);
			
			m_args->m_data->Log(m_tool_name+": added "+std::to_string(next_index)
				                +" messages to datamodel",5); // FIXME better logging
			m_args->last_transfer = std::chrono<steady_clock>now();
			m_args->make_new=true;
			++(*m_args->m_data->write_buffer_transfers);
			
		}
		
	}
	
	// poll
	// ====
	try {
		std::unique_lock<std::mutex> lock(m_args->socket_mtx);
		m_args->get_ok = zmq::poll(&m_args->poll, 1, m_args->poll_timeout_ms);
	} catch(zmq::error_t& err){
		// ignore poll aborting due to signals
		if(zmq_errno()==EINTR) return;
		std::cerr<<m_tool_name<<" poll caught "<<err.what()<<std::endl; // FIXME better logging
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		++(*m_args->m_data->write_polls_failed);
		return;
	} // FIXME catch non-zmq errors? can we handle them any better?
	catch(...){
		std::cerr<<m_tool_name<<" poll caught "<<strerror(errno)<<std::endl;
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		++(*m_args->m_data->write_polls_failed);
		return;
	}
	if(m_args->get_ok<0){
		std::cerr<<m_tool_name<<" poll caught "<<zmq_strerror(errno)<<std::endl;
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		++(*m_args->m_data->write_polls_failed);
		return;
	}
	
	// read
	// ====
	if(m_args->poll.revents & ZMQ_POLLIN){
		m_data->Log(m_tool_name+": got a write query from client",v_debug);
		
		if(m_args->make_new){
			m_args->in_local_queue->emplace_back(); // FIXME we could resize(local_buffer_size) on retreive new
			m_args->make_new = false;               // then resize down to actual size on transfer out
		}
		ZmqQuery& msg_buf = m_args->in_local_queue->back().queries;
		msg_buf.resize(4);
		// received parts are [topic, client, msg_id, query]
		// reorder parts on receipt as client and msg_id will be left untouched and re-used for response
		static constexpr char part_order[4] = {2,0,1,3};
		
		std::unique_lock<std::mutex> locker(m_args->socket_mtx);
		for(m_args->msg_parts=0; m_args->msg_parts<4; ++m_args->msg_parts){
			m_args->get_ok = m_args->socket->recv(&msg_buf[part_order[m_args->msg_parts]]);
			if(!m_args->get_ok || !msg_buf[part_order[m_args->msg_parts]].more()) break;
		}
		
		// if there are more than 4 parts, read the remainder to flush the buffer, but discard the message
		if(m_args->get_ok && msg_buf[3].more()){
			while(true){
				m_args->socket->recv(&m_args->msg_discard);
				++m_args->msg_parts;
			}
		}
		locker.unlock();
		
		// if the read failed, discard the message
		if(!m_args->get_ok){
			std::cerr<<m_tool_name<<": Error receiving message part "<<m_args->msg_parts<<std::endl; // FIXME better logging
			++(*m_args->m_data->write_rcv_fails);
			return;
		}
		
		// if there weren't 4 parts, discard the message
		if(m_args->msg_parts!=4){
			std::cerr<<m_tool_name<<": Unexpected "<<m_args->msg_parts<<" part message"<<std::endl;
			// FIXME print other info we have (client, message, parts) to help identify culprit
			// FIXME do we do this? for efficiency? here? do we add a flag for bad and do it in the processing?
			// FIXME do we try to make a query out of the first 4 parts? i'm gonna say no, for now
			// pass of as fail job
			++(*m_args->m_data->write_bad_msgs);
			return;
		}
		
		// else success
		make_new=true;
		++(*m_args->m_data->write_msgs_rcvd);
		
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
