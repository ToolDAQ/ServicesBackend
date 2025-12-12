#include "ReadReceiverReplySender.h"

ReadReceiverReplySender::ReadReceiverReplySender():Tool(){}

//FIXME call it readqueryreceviverandreplysender
bool ReadReceiverReplySender::Initialise(std::string configfile, DataModel &data){
	
	if(configfile!="")  m_variables.Initialise(configfile);
	//m_variables.Print();
	
	m_data= &data;
	m_log= m_data->Log;
	
	if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;
	
	/* ----------------------------------------- */
	/*               Configuration               */
	/* ----------------------------------------- */
	
	port_name = "db_read";
	// FIXME do these timeouts need to be << transfer_period_ms?
	int rcv_timeout_ms=500;
	int snd_timeout_ms=500;
	int poll_timeout_ms=500;
	int rcv_hwm=10000;
	int conns_backlog=1000; // FIXME sufficient?
	int local_buffer_size = 200;
	int transfer_period_ms = 200;
	
	m_variables.Get("snd_timeout",snd_timeout_ms);
	m_variables.Get("rcv_timeout",rcv_timeout_ms);
	m_variables.Get("poll_timeout_ms",poll_timeout_ms);
	m_variables.Get("port_name", port_name);
	m_variables.Get("rcv_hwm", rcv_hwm); // max num outstanding messages in receive buffer
	m_variables.Get("conns_backlog", conns_backlog); // max num oustanding connection requests
	m_variables.Get("local_buffer_size", local_buffer_size);
	m_variables.Get("transfer_period_ms", transfer_period_ms);
	
	/* ----------------------------------------- */
	/*               Socket Setup                */
	/* ----------------------------------------- */
	
	// A ROUTER socket is used for read queries as it naturally load balances
	// (since read queries can be handled by both master/slave middlemen and will be round-robined between them)
	// and is also used to asynchronously send both read and write query acknowledgements/replies
	
	ManagedSocket* managed_socket = new ManagedSocket;
	managed_socket->service_name=""; // attach to any client type...
	managed_socket->port_name = port_name; // ...that advertises a service on port 'port_name'
	managed_socket->socket = new zmq::socket_t(*m_data->context, ZMQ_ROUTER);
	managed_socket->socket->setsockopt(ZMQ_SNDTIMEO, snd_timeout_ms);
	managed_socket->socket->setsockopt(ZMQ_RCVTIMEO, rcv_timeout_ms);
	managed_socket->socket->setsockopt(ZMQ_RCVHWM,rcv_hwm);
	managed_socket->socket->setsockopt(ZMQ_BACKLOG,conns_backlog);
	managed_socket->socket->setsockopt(ZMQ_LINGER, 10);
	// make reply socket error, rather than silently drop, if the destination is unreachable
	managed_socket->socket->setsockopt(ZMQ_ROUTER_MANDATORY, 1); // FIXME do we want this?
	// make router transfer connections with an already seen ZMQ_IDENTITY to a new connection
	// rather than rejecting the new connection attempt
	// FIXME need to update ZMQ version to enable, but we should do this
	/*
	try{
		managed_socket->socket->setsockopt(ZMQ_ROUTER_HANDOVER, 1);
	} catch(std::exception& e){
		std::cout<<"caught "<<e.what()<<" in setsockopt "<<ZMQ_ROUTER_HANDOVER<<std::endl;
		throw;
	}
	*/
	
	// make items to poll the input and output sockets
	
	/* ----------------------------------------- */
	/*               Thread Setup                */
	/* ----------------------------------------- */
	
	thread_args.m_data = m_data;
	thread_args.socket = managed_socket->socket; // FIXME get from struct. 
	thread_args.socket_mtx = managed_socket->socket_mtx; // FIXME get from struct. For sharing socket with SocketManager
	thread_args.poll_timeout_ms = poll_timeout_ms;
	thread_args.polls.emplace_back(*socket,0,ZMQ_POLLIN,0);
	thread_args.polls.emplace_back(*socket,0,ZMQ_POLLOUT,0);
	thread_args.in_local_queue = m_data->querybatch_pool.GetNew();
	thread_args.in_local_queue.reserve(local_buffer_size);
	thread_args.local_buffer_size = local_buffer_size;
	thread_args.transfer_period_ms = transfer_period_ms;
	
	// add the socket to the datamodel for the SocketManager, which will handle making new connections to clients
	std::unique_lock<std::mutex> locker(m_data->managed_sockets_mtx);
	m_data->managed_sockets[port_name] = managed_socket;
	locker.unlock();
	
	m_args->in_local_queue = m_data->rdmsg_buffer_pool.GetNew(local_buffer_size);
	thread_args.make_new = true;
	m_data->utils.CreateThread("readrep_sendreceiver", &Thread, &thread_args); // thread needs a unique name
	m_data->num_threads++;
	
	return true;
}


bool ReadReceiverReplySender::Execute(){
	
	if(!thread_args.running){
		Log(m_tool_name+" Execute found thread not running!",v_error);
		Finalise();
		Initialise(); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++m_data->read_rcv_thread_crashes;
	}
	
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


bool ReadReceiverReplySender::Finalise(){
	
	// signal background receiver thread to stop
	Log(m_tool_name+": Joining receiver thread",v_warning);
	m_data->utils.KillThread(&thread_args);
	Log(m_tool_name+": Finished",v_warning);
	m_data->num_threads--;
	
	// FIXME ensure we don't interfere with SocketManager? Better to leave that to do deletion in its destructor?
	/*
	if(managed_socket->socket){
		std::unique_lock<std::mutex> lock(managed_socket->socket_mtx);
		delete managed_socket->socket;
		managed_socket->socket=nullptr;
	}
	*/
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

void ReadReceiverReplySender::Thread(Thread_args* args){
	
	ReadReceiverReplySender_args* m_args = reinterpret_cast<ReadReceiverReplySender_args*>(args);
	
	// transfer to datamodel
	// =====================
	if(m_args->in_local_queue.size() >= m_args->local_buffer_size ||
	  (m_args->last_transfer - std::chrono<steady_clock>now()) > transfer_period_ms){
		
		if(!make_new) pop_back();
		if(!m_args->in_local_queue.empty()){
			
			std::unique_lock<std::mutex> locker(m_args->m_data->read_msg_queue_mtx);
			m_args->m_data->read_msg_queue.push_back(m_args->in_local_queue);
			locker.unlock();
			
			m_args->in_local_queue = m_args->m_data->querybatch_pool.GetNew();
			m_args->in_local_queue.reserve(m_args->local_buffer_size);
			
			m_args->m_data->Log(m_tool_name+": added "+std::to_string(next_index)
				                +" messages to datamodel",5); // FIXME better logging
			m_args->last_transfer = std::chrono<steady_clock>now();
			m_args->make_new=true;
			++(*m_args->m_data->readrep_in_buffer_transfers);
			
		}
	}
	
	// poll
	// ====
	try {
		std::unique_lock<std::mutex> lock(m_args->socket_mtx);
		get_ok = zmq::poll(&m_args->polls, 2, m_args->poll_timeout_ms);
	} catch(zmq::error_t& err){
		// ignore poll aborting due to signals
		if(zmq_errno()==EINTR) return;
		std::cerr<<m_tool_name<<" poll caught "<<err.what()<<std::endl; // FIXME better logging
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		++(*m_args->m_data->readrep_polls_failed);
		return;
	} // FIXME catch non-zmq errors? can we handle them any better?
	catch(...){
		std::cerr<<m_tool_name<<" poll caught "<<strerror(errno)<<std::endl;
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		++(*m_args->m_data->readrep_polls_failed);
		return;
	}
	if(get_ok<0){
		std::cerr<<m_tool_name<<" poll caught "<<zmq_strerror(errno)<<std::endl;
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		++(*m_args->m_data->readrep_polls_failed);
		return;
	}
	
	// read
	// ====
	if(m_args->polls[0].revents & ZMQ_POLLIN){
		m_data->Log(">>> got a read query from client",3); // FIXME better logging
		
		if(m_args->make_new){
			m_args->in_local_queue.emplace_back();
			m_args->make_new = false;
		}
		ZmqQuery& msg_buf = m_args->in_local_queue.back().queries;
		msg_buf.resize(4);
		// received parts are [client, topic, msg_id, query]
		// reorder parts on receipt as client and msg_id will be left untouched and re-used for response
		static constexpr char part_order[4] = {0,2,1,3};
		
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
			++(*m_args->m_data->readrep_rcv_fails);
			return;
		}
		
		// if there weren't 4 parts, discard the message
		if(m_args->msg_parts!=4){
			std::cerr<<m_tool_name<<": Unexpected "<<m_args->msg_parts<<" part message"<<std::endl;
			// FIXME print other info we have (client, message, parts) to help identify culprit
			// FIXME do we do this? for efficiency? here? do we add a flag for bad and do it in the processing?
			// FIXME do we try to make a query out of the first 4 parts? i'm gonna say no, for now
			++(*m_args->m_data->readrep_bad_msgs);
			return;
		}
		
		// else success
		m_args->make_new=true;
		++(*m_args->m_data->readrep_msgs_rcvd);
		
	} // else no messages from clients
	
	// write
	// =====
	m_args->m_data->Log("Size of reply queue is "+
	    (m_args->out_local_queue ? std::to_string(m_args->out_local_queue.size()) : std::string{"0"}),10); // FIXME
	
	// send next response message, if we have one in the queue
	if(!m_args->out_local_queue!=nullptr && m_args->out_i<m_args->out_local_queue->queries.size()){
		
		// check we had a listener ready
		if(m_args->polls[1].revents & ZMQ_POLLOUT){
			
			ZmqQuery& rep = m_args->out_local_queue->queries[m_args->out_i++];
			// FIXME maybe don't pop (increment out_i) until send succeeds?
			// FIXME maybe impelement 'retries' mechanism as previously?
			
			std::unique_lock<std::mutex> locker(m_args->socket_mtx);
			try {
				for(size_t i=0; i<rep.size()-1; ++i){
						m_args->get_ok = m_args->socket->send(rep[i], ZMQ_SNDMORE);
						if(!m_args->get_ok) break;
				}
				if(m_args->get_ok) m_args->get_ok = m_args->socket->send(rep[rep.size()-1]);
			} catch(zmq::exception_t& e){
				std::cerr<<m_tool_name<<": Error sending reply '"<<e.what()<<std::endl;
				break;
			}
			locker.unlock();
			
			if(m_args->get_ok){
				// remove from the to-send queue
				++(*m_args->m_data->readrep_reps_sent);
				//m_args->out_local_queue.pop_front();  // FIXME if we didn't do it before
				
			} else {
				std::cerr<<m_tool_name<<": error sending acknowledgement message!"<<std::endl; // FIXME
				++(*m_args->m_data->readrep_rep_send_fails); // FIXME or move into below if we retry? or track both?
				/*
				if(next_msg.retries>=max_send_attempts){
					resp_queue.erase(resp_queue.begin()->first);
				} else {
					++next_msg.retries;
				}
				*/
			}
			
		} // else no available listeners
		
	} else {
		
		// no responses to send - see if there's any in the DataModel
		std::unique_lock<std::mutex> locker(m_args->m_data->query_replies_mtx);
		if(!m_args->m_data->query_replies.empty()){
			
			// return our batch to the pool if applicable
			if(m_args->out_local_queue!=nullptr){
				m_args->m_data->querybatch_pool.Add(m_args->out_local_queue);
				m_args->out_local_queue = nullptr;
			}
			
			// grab a new batch
			m_args->out_local_queue = m_args->m_data->query_replies.front();
			m_args->m_data->query_replies.pop_front();
			
			*(m_args->m_data->readrep_out_buffer_transfers;
			
			// start sending from the beginning
			m_args->out_i=0;
		}
		locker.unlock();
		
		
	}
	
	return true;
}
