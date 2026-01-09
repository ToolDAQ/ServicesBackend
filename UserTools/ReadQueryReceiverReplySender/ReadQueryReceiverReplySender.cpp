#include "ReadQueryReceiverReplySender.h"

ReadQueryReceiverReplySender::ReadQueryReceiverReplySender():Tool(){}

bool ReadQueryReceiverReplySender::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	//m_variables.Print();
	
	if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;
	
	/* ----------------------------------------- */
	/*               Configuration               */
	/* ----------------------------------------- */
	
	remote_port_name = "db_read";
	// FIXME do these timeouts need to be << transfer_period_ms?
	int rcv_timeout_ms=500;
	int snd_timeout_ms=500;
	int poll_timeout_ms=500;
	int rcv_hwm=10000; // FIXME sufficient?
	int conns_backlog=1000; // FIXME sufficient?
	int local_buffer_size = 200;
	int transfer_period_ms = 200;
	
	m_variables.Get("remote_port_name", remote_port_name);
	m_variables.Get("rcv_hwm", rcv_hwm); // max num outstanding messages in receive buffer
	m_variables.Get("conns_backlog", conns_backlog); // max num oustanding connection requests
	m_variables.Get("poll_timeout_ms",poll_timeout_ms);
	m_variables.Get("snd_timeout_ms",snd_timeout_ms);
	m_variables.Get("rcv_timeout_ms",rcv_timeout_ms);
	m_variables.Get("local_buffer_size", local_buffer_size);
	m_variables.Get("transfer_period_ms", transfer_period_ms);
	
	ExportConfiguration();
	
	/* ----------------------------------------- */
	/*               Socket Setup                */
	/* ----------------------------------------- */
	
	// A ROUTER socket is used for read queries as it naturally load balances
	// (since read queries can be handled by both master/slave middlemen and will be round-robined between them)
	// and is also used to asynchronously send both read and write query acknowledgements/replies
	
	ManagedSocket* managed_socket = new ManagedSocket;
	managed_socket->service_name=""; // attach to any client type...
	managed_socket->remote_port_name = remote_port_name; // ...that advertises a service on port 'remote_port_name'
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
	
	// add the socket to the datamodel for the SocketManager, which will handle making new connections to clients
	std::unique_lock<std::mutex> locker(m_data->managed_sockets_mtx);
	m_data->managed_sockets[remote_port_name] = managed_socket;
	
	/* ----------------------------------------- */
	/*               Thread Setup                */
	/* ----------------------------------------- */
	
	// monitoring struct to encapsulate tracking info
	locker =std::unique_lock<std::mutex>(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.emplace(m_tool_name, &monitoring_vars);
	
	thread_args.m_data = m_data;
	thread_args.m_tool_name = m_tool_name;
	thread_args.monitoring_vars = &monitoring_vars;
	thread_args.socket = managed_socket->socket;
	thread_args.socket_mtx = &managed_socket->socket_mtx;
	thread_args.poll_timeout_ms = poll_timeout_ms;
	thread_args.polls.emplace_back(*managed_socket->socket,0,ZMQ_POLLIN,0);
	thread_args.polls.emplace_back(*managed_socket->socket,0,ZMQ_POLLOUT,0);
	thread_args.in_local_queue = m_data->querybatch_pool.GetNew(local_buffer_size);
	thread_args.make_new = true;
	thread_args.local_buffer_size = local_buffer_size;
	thread_args.transfer_period_ms = std::chrono::milliseconds{transfer_period_ms};
	thread_args.last_transfer = std::chrono::steady_clock::now();
	
	// thread needs a unique name
	if(!m_data->utils.CreateThread("readrep_sendreceiver", &Thread, &thread_args)){
		Log(m_tool_name+": Failed to spawn background thread",v_error,m_verbose);
		return false;
	}
	m_data->num_threads++;
	
	return true;
}


bool ReadQueryReceiverReplySender::Execute(){
	
	if(!thread_args.running){
		Log(m_tool_name+" Execute found thread not running!",v_error);
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++(monitoring_vars.thread_crashes);
	}
	// FIXME add monitoring info: queue sizes
	
	return true;
}


bool ReadQueryReceiverReplySender::Finalise(){
	
	// signal background receiver thread to stop
	Log(m_tool_name+": Joining receiver thread",v_warning);
	m_data->utils.KillThread(&thread_args);
	std::cerr<<"ReadReceiver thread terminated"<<std::endl;
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->managed_sockets_mtx);
	if(m_data->managed_sockets.count(remote_port_name)){
		ManagedSocket* sock = m_data->managed_sockets[remote_port_name];
		m_data->managed_sockets.erase(remote_port_name);
		locker.unlock();
		if(sock->socket) delete sock->socket; // destructor closes socket
		delete sock;
	}
	
	locker = std::unique_lock<std::mutex>(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	Log(m_tool_name+": Finished",v_warning);
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

void ReadQueryReceiverReplySender::Thread(Thread_args* args){
	
	ReadQueryReceiverReplySender_args* m_args = reinterpret_cast<ReadQueryReceiverReplySender_args*>(args);
	
	// transfer to datamodel
	// =====================
	if(m_args->in_local_queue->queries.size() >= m_args->local_buffer_size ||
	  (std::chrono::steady_clock::now() - m_args->last_transfer) > m_args->transfer_period_ms){
		
		if(!m_args->in_local_queue->queries.empty()){
			
			if(!m_args->make_new) m_args->in_local_queue->queries.pop_back();
			
			printf("%s adding %ld messages to datamodel\n",m_args->m_tool_name.c_str(),m_args->in_local_queue->queries.size());
			
			std::unique_lock<std::mutex> locker(m_args->m_data->read_msg_queue_mtx);
			m_args->m_data->read_msg_queue.push_back(m_args->in_local_queue);
			locker.unlock();
			
			m_args->in_local_queue = m_args->m_data->querybatch_pool.GetNew(m_args->local_buffer_size);
			
			m_args->make_new=true;
			++(m_args->monitoring_vars->in_buffer_transfers);
			
		}
		
		m_args->last_transfer = std::chrono::steady_clock::now();
		
	}
	
	// poll
	// ====
	try {
		m_args->get_ok=0;
		std::unique_lock<std::mutex> locker(*m_args->socket_mtx);
		m_args->get_ok = zmq::poll(m_args->polls.data(), 2, m_args->poll_timeout_ms);
	} catch(zmq::error_t& err){
		// ignore poll aborting due to signals
		if(zmq_errno()==EINTR) return; // this is probably fine
		//std::cerr<<m_args->m_tool_name<<" poll caught "<<err.what()<<std::endl; // FIXME re-enable
		++(m_args->monitoring_vars->polls_failed);
//		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		return;
	}
	catch(std::exception& err){
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
	if(m_args->get_ok<0){
		std::cerr<<m_args->m_tool_name<<" poll failed with "<<zmq_strerror(errno)<<std::endl;
		++(m_args->monitoring_vars->polls_failed);
//		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		return;
	}
	
	// read
	// ====
	if(m_args->polls[0].revents & ZMQ_POLLIN){
		printf("%s receiving message\n",m_args->m_tool_name.c_str());
		
		if(m_args->make_new){
			m_args->in_local_queue->queries.emplace_back();
			m_args->make_new = false;
		}
		ZmqQuery& msg_buf = m_args->in_local_queue->queries.back();
		msg_buf.parts.resize(4);
		// received parts are [client, topic, msg_id, query]
		// reorder parts on receipt as client and msg_id will be left untouched and re-used for response
		static constexpr char part_order[4] = {0,2,1,3};
		m_args->msg_parts=0;
		
		try {
			
			std::unique_lock<std::mutex> locker(*m_args->socket_mtx);
			printf("%s receiving part...",m_args->m_tool_name.c_str());
			do {
				m_args->get_ok = m_args->socket->recv(&msg_buf[part_order[std::min(3,m_args->msg_parts++)]]);
				printf("%d=%d (more: %d),...",m_args->msg_parts,m_args->get_ok,msg_buf[part_order[std::min(3,m_args->msg_parts-1)]].more());
			} while(m_args->get_ok && msg_buf[part_order[std::min(3,m_args->msg_parts-1)]].more());
			locker.unlock();
			printf("\n");
			
			// if the read failed, discard the message
			if(!m_args->get_ok){
				
				std::cerr<<m_args->m_tool_name<<" receive failed with "<<zmq_strerror(errno)<<std::endl;
				++(m_args->monitoring_vars->rcv_fails);
				
			// if there weren't 4 parts, discard the message
			} else if(m_args->msg_parts!=4){
				
				std::cerr<<m_args->m_tool_name<<": Unexpected "<<m_args->msg_parts<<" part message"<<std::endl;
				for(int i=0; i<m_args->msg_parts; ++i){
					char msg_str[msg_buf[part_order[i]].size()];
					snprintf(&msg_str[0], msg_buf[part_order[i]].size()+1, "%s", msg_buf[part_order[i]].data());
					printf("\tpart %d: %s\n",i, msg_str);
				}
				// FIXME print other info we have (client, message, parts) to help identify culprit
				// FIXME do we do this? for efficiency? here? do we add a flag for bad and do it in the processing?
				// FIXME do we try to make a query out of the first 4 parts? i'm gonna say no, for now
				++(m_args->monitoring_vars->bad_msgs);
				
			// else success
			} else {
				
				m_args->make_new=true;
				++(m_args->monitoring_vars->msgs_rcvd);
				printf("%s received query %u, '%s' message '%s' into ZmqQuery at %p, %p\n",m_args->m_tool_name.c_str(), msg_buf.msg_id(), msg_buf.topic().data(), msg_buf.msg().data(), &msg_buf, &msg_buf.parts[3]);
				
			}
			
		} catch(zmq::error_t& err){
			// receive aborted due to signals?
			if(zmq_errno()==EINTR) return; // FIXME this is probably not appropriate: should resume receive?
			std::cerr<<m_args->m_tool_name<<" receive caught "<<err.what()<<std::endl;
			++(m_args->monitoring_vars->rcv_fails);
//			m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		} catch(std::exception& err){
			std::cerr<<m_args->m_tool_name<<" receive caught "<<err.what()<<std::endl;
			++(m_args->monitoring_vars->rcv_fails);
//			m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		} catch(...){
			std::cerr<<m_args->m_tool_name<<" receive caught "<<strerror(errno)<<std::endl;
			++(m_args->monitoring_vars->rcv_fails);
//			m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		}
		
	} // else no messages from clients
	
	// write
	// =====
	//m_args->m_data->Log("Size of reply queue is "+
	//    (m_args->out_local_queue ? std::to_string(m_args->out_local_queue.size()) : std::string{"0"}),10);
	
	// send next response message, if we have one in the queue
	if(m_args->out_local_queue!=nullptr && m_args->out_i<m_args->out_local_queue->queries.size()){
		
		// check we had a listener ready
		if(m_args->polls[1].revents & ZMQ_POLLOUT){
			
			printf("%s sending reply %d/%d\n",m_args->m_tool_name.c_str(),m_args->out_i,m_args->out_local_queue->queries.size()); // FIXME better logging
			
			ZmqQuery& rep = m_args->out_local_queue->queries[m_args->out_i++];
			// FIXME maybe don't pop (increment out_i) until send succeeds?
			// FIXME maybe impelement 'retries' mechanism as previously?
			printf("reply to message %u has %d parts\n", rep.msg_id(), rep.size());
			
			try {
				
				std::unique_lock<std::mutex> locker(*m_args->socket_mtx);
				for(size_t i=0; i<rep.size()-1; ++i){
						m_args->get_ok = m_args->socket->send(rep[i], ZMQ_SNDMORE);
						if(!m_args->get_ok) break;
				}
				if(m_args->get_ok) m_args->get_ok = m_args->socket->send(rep[rep.size()-1]);
				locker.unlock();
				
				if(!m_args->get_ok){
					std::cerr<<m_args->m_tool_name<<": send failed with "<<zmq_strerror(errno)<<std::endl;
					++(m_args->monitoring_vars->send_fails); // FIXME or move into below if we retry? or track both?
					/*
					if(next_msg.retries>=max_send_attempts){
						resp_queue.erase(resp_queue.begin()->first);
					} else {
						++next_msg.retries;
					}
					*/
					return;
				}
				// FIXME if we do implement re-sending, then do not do this
				rep.parts.resize(0); // safety to prevent accidentally accessing sent messages, which can segfault
				
				// else success
				printf("%s reply at %p sent\n",m_args->m_tool_name.c_str(), &rep);
				++(m_args->monitoring_vars->msgs_sent);
				
			} catch(zmq::error_t& err){
				// send aborted due to signals?
				if(zmq_errno()==EINTR) return; // FIXME is this appropriate here?
				std::cerr<<m_args->m_tool_name<<" send caught "<<err.what()<<std::endl; // FIXME better logging
				++(m_args->monitoring_vars->send_fails);
//				m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
			} catch(std::exception& e){
				std::cerr<<m_args->m_tool_name<<" send caught "<<e.what()<<std::endl;
				++(m_args->monitoring_vars->send_fails);
//				m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
			} catch(...){
				std::cerr<<m_args->m_tool_name<<" send caught "<<strerror(errno)<<std::endl;
				++(m_args->monitoring_vars->send_fails);
//				m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
			}
			
		} // else no available listeners
		
	} else {
		
		// no responses to send - see if there's any in the DataModel
		std::unique_lock<std::mutex> locker(m_args->m_data->query_replies_mtx);
		if(!m_args->m_data->query_replies.empty()){
			
			printf("%s fetching new replies\n",m_args->m_tool_name.c_str());
			
			// return our batch to the pool if applicable
			if(m_args->out_local_queue!=nullptr){
				m_args->out_local_queue->queries.clear();
				m_args->m_data->querybatch_pool.Add(m_args->out_local_queue);
				m_args->out_local_queue = nullptr;
			}
			
			// grab a new batch
			m_args->out_local_queue = m_args->m_data->query_replies.front();
			m_args->m_data->query_replies.pop_front();
			
			++(m_args->monitoring_vars->out_buffer_transfers);
			
			// start sending from the beginning
			m_args->out_i=0;
		}
		locker.unlock();
		
	}
	
	return;
}
