#include "MulticastReceiverSender.h"

MulticastReceiverSender::MulticastReceiverSender():Tool(){}


bool MulticastReceiverSender::Initialise(std::string configfile, DataModel &data){
	
	if(configfile!="")  m_variables.Initialise(configfile);
	//m_variables.Print();
	
	m_data= &data;
	m_log= m_data->Log;
	
	/* ----------------------------------------- */
	/*               Configuration               */
	/* ----------------------------------------- */
	
	m_verbose=1;
	std::string type_str;  // "logging" or "monitoring"
	int port = 5000; // shared with service discovery, logging and monitoring
	std::string multicast_address; // separate for each
	// FIXME slow controls to vary them
	int local_buffer_size = 100;
	int transfer_ms = 1000;
	int poll_timeout_ms = 100;
	
	m_variables.Get("type",type_str);
	if(type_str!="logging" && type_str!="monitoring"){
		Log(m_tool_name+": invalid port type '"+type_str+"'; valid values are 'logging' and 'monitoring'",v_error);
		return false;
	}
	m_variables.Get("verbose",m_verbose);
	m_variables.Get("port",port);
	if(!m_variables.Get("multicast_address",multicast_address)){
		if(type_str=="logging") multicast_address = "239.192.1.2";
		else multicast_address = "239.192.1.3";
	}
	// buffer received messages in a local vector until size exceeds local_buffer_size...
	m_variables.Get("local_buffer_size",local_buffer_size);
	// ... or time since last transfer exceeds transfer_ms
	m_variables.Get("transfer_ms",transfer_ms);
	m_variables.Get("poll_timeout_ms",poll_timeout_ms);
	
	/* ----------------------------------------- */
	/*               Socket Setup                */
	/* ----------------------------------------- */
	
	int socket = socket(AF_INET, SOCK_DGRAM, 0);
	if(socket<=0){
		Log(m_tool_name+": Failed to open multicast socket with error "+strerror(errno),v_error);
		return false;
	}
	
	// set linger options - do not linger, discard queued messages on socket close
	struct linger l;
	l.l_onoff  = 0;  // whether to linger
	l.l_linger = 0;  // seconds to linger for
	get_ok = setsockopt(socket, SOL_SOCKET, SO_LINGER, (char*) &l, sizeof(l));
	if(get_ok!=0){
		Log(m_tool_name+": Failed to set multicast socket linger with error "+strerror(errno),v_error);
		return false;
	}
	
	// disable blocking connections to this ip+port fomr TIME_WAIT after closure.
	// this is intended to prevent delivery of delayed packets to the wrong application,
	// but means a new middleman instance won't be able to bind for 30-120 seconds after another closes.
	int a =1;
	get_ok = setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, &a, sizeof(a));
	if(get_ok!=0){
		Log(m_tool_name+": Failed to set multicast socket reuseaddr with error "+strerror(errno),v_error);
		return false;
	}
	
	// set the socket to non-blocking mode - should be irrelevant as we poll
	get_ok = fcntl(socket, F_SETFL, O_NONBLOCK);
	if(get_ok!=0){
		Log(m_tool_name+": Failed to set multicast socket to non-blocking with error "+strerror(errno),v_warning);
	}
	
	// format destination address from IP string
	struct sockaddr_in addr;
	bzero((char *)&addr, sizeof(addr)); // init to 0
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	
	// sending: which multicast group to send to
	get_ok = inet_aton(multicast_address.c_str(), &addr.sin_addr);
	if(get_ok==0){ // returns 0 if invalid, unlike other functions
		Log(m_tool_name+": Bad multicast address '"+multicast_address+"'",v_error);
		return false;
	}
	
	// used in sendto / recvfrom methods
	socklen_t addrlen = sizeof(addr);
	
	/* FIXME FIXME FIXME
	// for two-way comms, we should bind to INADDR_ANY, not a specific multicast address.... maybe?
	struct sockaddr_in multicast_addr2;
	bzero((char *)&multicast_addr2, sizeof(multicast_addr2)); // init to 0
	multicast_addr2.sin_family = AF_INET;
	multicast_addr2.sin_port = htons(log_port);
	multicast_addr2.sin_addr.s_addr = htonl(INADDR_ANY);      << like this
	*/
	
	// to listen we need to bind to the socket
	get_ok = (bind(socket, (struct sockaddr*)&addr, addrlen) == 0);
	if(!get_ok) {
		Log(m_tool_name+": Failed to bind to multicast listen socket",v_error);
		return false;
	}
	
	// and join a multicast group
	struct ip_mreq mreq;
	mreq.imr_interface.s_addr = htonl(INADDR_ANY);
	get_ok = inet_aton(multicast_address.c_str(), &mreq.imr_multiaddr);
	if(get_ok==0){
		Log(m_tool_name+": Bad multicast group '"+multicast_address+"'",v_error);
		return false;
	}
	get_ok = setsockopt(socket, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
	if(get_ok!=0){
		Log(m_tool_name+": Failed to join multicast group",v_error);
		return false;
	}
	
	/* ----------------------------------------- */
	/*               Thread Setup                */
	/* ----------------------------------------- */
	
	thread_args.m_data = m_data;
	thread_args.socket = socket;
	thread_args.addr = addr;
	thread_args.addrlen = addrlen;
	thread_args.poll = zmq::pollitem_t{NULL, socket, ZMQ_POLLIN, 0};
	thread_args.poll_timeout_ms = poll_timeout_ms;
	thread_args.local_buffer_size = local_buffer_size;
	thread_args.in_local_queue = m_data->multicast_buffer_pool.GetNew(local_buffer_size);
	thread_args.last_transfer = std::chrono<steady_clock>now();
	thread_args.transfer_period_ms = std::chrono::milliseconds{transfer_ms};
	thread_args.in_queue = &m_data->in_multicast_msg_queue;
	thread_args.in_queue_mtx = &m_data->in_multicast_msg_queue_mtx;
	if(type_str=="logging"){
		// TODO encapsulate these in a socket-receive monitoring struct, w/ method for turning to json
		// can be shared across multicast and both zmq socket receivers
		{
		thread_args.polls_failed = &m_data->log_polls_failed;
		thread_args.msgs_rcvd = &m_data->logs_recvd;
		thread_args.rcv_fails = &m_data->log_recv_fails;
		thread_args.in_buffer_transfers = &m_data->log_in_buffer_transfers;
		thread_args.out_buffer_transfers = &m_data->log_out_buffer_transfers;
		}
		
		thread_args.out_queue = &m_data->out_log_msg_queue;
		thread_args.out_queue_mtx = &m_data->out_log_msg_queue_mtx;
		
		thread_crashes = &m_data->log_thread_crashes;
	} else {
		{
		thread_args.polls_failed = &m_data->mon_polls_failed;
		thread_args.msgs_rcvd = &m_data->mons_recvd;
		thread_args.rcv_fails = &m_data->mon_recv_fails;
		thread_args.in_buffer_transfers = &m_data->mon_in_buffer_transfers;
		thread_args.out_buffer_transfers = &m_data->mon_out_buffer_transfers;
		}
		
		thread_args.out_queue = &m_data->out_mon_msg_queue;
		thread_args.out_queue_mtx = &m_data->out_mon_msg_queue_mtx;
		
		thread_crashes = &m_data->mon_thread_crashes;
	}
	type_str+="_sendreceiver"; // thread needs a unique name
	m_data->utils.CreateThread(type_str, &Thread, &thread_args);
	m_data->num_threads++;
	
	return true;
}


bool MulticastReceiverSender::Execute(){
	
	if(!thread_args.running){
		Log(m_tool_name+" Execute found thread not running!",v_error);
		Finalise();
		Initialise(); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		// FIXME if restarts > X times in last Y mins, alarm (bypass, shove into DB? send to websocket?) and StopLoop.
		++(*thread_crashes);
	}
	
	return true;
}


bool MulticastReceiverSender::Finalise(){
	
	// signal background receiver thread to stop
	Log(m_tool_name+": Joining receiver thread",v_warning);
	m_data->utils.KillThread(&thread_args);
	Log(m_tool_name+": Finished",v_warning);
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->in_multicast_msg_queue_mtx);
	m_data->in_multicast_msg_queue->clear();
	locker.unlock();
	
	if(type_str=="logging"){
		locker = std::unique_lock<std::mutex>(m_data->out_log_msg_queue_mtx);
		m_data->out_log_msg_queue->clear();
	} else {
		locker = std::unique_lock<std::mutex>(m_data->out_mon_msg_queue_mtx);
		m_data->out_mon_msg_queue->clear();
	}
	
	get_ok = close(socket);
	if(get_ok!=0){
		Log(m_tool_name+": Error closing socket "+strerror(errno),v_error);
		return false;
	}
	
	return true;
}

void ReceiveSQL::Thread(Thread_args* arg){
	
	MulticastReceive_args* m_args=reinterpret_cast<MulticastReceive_args*>(arg);
	DataModel* m_data = m_args->m_data;
	
	// transfer to datamodel
	// =====================
	if(!m_args->in_local_queue->empty() &&
	   ((m_args->in_local_queue->size()>m_args->local_buffer_size) ||
	    (m_args->last_transfer - std::chrono<steady_clock>now()) > transfer_period_ms) ){
		
		std::unique_lock<std::mutex> locker(m_args->in_queue_mtx);
		m_args->in_queue->push_back(m_args->in_local_queue);
		locker.unlock();
		
		m_args->in_local_queue = m_data->multicast_buffer_pool.GetNew(local_buffer_size);
		
		m_args->Log(m_tool_name+": added "+std::to_string(m_args->in_local_queue.size())
		          +" messages to datamodel",5); // FIXME streamline
		m_args->last_transfer = std::chrono<steady_clock>now();
		++(*m_args->in_buffer_transfers);
	}
	
	
	// poll
	// ====
	try {
		get_ok = zmq::poll(&m_args->poll, 1, m_args->poll_timeout_ms);
	} catch(zmq::error_t& err){
		// ignore poll aborting due to signals
		if(zmq_errno()==EINTR) return;
		std::cerr<<m_tool_name<<" poll caught "<<err.what()<<std::endl; // FIXME better logging
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread?
		++(*m_args->polls_failed);
		return;
	}
	catch(...){
		std::cerr<<m_tool_name<<" poll caught "<<strerror(errno)<<std::endl;
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread? or throw?
		++(*m_args->polls_failed);
		return;
	}
	if(get_ok<0){
		std::cerr<<m_tool_name<<" poll caught "<<zmq_strerror(errno)<<std::endl;
		m_args->running=false; // FIXME Handle other errors? or just globally via restarting thread?
		++(*m_args->polls_failed);
		return;
	}
	
	// read
	// ====
	if(m_args->poll.revents & ZMQ_POLLIN){
		m_data->Log(m_tool_name+": reading multicast message",10);  // FIXME streamline
		
		// read the messge        FIXME name max num bytes in multicast message
		m_args->get_ok = recvfrom(m_args->socket, m_args->message, 655355, 0, &m_args->addr, &m_args->addrlen);
		if(m_args->get_ok <= 0){
			++(*m_args->rcv_fails);
			// FIXME better logging
			std::cerr<<m_tool_name<<": Failed to receive message "
			         <<"from "<<std::string{inet_ntoa(&m_args->addr->sin_addr)} // FIXME is this valid on failure?
			         <<" with error "<<strerror(errno)<<std::endl;
			// FIXME error handling - is this a socket error or just a message error?
		} else {
			
			++(*m_args->msgs_rcvd);
			m_data->Log(m_tool_name+": Received multicast message '"+std::string(m_args->message)
			            +"' from "+std::string{inet_ntoa(&m_args->addr->sin_addr)},12); // FIXME streamline
			
			m_args->in_local_queue->emplace_back(m_args->message);
			
		}
	}
	
	// write
	// =====
	if(!m_args->out_local_queue.empty()){
		
		// Get the message
		std::string& message = out_local_queue.front();
		
		// send it
		int cnt = sendto(m_args->socket, message.c_str(), message.length()+1, 0, &m_args->addr, m_args->addrlen);
		
		// check success
		if(cnt < 0){
			m_data->Log(m_tool_name+": Error sending multicast message: "+strerror(errno),v_error); // FIXME ensure this isn't circular
			m_args->out_local_queue.pop_front(); // FIXME discard it anyway? or maybe don't until it succeeds?
			++(*m_args->send_fails);
			
		} else {
			m_args->out_local_queue.pop_front();
			++(*m_args->msgs_sent);
			
		}
		
	} else {
		
		// else see if there are any in datamodel to grab
		std::unique_lock<std::mutex> locker(m_args->out_queue_mtx);
		if(!m_args->out_queue->empty()){
			std::swap(m_args->out_queue, m_args->out_local_queue);
			++(*m_args->out_buffer_transfers);
		}
		locker.unlock();
		
	}
	
	return;
}
