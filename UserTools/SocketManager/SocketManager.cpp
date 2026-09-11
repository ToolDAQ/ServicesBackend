#include "SocketManager.h"

SocketManager::SocketManager():Tool(){}


bool SocketManager::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	logger = m_data->logger;
	//m_variables.Print();
	
	m_verbose=1;
	int update_ms=2000;
	
	m_variables.Get("verbose",m_verbose);
	m_variables.Get("update_ms",update_ms);
	
	ExportConfiguration();
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.emplace(m_tool_name, &monitoring_vars);
	
	// for doing UpdateConnections
	daq_utils = DAQUtilities(m_data->context);
	
	thread_args.m_data = m_data;
	thread_args.monitoring_vars = &monitoring_vars;
	thread_args.daq_utils = &daq_utils;
	thread_args.update_period_ms = std::chrono::milliseconds{update_ms};
	thread_args.last_update = std::chrono::steady_clock::now();
	thread_args.new_clients = &new_clients;
	thread_mtx.lock();
	thread_args.thread_mtx = &thread_mtx;
	
	if(!m_data->utils.CreateThread("socket_manager", &Thread, &thread_args)){
		LOG(logger,LOG_ERR,"Failed to spawn %s background thread",m_tool_name.c_str());
		return false;
	}
	m_data->num_threads++;
	
	m_data->sc_vars.Add("Clients", SlowControlElementType::INFO, nullptr, nullptr); // INFO type doesnt need read fnct
	m_data->sc_vars.Add("ClearClients", SlowControlElementType::BUTTON,
	                    std::bind(&SocketManager::ClearClients, this, std::placeholders::_1), nullptr);
	m_data->sc_vars.Add("DisconnectAllClients", SlowControlElementType::BUTTON,
	                    std::bind(&SocketManager::DisconnectAllClients, this, std::placeholders::_1), nullptr);
	m_data->sc_vars.Add("DisconnectClient", SlowControlElementType::COMMAND,
	                    std::bind(&SocketManager::DisconnectClient, this, std::placeholders::_1), nullptr);
	
	return true;
}


bool SocketManager::Execute(){
	
	if(!thread_args.running){
		LOG(logger,LOG_ERR,"%s Execute found thread not running!",m_tool_name.c_str());
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++(monitoring_vars.thread_crashes);
	}
	
	auto time_now = std::chrono::steady_clock::now();
	auto time_since_last = time_now - last_exec;
	if(time_since_last < std::chrono::milliseconds(1000)) return true;
	last_exec = time_now;
	
	// updating monitoring slow control of connected clients
	if(new_clients){
		// need to rebuild the list of clients
		new_clients=false;
		clientsmap.clear();
		bool first=true;
		std::map<std::string, std::string>::iterator it;
		
		std::shared_lock<std::shared_mutex> container_locker(m_data->managed_sockets_mtx);
		for(std::pair<const std::string&, ManagedSocket*> mgd_sock : m_data->managed_sockets){
			
			ManagedSocket* sock = mgd_sock.second;
			std::unique_lock<std::mutex> locker2(sock->connections_mtx);
			
			for(std::pair<const std::string, Store*>& aservice : sock->connections){
				
				std::string client_name = aservice.second->Get<std::string>("msg_value");
				std::string client_ip = aservice.second->Get<std::string>("ip");
				std::string client_port = aservice.second->Get<std::string>(sock->remote_port_name);
				std::string client_uuid = aservice.second->Get<std::string>("uuid");
				
				LOG(logger,LOG_INFO, "%s connection to client application '%s' with uuid '%s' at ip '%s' on port '%s'",
				    sock->remote_port_name.c_str(), client_name.c_str(), client_uuid.c_str(),
				    client_ip.c_str(), client_port.c_str());
				
				// we want to group by application
				// a given application will have a single client_name, IP and UUID, so bundle these
				std::string client_key = client_name+"["+client_uuid+"]@"+client_ip;
				
				// an application may have multiple connection types on different ports
				std::string client_conn = sock->remote_port_name+" ("+client_port+")";
				if(!first) it = clientsmap.find(client_key);
				if(first || it==clientsmap.end()){
					LOG(logger,LOG_NOTICE,"adding new %s client: '%s' with connection '%s'",
					    sock->remote_port_name.c_str(),client_key.c_str(), client_conn.c_str());
					clientsmap.emplace(client_key, client_conn);
				} else {
					LOG(logger,LOG_NOTICE,"updating %s client '%s', adding connection '%s'",
					    sock->remote_port_name.c_str(),client_key.c_str(),client_conn.c_str());
					it->second += "; "+client_conn; // FIXME , gets replaced by . on web, for now...
				}
			}
			first=false;
			
		}
		
		std::string clientlist;
		for(std::pair<const std::string,std::string>& aclient : clientsmap){
			if(!clientlist.empty()) clientlist+="\n";
			clientlist += aclient.first+": "+aclient.second;
		}
		if(clientlist.size()>0){
			// if client list is non-empty set as slow control indicator
			m_data->sc_vars["Clients"]->SetValue(clientlist);
			std::string recheck = m_data->sc_vars["Clients"]->GetValue<std::string>();
		}
		
	}
	
	return true;
}


bool SocketManager::Finalise(){
	
	// signal job distributor thread to stop
	LOG(logger,LOG_NOTICE,"%s Joining background thread",m_tool_name.c_str());
	thread_args.running=false;
	thread_mtx.unlock();
	m_data->utils.KillThread(&thread_args);
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	LOG(logger,LOG_NOTICE,"%s Finished",m_tool_name.c_str());
	return true;
}

void SocketManager::Thread(Thread_args* args){
	
	SocketManager_args* m_args = dynamic_cast<SocketManager_args*>(args);
	
	//printf("SocketManager checking for new clients after %lu ms\n",std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-m_args->last_update).count());
	m_args->last_update = std::chrono::steady_clock::now();
	
	std::shared_lock<std::shared_mutex> container_locker(m_args->m_data->managed_sockets_mtx);
	for(std::pair<const std::string&, ManagedSocket*> mgd_sock : m_args->m_data->managed_sockets){
		
		ManagedSocket* sock = mgd_sock.second;
		
		std::unique_lock<std::mutex> locker(sock->socket_mtx, std::defer_lock);
		if(!locker.try_lock()){
			sock->socket_manager_request=true;
			locker.lock();
			sock->socket_manager_request=false;
		}
		std::unique_lock<std::mutex> locker2(sock->connections_mtx);
		
		int new_conn_count = std::abs((long long int)sock->connections.size() - m_args->daq_utils->UpdateConnections(sock->service_name, sock->socket, sock->connections, "", sock->remote_port_name));
		locker.unlock();
		
		if(new_conn_count!=0){
			LOG(m_args->m_data->logger,LOG_NOTICE, "%d new %s connections made!",new_conn_count, sock->remote_port_name.c_str());
			*m_args->new_clients = true;
		}
		
	}
	container_locker.unlock();
	
	//std::this_thread::sleep_until(m_args->last_update+m_args->update_period_ms);
	std::unique_lock<std::timed_mutex> timed_locker(*m_args->thread_mtx, std::defer_lock);
	timed_locker.try_lock_until(m_args->last_update+m_args->update_period_ms);
	
	return;
	
}

std::string SocketManager::ClearClients(const char*){
	// not sure if a good idea, but clear the set of connections to re-invoke 'connect' in UpdateConnections
	std::shared_lock<std::shared_mutex> container_locker(m_data->managed_sockets_mtx);
	for(std::pair<const std::string&, ManagedSocket*> mgd_sock : m_data->managed_sockets){
		ManagedSocket* sock = mgd_sock.second;
		std::unique_lock<std::mutex> locker2(sock->connections_mtx);
		sock->connections.clear();
	}
	clientsmap.clear();
	return "clients cleared";
}

std::string SocketManager::DisconnectAllClients(const char*){
	
	LOG(logger,LOG_WARNING,"SocketManager kicking all clients!");
	
	std::shared_lock<std::shared_mutex> container_locker(m_data->managed_sockets_mtx);
	for(std::pair<const std::string&, ManagedSocket*> mgd_sock : m_data->managed_sockets){
		
		ManagedSocket* sock = mgd_sock.second;
		std::unique_lock<std::mutex> socket_locker(sock->socket_mtx, std::defer_lock);
		if(!socket_locker.try_lock()){
			sock->socket_manager_request=true;
			socket_locker.lock();
			sock->socket_manager_request=false;
		}
		std::unique_lock<std::mutex> connections_locker(sock->connections_mtx);
		
		for(std::pair<const std::string, Store*>& aservice : sock->connections){
			
			std::string client_ip = aservice.second->Get<std::string>("ip");
			std::string client_port = aservice.second->Get<std::string>(sock->remote_port_name);
			
			std::string connection_string="tcp://"+client_ip + ":" + client_port;
			sock->socket->disconnect(connection_string.c_str());
			
			LOG(logger,LOG_NOTICE,"Disconnected %s %s port",aservice.second->Get<std::string>("msg_value").c_str(), sock->remote_port_name.c_str());
			
		}
		sock->connections.clear();
		
	}
	container_locker.unlock();
	new_clients = true;
	
	return "All clients disconnected";
}

std::string SocketManager::DisconnectClient(const char* client){
	
	LOG(logger,LOG_WARNING,"SocketManager kicking client %s!", client);
	
	std::shared_lock<std::shared_mutex> container_locker(m_data->managed_sockets_mtx);
	for(std::pair<const std::string&, ManagedSocket*> mgd_sock : m_data->managed_sockets){
		
		ManagedSocket* sock = mgd_sock.second;
		std::unique_lock<std::mutex> socket_locker(sock->socket_mtx, std::defer_lock);
		if(!socket_locker.try_lock()){
			sock->socket_manager_request=true;
			socket_locker.lock();
			sock->socket_manager_request=false;
		}
		std::unique_lock<std::mutex> connections_locker(sock->connections_mtx);
		
		for(std::pair<const std::string, Store*>& aservice : sock->connections){
			
			if(strcmp(aservice.second->Get<std::string>("msg_value").c_str(),client)!=0){
				continue;
			}
			
			std::string client_ip = aservice.second->Get<std::string>("ip");
			std::string client_port = aservice.second->Get<std::string>(sock->remote_port_name);
			
			std::string connection_string="tcp://"+client_ip + ":" + client_port;
			sock->socket->disconnect(connection_string.c_str());
			
			LOG(logger,LOG_WARNING,"Disconnected %s %s port",aservice.second->Get<std::string>("msg_value").c_str(), sock->remote_port_name.c_str());
			
		}
		sock->connections.clear();
		
	}
	container_locker.unlock();
	new_clients = true;
	
	return "All clients disconnected";
}
