#include "SocketManager.h"

SocketManager::SocketManager():Tool(){}


bool SocketManager::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
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
	thread_mtx.lock();
	thread_args.thread_mtx = &thread_mtx;
	
	if(!m_data->utils.CreateThread("socket_manager", &Thread, &thread_args)){
		Log("Failed to spawn background thread",v_error,m_verbose);
		return false;
	}
	m_data->num_threads++;
	
	m_data->sc_vars.Add("Clients", SlowControlElementType::INFO, nullptr, nullptr); // INFO type doesnt need read fnct
	m_data->sc_vars.Add("ClearClients", SlowControlElementType::BUTTON,
	                    std::bind(&SocketManager::ClearClients, this, std::placeholders::_1), nullptr);
	
	return true;
}


bool SocketManager::Execute(){
	
	if(!thread_args.running){
		Log("Execute found thread not running!",v_error);
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++(monitoring_vars.thread_crashes);
	}
	
	return true;
}


bool SocketManager::Finalise(){
	
	// signal job distributor thread to stop
	Log("Joining socket manager thread",v_warning);
	thread_args.running=false;
	thread_mtx.unlock();
	m_data->utils.KillThread(&thread_args);
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	Log("Finished",v_warning);
	return true;
}

void SocketManager::Thread(Thread_args* args){
	
	SocketManager_args* m_args = dynamic_cast<SocketManager_args*>(args);
	
	//printf("SocketManager checking for new clients after %lu ms\n",std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-m_args->last_update).count());
	m_args->last_update = std::chrono::steady_clock::now();
	
	bool new_clients=false;
	
	std::unique_lock<std::mutex> container_locker(m_args->m_data->managed_sockets_mtx);
	for(std::pair<const std::string&, ManagedSocket*> mgd_sock : m_args->m_data->managed_sockets){
		
		ManagedSocket* sock = mgd_sock.second;
		
		std::unique_lock<std::mutex> locker(sock->socket_mtx, std::defer_lock);
		if(!locker.try_lock()){
			sock->socket_manager_request=true;
			locker.lock();
			sock->socket_manager_request=false;
		}
		
		int new_conn_count = std::abs((long long int)sock->connections.size() - m_args->daq_utils->UpdateConnections(sock->service_name, sock->socket, sock->connections, "", sock->remote_port_name));
		locker.unlock();
		
		if(new_conn_count!=0){
			//m_args->m_data->services->SendLog(std::to_string(std::abs(new_conn_count))+" new connections to "+sock->service_name, v_message); // FIXME logging
			//printf("mm %d new %s connections made!\n",new_conn_count, sock->remote_port_name.c_str());
			new_clients = true;
			
			// update the list of clients so they can be queried
			for(std::pair<const std::string, Store*>& aservice : sock->connections){
				
				std::string client_name = aservice.second->Get<std::string>("msg_value");
				std::string client_ip = aservice.second->Get<std::string>("ip");
				std::string client_port = aservice.second->Get<std::string>(sock->remote_port_name);
				std::string client_uuid = aservice.second->Get<std::string>("uuid");
				//printf("%s connection to client application '%s' with uuid '%s' at ip '%s' on port '%s'\n",
				//       sock->remote_port_name.c_str(), client_name.c_str(), client_uuid.c_str(),
				//       client_ip.c_str(), client_port.c_str());
				
				// we want to group by application
				// a given application will have a single client_name, IP and UUID, so bundle these
				std::string client_key = client_name+"["+client_uuid+"]@"+client_ip;
				
				// an application may have multiple connection types on different ports
				std::string client_conn = sock->remote_port_name+" ("+client_port+")";

				if(!m_args->clientsmap.count(client_key)){
					//printf("mm adding new %s client: '%s' with connection '%s'\n",sock->remote_port_name.c_str(),client_key.c_str(), client_conn.c_str());
					m_args->clientsmap.emplace(client_key, client_conn);
				} else {
					//printf("mm updating %s client '%s', adding connection '%s'\n",sock->remote_port_name.c_str(),client_key.c_str(),client_conn.c_str());
					m_args->clientsmap.at(client_key)+= ", "+client_conn;
				}
			}
			
		}
		
	}
	
	if(new_clients){
		
		std::string clientlist;
		for(std::pair<const std::string,std::string>& aclient : m_args->clientsmap){
			if(!clientlist.empty()) clientlist+="\r\n";
			clientlist += aclient.first+": "+aclient.second;
		}
		if(clientlist.size()>0){
			// if client list is non-empty set as slow control indicator
			m_args->m_data->sc_vars["Clients"]->SetValue(clientlist);
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
	std::unique_lock<std::mutex> container_locker(m_data->managed_sockets_mtx);
	for(std::pair<const std::string&, ManagedSocket*> mgd_sock : m_data->managed_sockets){
		ManagedSocket* sock = mgd_sock.second;
		std::unique_lock<std::mutex> locker(sock->socket_mtx, std::defer_lock);
		if(!locker.try_lock()){
			sock->socket_manager_request=true;
			locker.lock();
			sock->socket_manager_request=false;
		}
		sock->connections.clear();
	}
	thread_args.clientsmap.clear();
	return "clients cleared";
}
