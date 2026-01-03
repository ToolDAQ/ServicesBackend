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
	
	if(!m_data->utils.CreateThread("socket_manager", &Thread, &thread_args)){
		Log(m_tool_name+": Failed to spawn background thread",v_error,m_verbose);
		return false;
	}
	m_data->num_threads++;
	
	m_data->sc_vars.Add("Clients", SlowControlElementType::INFO, nullptr, nullptr); // INFO type doesnt need read fnct
	
	return true;
}


bool SocketManager::Execute(){
	
	if(!thread_args.running){
		Log(m_tool_name+" Execute found thread not running!",v_error);
		Finalise();
		Initialise(m_configfile, *m_data); // FIXME should we give up if Initialise returns false? should we set StopLoop to 1?
		++(monitoring_vars.thread_crashes);
	}
	
	return true;
}


bool SocketManager::Finalise(){
	
	// signal job distributor thread to stop
	Log(m_tool_name+": Joining socket manager thread",v_warning);
	m_data->utils.KillThread(&thread_args);
	m_data->num_threads--;
	
	std::unique_lock<std::mutex> locker(m_data->monitoring_variables_mtx);
	m_data->monitoring_variables.erase(m_tool_name);
	
	Log(m_tool_name+": Finished",v_warning);
	return true;
}

void SocketManager::Thread(Thread_args* args){
	
	SocketManager_args* m_args = dynamic_cast<SocketManager_args*>(args);
	
	m_args->last_update = std::chrono::steady_clock::now();
	//printf("SocketManager checking for new clients\n");
	
	bool new_clients=false;
	
	std::unique_lock<std::mutex> container_locker(m_args->m_data->managed_sockets_mtx);
	for(std::pair<const std::string&, ManagedSocket*> mgd_sock : m_args->m_data->managed_sockets){
		
		ManagedSocket* sock = mgd_sock.second;
		
		std::unique_lock<std::mutex> locker(sock->socket_mtx);
		int new_conn_count = std::abs((long long int)sock->connections.size() - m_args->daq_utils->UpdateConnections(sock->service_name, sock->socket, sock->connections, "", sock->remote_port_name));
		locker.unlock();
		
		if(new_conn_count!=0){
			//m_args->m_data->services->SendLog(m_tool_name+": "+std::to_string(std::abs(new_conn_count))+" new connections to "+sock->service_name, v_message); // FIXME logging
			printf("%d new %s connections made!\n",new_conn_count, sock->remote_port_name.c_str());
			new_clients = true;
			
			// update the list of clients so they can be queried
			for(std::pair<const std::string, Store*>& aservice : sock->connections){
				if(!m_args->clientsmap.count(aservice.first)){
					m_args->clientsmap.emplace(aservice.first,sock->service_name);
				} else {
					m_args->clientsmap.at(aservice.first)+= ", "+sock->service_name;
				}
			}
			
		}
		
	}
	container_locker.unlock();
	
	if(new_clients){
		
		std::string clientlist;
		for(std::pair<const std::string,std::string>& aclient : m_args->clientsmap){
			if(!clientlist.empty()) clientlist+="\n";
			clientlist += aclient.first+": "+aclient.second;
		}
		if(clientlist.size()>0){
			// if client list is non-empty, remove trailing newline and set as slow control indicator
			clientlist.pop_back();
			m_args->m_data->sc_vars["Clients"]->SetValue(clientlist);
		}
		
	}
	
	std::this_thread::sleep_until(m_args->last_update+m_args->update_period_ms);
	
	return;
	
}


