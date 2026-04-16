#ifndef ManagedSocket_H
#define ManagedSocket_H

#include <map>
#include <mutex>
#include <zmq.hpp>

struct ManagedSocket {
	std::mutex socket_mtx;
	bool socket_manager_request=false;
	zmq::socket_t* socket=nullptr;
	std::string service_name;
/*	std::string remote_port;*/
	std::string remote_port_name;
	std::map<std::string,ToolFramework::Store*> connections;
	~ManagedSocket(){
		for(auto&& aconn : connections){
			delete aconn.second;
		}
	}
};

#endif
