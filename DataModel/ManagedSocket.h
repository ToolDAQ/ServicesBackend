#ifndef ManagedSocket_H
#define ManagedSocket_H

#include <map>
#include <mutex>
#include <zmq.hpp>

struct ManagedSocket {
	std::mutex socket_mtx;
	zmq::socket_t* socket=nullptr;
	std::string service_name;
	std::string port;
	std::string port_name;
	std::map<std::string,ToolFramework::Store*> connections;
};

#endif
