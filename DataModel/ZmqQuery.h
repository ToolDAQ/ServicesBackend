#ifndef ZMQ_QUERY_H
#define ZMQ_QUERY_H
#include <zmq.h>

struct ZmqQuery {
	
	ZmqQuery(){};
	~ZmqQuery(){};
	
	// no copy constructor
	ZmqQuery(const ZmqQuery&) = delete;
	// no copy assignment operator
	ZmqQuery& operator=(const ZmqQuery&) = delete;
	
	// allow move constructor
	ZmqQuery(ZmqQuery&& c) = default;
	// allow move assignment operator
	ZmqQuery& operator=(ZmqQuery&& c) = default;
	
	// 4 parts for receiving, for sending 3+ parts
	std::vector<zmq::message_t> parts(4);
	size_t size() const {
		return parts.size();
	}
	
	// pub socket: topic, client, msgnum, query
	// router socket: client, topic, msgnum, query
	// replies: client, msgnum, success, results (if present)...
	
	zmq::message_t& operator[](int i){
		return parts[i];
	}
	// received and returned
	std::string_view client_id(){
		return std::string_view{parts[0].data(),parts[0].data().size()};
	}
	uint32_t msg_id(){
		return *reinterpret_cast<uint32_t*>(parts[1].data());
	}
	// received only
	std::string_view topic(){
		return std::string_view{parts[2].data(),parts[2].data().size()};
	}
	std::string_view msg(){
		return std::string_view{parts[3].data(),parts[3].data().size()};
	}
	
	// for setting success
	void setsuccess(uint32_t succeeded){
		//parts[2]=new(parts[2]) zmq::message_t(sizeof(uint32_t)); // uhh, is there a better way to call zmq_msg_init_size?
		zmq_msg_init_size(&parts[2],sizeof(uint32_t)); // this is from underlying c api... FIXME?
		memcpy((void*)parts[2].data(),&succeeded,sizeof(uint32_t));
		return;
	}
	
	// for read queries, returned directly from pqxx, decoded later
	pqxx::result result;
	
	// for setting responses of read queries
	void setresponserows(size_t n_rows){
		parts.resize(3+n_rows);
		return;
	}
	void setresponse(size_t row_num, std::string_view row){
		zmq_msg_init_size(&parts[row_num+3],row.size()); // this is from underlying c api... FIXME?
		memcpy((void*)parts[row_num+3].data(),row.data(),row.size());
		return;
	}
}


#endif
