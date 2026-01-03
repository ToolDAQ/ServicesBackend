#ifndef ZMQ_QUERY_H
#define ZMQ_QUERY_H

#include <zmq.hpp>
#include <pqxx/pqxx>

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
	std::vector<zmq::message_t> parts{4};
	size_t size() const {
		return parts.size();
	}
	
	// pub socket: topic, client, msgnum, query
	// router socket: client, topic, msgnum, query
	// replies: client, msgnum, success, results (if present)...
	// if success is false, results are 1-part with an error message
	
	zmq::message_t& operator[](int i){
		return parts[i];
	}
	// received and returned
	std::string_view client_id(){
		return std::string_view{(const char*)parts[0].data(),parts[0].size()};
	}
	uint32_t msg_id(){
		return *reinterpret_cast<uint32_t*>(parts[1].data());
	}
	// received only
	std::string_view topic(){
		return std::string_view{(const char*)parts[2].data(),parts[2].size()};
	}
	std::string_view msg(){
		return std::string_view{(const char*)parts[3].data(),parts[3].size()};
	}
	
	// for setting success
	void setsuccess(uint32_t succeeded){
		//zmq_msg_init_size(&parts[2],sizeof(uint32_t)); // this is from underlying c api... mismatch zmq_msg_t* / zmq::message_t
		new(&parts[2]) zmq::message_t(sizeof(uint32_t)); // FIXME is there a better way to call zmq_msg_init_size?
		memcpy((void*)parts[2].data(),&succeeded,sizeof(uint32_t)); // FIXME make bool instead of uint32_t?
		return;
	}
	
	// for read queries, returned directly from pqxx, decoded later
	pqxx::result result;
	std::string err;
	
	void Clear(){
		result.clear();
		err.clear();
	}
	
	// for setting responses of read queries
	void setresponserows(size_t n_rows){
		printf("ZmqQuery at %p set to %lu response rows\n",this, n_rows);
		parts.resize(3+n_rows);
		return;
	}
	
	void setresponse(size_t row_num, std::string_view val){
		//zmq_msg_init_size(&parts[row_num+3],row.size()); // mismatch zmq_msg_t* / zmq::message_t
		printf("response part %lu set to %s on ZmqQuery at %p\n", row_num, val.data(), this);
		new(&parts[row_num+3]) zmq::message_t(val.size()); // FIXME better way to call zmq_msg_init_size
		memcpy((void*)parts[row_num+3].data(),val.data(),val.size());
		return;
	}
	
	template<typename T>
	typename std::enable_if<std::is_fundamental<T>::value, void>::type
	setresponse(size_t row_num, T val){
		//zmq_msg_init_size(&parts[row_num+3],row.size()); // mismatch zmq_msg_t* / zmq::message_t
		
		// what a mess. but only printf bypasses our great overlord's wonderful logging decorations
		std::ostringstream oss;
		oss << val;
		printf("response part %lu set to %s on ZmqQuery at %p\n", row_num, oss.str().c_str(), this);
		
		new(&parts[row_num+3]) zmq::message_t(sizeof(val)); // FIXME better way to call zmq_msg_init_size
		memcpy((void*)parts[row_num+3].data(),&val,sizeof(val));
		return;
	}
	
};


#endif
