#ifndef MonitoringMonitoring_H
#define MonitoringMonitoring_H

#include "MonitoringVariables.h"

class MonitoringMonitoring : public MonitoringVariables {
	public:
	MonitoringMonitoring(){};
	~MonitoringMonitoring(){};
	
	// TODO add more monitoring
	std::atomic<int> thread_crashes; // restarts of tool worker thread (main thread found reader thread 'running' was false)
	
	std::string toJSON(){
		
		std::string s="{\"thread_crashes\":"+std::to_string(thread_crashes.load())
		             +"}";
		
		return s;
	}
};

#endif
