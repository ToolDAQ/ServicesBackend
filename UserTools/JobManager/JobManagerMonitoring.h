#ifndef JobManagerMonitoring_H
#define JobManagerMonitoring_H

#include "MonitoringVariables.h"

class JobManagerMonitoring : public MonitoringVariables {
	public:
	JobManagerMonitoring(){};
	~JobManagerMonitoring(){};
	
	std::string toJSON(){
		
		std::string s="";
		
		return s;
	}
};

#endif
