#ifndef MonitoringVariables_H
#define MonitoringVariables_H

class MonitoringVariables {
	public:
	MonitoringVariables(){};
	virtual ~MonitoringVariables(){};
	virtual std::string toJSON()=0;
};

#endif
