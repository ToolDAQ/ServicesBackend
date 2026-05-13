#include "Sleep.h"

Sleep::Sleep():Tool(){}


bool Sleep::Initialise(std::string configfile, DataModel &data){
	
	InitialiseTool(data);
	m_configfile = configfile;
	InitialiseConfiguration(configfile);
	//m_variables.Print();
	
	ExportConfiguration();
	
	unsigned int period_us = 10;
	m_variables.Get("period_us",period_us);
	toolchain_period_us = std::chrono::microseconds{period_us};
	
	last_execute = std::chrono::steady_clock::now();
	
	return true;
}


bool Sleep::Execute(){
	
	std::this_thread::sleep_until(last_execute+toolchain_period_us);
	last_execute = std::chrono::steady_clock::now();
	
	return true;
}


bool Sleep::Finalise(){
	
	return true;
}
