#include "TestAlerts.h"

TestAlerts::TestAlerts():Tool(){}

bool TestAlerts::Initialise(std::string configfile, DataModel &data){
	
	m_configfile=configfile;
	InitialiseTool(data);
	InitialiseConfiguration(m_configfile);
	LoadConfig();
	ExportConfiguration();
	
	return true;
	
}

bool TestAlerts::Execute(){
	
	if(m_data->change_config){
		InitialiseConfiguration(m_configfile);
		LoadConfig();
		ExportConfiguration();
	}
	
	return true;
}


bool TestAlerts::Finalise(){
	
	out_file.close();
	return true;
}


bool TestAlerts::LoadConfig(){
	
	m_variables.Get("verbose",m_verbose);
	
	std::string out_fname = "./alert_tester.log";
	if(!m_variables.Get("out_file",out_fname));
	out_file.open(out_fname);
	if(!out_file.is_open()){
		Log("Error opening output file '"+out_fname+"'",v_error,m_verbose);
		return false;
	}
	
	std::string next_alert;
	if(!m_variables.Get("alert_names",next_alert)){
		Log("Error: No alert_names given!",v_error,m_verbose);
		return false;
	}
	std::stringstream alert_names;
	alert_names.str(next_alert);
	boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
	while(alert_names >> next_alert){
		Log("Subscribed to alert '"+next_alert+"'",v_message,m_verbose);
		bool ok = m_data->services->AlertSubscribe(next_alert, std::bind(&TestAlerts::AlertReceive, this, std::placeholders::_1, std::placeholders::_2));
		out_file << now << "subscribing to alert '" << next_alert << "' returned " << ok << std::endl;
	}
	
	ExportConfiguration();
	
	return true;
	
}

bool TestAlerts::AlertReceive(const char* alert_name, const char* alert_payload){
	
	boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
	out_file << now << " " << alert_name << " " << alert_payload << std::endl;
	if(m_verbose>3) std::cout << "TestAlerts received '" << alert_name << "' at "
	                          << boost::posix_time::to_simple_string(now)
	                          << ", with payload '" << alert_payload << "'" << std::endl;
	
	return true;
}

