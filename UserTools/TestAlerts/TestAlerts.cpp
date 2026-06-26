#include "TestAlerts.h"

TestAlerts::TestAlerts():Tool(){}

std::ofstream TestAlerts::out_file = std::ofstream{};

int TestAlerts::verb=1;

bool TestAlerts::Initialise(std::string configfile, DataModel &data){
	
	m_configfile=configfile;
	InitialiseTool(data);
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
	verb=m_verbose; // static used by thread
	
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
	while(alert_names >> next_alert){
		Log("Subscribed to alert '"+next_alert+"'",v_message,m_verbose);
		m_data->services->AlertSubscribe(next_alert, &AlertReceive);
	}
	                      
	ExportConfiguration();
	
	return true;
	
}

bool TestAlerts::AlertReceive(const char* alert_name, const char* alert_payload){
	
	boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
	out_file << now << " " << /*m_data->coarse_counts << " " << */ alert_name << " " << alert_payload << std::endl;
	/*if(verb>3)*/ std::clog<<"TestAlerts received '"<<alert_name<<"' at "<<boost::posix_time::to_simple_string(now) 
	                  /*<<", coarse counter "<<m_data->coarse_counter*/ << ", with payload '"<<alert_payload<<"'"<<std::endl;
	
	return true;
}

