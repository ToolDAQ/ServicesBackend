#include "TestAlerts.h"

TestAlerts::TestAlerts():Tool(){}

bool TestAlerts::Initialise(std::string configfile, DataModel &data){
	
	m_configfile=configfile;
	InitialiseTool(data);
	InitialiseConfiguration(m_configfile);
	LoadConfig();
	ExportConfiguration();
	
	last_run_start = std::chrono::steady_clock::now() - std::chrono::hours(1);
	
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
	/*
	// maybe it makes sense to only subscribe to alerts of interest
	// but for now we're using this tool as a hack for run_info, and that requires acting on
	// alerts we already subscribe to elsewhere (ChangeConfig).
	// While we cannot register multiple callbacks for one alert, we can register a single callback
	// for all alerts. So let's do that instead.
	while(alert_names >> next_alert){
		Log("Subscribed to alert '"+next_alert+"'",v_message,m_verbose);
		bool ok = m_data->services->AlertSubscribe(next_alert, std::bind(&TestAlerts::AlertReceive, this, std::placeholders::_1, std::placeholders::_2));
		out_file << now << "subscribing to alert '" << next_alert << "' returned " << ok << std::endl;
	}
	*/
	bool ok = m_data->services->AlertSubscribe("*", std::bind(&TestAlerts::AlertReceive, this, std::placeholders::_1, std::placeholders::_2));
	out_file << now << "subscribing to alert '*' returned " << ok << std::endl;
	
	ExportConfiguration();
	
	return true;
	
}

bool TestAlerts::AlertReceive(const char* alert_name, const char* alert_payload){
	
	// FIXME we could filter out those alerts that aren't in the alerts passed in config here
	
	boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
	out_file << now << " " << alert_name << " " << alert_payload << std::endl;
	if(m_verbose>3){
		std::cout << "TestAlerts received '" << alert_name << "' at "
		          << boost::posix_time::to_simple_string(now);
		if(alert_payload) std::cout << ", with payload '" << alert_payload << "'";
		std::cout << std::endl;
	}
	
	// the middleman would not normally handle all this, but in lieu of a broker, we do it here
	if(strcmp(alert_name,"ChangeConfig")==0 || strcmp(alert_name,"RunStop")==0){
		// once detector configuration changes, any current run must be flagged as ended
		// so if there is a run going without end time, set it to now.
		// Technically we don't check that the run is still going, so if the last run crashed
		// setting the run end time to now may be inaccurate....
		// well this whole thing is a quick hack anyway. We'll handle it better in the real HK DAQ.
		static const std::string query = "UPDATE run_info SET stop_time='now()' WHERE run_number=( select max(run_number) from run_info ) AND stop_time IS NULL";
		std::string response;
		int ok = m_data->services->SQLQuery(query, response);
	}
	
	if(strcmp(alert_name,"ChangeConfig")==0){
		// The Broker decides when a new run starts, and make a new run_info DB entry.
		// Since we have no broker yet, we do it here on ChangeConfig.
		// This will always be sent when run changes, even on a warm start.
		std::string base_config_id;
		std::string runmode_config_id;
		m_data->vars.Get("base_config_id",base_config_id);
		m_data->vars.Get("runmode_config_id",runmode_config_id);
		
		// disallow new runs within 5 seconds of each other.... currently working around an alert duplication issue
		// (being investigated), but we may want to do something similar in HK.
		if((std::chrono::steady_clock::now()-last_run_start) < std::chrono::seconds(5)){
			std::cerr<<"Additional RunStart alert received within 5 seconds of last call! Ignoring..."<<std::endl;
		} else {
			last_run_start = std::chrono::steady_clock::now();
			std::string query = "insert into run_info ( start_time, base_config_id, runmode_config_id, testing, comments ) "
				                "values ( 'now()', "+base_config_id+", "+runmode_config_id+", False, 'test run' )";
			
			std::string response;
			int ok = m_data->services->SQLQuery(query, response);
		}
	}
	
	return true;
}

