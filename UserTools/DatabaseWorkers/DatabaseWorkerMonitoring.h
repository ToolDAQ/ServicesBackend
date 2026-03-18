#ifndef DatabaseWorkerMonitoring_H
#define DatabaseWorkerMonitoring_H

#include "MonitoringVariables.h"

class DatabaseWorkerMonitoring : public MonitoringVariables {
	public:
	DatabaseWorkerMonitoring(){};
	~DatabaseWorkerMonitoring(){};
	
	std::atomic<int> logging_submissions;
	std::atomic<int> logging_submissions_failed;
	std::atomic<int> monitoring_submissions;
	std::atomic<int> monitoring_submissions_failed;
	std::atomic<int> rootplot_submissions;
	std::atomic<int> rootplot_submissions_failed;
	std::atomic<int> plotlyplot_submissions;
	std::atomic<int> plotlyplot_submissions_failed;
	std::atomic<int> alarm_submissions;
	std::atomic<int> alarm_submissions_failed;
	std::atomic<int> devconfig_submissions;
	std::atomic<int> devconfig_submissions_failed;
	std::atomic<int> base_config_submissions;
	std::atomic<int> base_config_submissions_failed;
	std::atomic<int> runmode_config_submissions;
	std::atomic<int> runmode_config_submissions_failed;
	std::atomic<int> calibration_submissions;
	std::atomic<int> calibration_submissions_failed;
	std::atomic<int> generic_submissions;
	std::atomic<int> generic_submissions_failed;
	std::atomic<int> readquery_submissions;
	std::atomic<int> readquery_submissions_failed;
	std::atomic<int> jobs_completed;
	std::atomic<int> jobs_failed;
	std::atomic<int> thread_crashes; // restarts of tool worker thread (main thread found reader thread 'running' was false)
	
	std::atomic<int> logging_bytes;
	std::atomic<int> monitoring_bytes;
	
	std::string toJSON(){
		
		std::string s="{\"logging_submissions\":"+std::to_string(logging_submissions.load())
		             +",\"logging_submissions_failed\":"+std::to_string(logging_submissions_failed.load())
		             +",\"logging_bytes\":"+std::to_string(logging_bytes.load())
		             +",\"monitoring_submissions\":"+std::to_string(monitoring_submissions.load())
		             +",\"monitoring_submissions_failed\":"+std::to_string(monitoring_submissions_failed.load())
		             +",\"monitoring_bytes\":"+std::to_string(monitoring_bytes.load())
		             +",\"rootplot_submissions\":"+std::to_string(rootplot_submissions.load())
		             +",\"rootplot_submissions_failed\":"+std::to_string(rootplot_submissions_failed.load())
		             +",\"plotlyplot_submissions\":"+std::to_string(plotlyplot_submissions.load())
		             +",\"plotlyplot_submissions_failed\":"+std::to_string(plotlyplot_submissions_failed.load())
		             +",\"alarm_submissions\":"+std::to_string(alarm_submissions.load())
		             +",\"alarm_submissions_failed\":"+std::to_string(alarm_submissions_failed.load())
		             +",\"devconfig_submissions\":"+std::to_string(devconfig_submissions.load())
		             +",\"devconfig_submissions_failed\":"+std::to_string(devconfig_submissions_failed.load())
		             +",\"base_config_submissions\":"+std::to_string(base_config_submissions.load())
		             +",\"base_config_submissions_failed\":"+std::to_string(base_config_submissions_failed.load())
		             +",\"runmode_config_submissions\":"+std::to_string(runmode_config_submissions.load())
		             +",\"runmode_config_submissions_failed\":"+std::to_string(runmode_config_submissions_failed.load())
		             +",\"calibration_submissions\":"+std::to_string(calibration_submissions.load())
		             +",\"calibration_submissions_failed\":"+std::to_string(calibration_submissions_failed.load())
		             +",\"generic_submissions\":"+std::to_string(generic_submissions.load())
		             +",\"generic_submissions_failed\":"+std::to_string(generic_submissions_failed.load())
		             +",\"readquery_submissions\":"+std::to_string(readquery_submissions.load())
		             +",\"readquery_submissions_failed\":"+std::to_string(readquery_submissions_failed.load())
		             +",\"jobs_failed\":"+std::to_string(jobs_failed.load())
		             +",\"jobs_completed\":"+std::to_string(jobs_completed.load())
		             +",\"thread_crashes\":"+std::to_string(thread_crashes.load())
		             +"}";
		
		return s;
	}
};

#endif
