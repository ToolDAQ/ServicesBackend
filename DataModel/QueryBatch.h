#ifndef QUERY_BATCH_H
#define QUERY_BATCH_H

#include <vector>

#include "ZmqQuery.h"

struct QueryBatch {
	
	QueryBatch(size_t prealloc_size){
		queries.reserve(prealloc_size);
	}
	
	// fill / read by receive/senders
	std::vector<ZmqQuery> queries;
	
	// prepare for batch insertion by workers
	std::string alarm_buffer;
	std::string devconfig_buffer;
	std::string runconfig_buffer;
	std::string calibration_buffer;
	std::string plotlyplot_buffer;
	std::string rootplot_buffer;
	
	// flagged for can't be batch inserted by workers
	std::vector<size_t> generic_query_indices;
	
	// set by database workers after batch insert
	std::vector<uint16_t> devconfig_version_nums;
	std::vector<uint16_t> runconfig_version_nums;
	std::vector<uint16_t> calibration_version_nums;
	std::vector<uint16_t> plotlyplot_version_nums;
	std::vector<uint16_t> rootplot_version_nums;
	
	std::string alarm_batch_err;
	std::string devconfig_batch_err;
	std::string runconfig_batch_err;
	std::string calibration_batch_err;
	std::string plotlyplot_batch_err;
	std::string rootplot_batch_err;
	
	void reset(){
		alarm_buffer = "[";
		devconfig_buffer = "[";
		runconfig_buffer = "[";
		calibration_buffer = "[";
		plotlyplot_buffer = "[";
		rootplot_buffer = "[";
		
		devconfig_version_nums.clear();
		runconfig_version_nums.clear();
		calibration_version_nums.clear();
		plotlyplot_version_nums.clear();
		rootplot_version_nums.clear();
		generic_query_indices.clear();
		
		alarm_batch_err.clear();
		devconfig_batch_err.clear();
		runconfig_batch_err.clear();
		calibration_batch_err.clear();
		plotlyplot_batch_err.clear();
		rootplot_batch_err.clear();
	}
	
	void close(){
		if(alarm_buffer.length()!=1) alarm_buffer += "]";
		else alarm_buffer.clear();
		
		if(devconfig_buffer.length()!=1) devconfig_buffer += "]";
		else devconfig_buffer.clear();
		
		if(runconfig_buffer.length()!=1) runconfig_buffer += "]";
		else runconfig_buffer.clear();
		
		if(calibration_buffer.length()!=1) calibration_buffer += "]";
		else calibration_buffer.clear();
		
		if(plotlyplot_buffer.length()!=1) plotlyplot_buffer += "]";
		else plotlyplot_buffer.clear();
		
		if(rootplot_buffer.length()!=1) rootplot_buffer += "]";
		else rootplot_buffer.clear();
	}
	
	bool got_alarms() const { return !alarm_buffer.empty(); }
	bool got_devconfigs() const { return !devconfig_buffer.empty(); }
	bool got_runconfigs() const { return !runconfig_buffer.empty(); }
	bool got_calibrations() const { return !calibration_buffer.empty(); }
	bool got_plotlyplots() const { return !plotlyplot_buffer.empty(); }
	bool got_rootplots() const { return !rootplot_buffer.empty(); }
	bool got_generics() const { return !generic_query_indices.empty(); }
	
};

#endif
