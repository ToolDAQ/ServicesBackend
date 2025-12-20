#ifndef QUERY_BATCH_H
#define QUERY_BATCH_H

#include <vector>

#include "ZmqQuery.h"

struct QueryBatch {
	
	// fill / read by receive/senders
	std::vector<ZmqQuery> queries;
	
	// prepare for batch insertion by workers
	std::string alarm_buffer;
	std::string devconfig_buffer;
	std::string runconfig_buffer;
	std::string calibration_buffer;
	std::string plotlyplot_buffer;
	std::string rooplot_buffer;
	
	// flagged for can't be batch inserted by workers
	std::vector<size_t> generic_write_query_indices;
	
	// set by database workers after batch insert
	bool alarm_batch_success;
	std::vector<uint32_t> devconfig_version_nums;
	std::vector<uint32_t> runconfig_version_nums;
	std::vector<uint32_t> calibration_version_nums;
	std::vector<uint32_t> plotlyplot_version_nums;
	std::vector<uint32_t> rootplot_version_nums;
	
	QueryBatch(size_t prealloc_size){
		queries.reserve(prealloc_size);
	}
	
	void reset(){
		alarm_buffer = "[";
		devconfig_buffer = "[";
		runconfig_buffer = "[";
		calibration_buffer = "[";
		plotlyplot_buffer = "[";
		rooplot_buffer = "[";
		
		alarm_batch_success = false;
		
		// the presence of returned version numbers is indication that these batch insertions worked
		devconfig_version_nums.clear();
		runconfig_version_nums.clear();
		calibration_version_nums.clear();
		plotlyplot_version_nums.clear();
		rootplot_version_nums.clear();
		generic_write_query_indices.clear();
		
	}
	
};

#endif
