#ifndef QUERY_BATCH_H
#define QUERY_BATCH_H

struct QueryBatch {
	// fill / read by receive/senders
	QueryBatch(size_t prealloc_size){
		queries.reserve(prealloc_size);
	}
	std::vector<ZmqQuery> queries;
	
	// prepare for batch insertion by workers
	std::string alarm_buffer;
	std::string devconfig_buffer;
	std::string runconfig_buffer;
	std::string calibration_buffer;
	std::string plotlyplot_buffer;
	std::string rooplot_buffer;
	void reset(){
		alarm_buffer = "[";
		devconfig_buffer = "[";
		runconfig_buffer = "[";
		calibration_buffer = "[";
		plotlyplot_buffer = "[";
		rooplot_buffer = "[";
		
		alarm_batch_status = false;
		
		// the presence of returned version numbers is indication that these batch insertions worked
		devconfig_version_nums.clear();
		runconfig_version_nums.clear();
		calibration_version_nums.clear();
		plotlyplot_version_nums.clear();
		rootplot_version_nums.clear();
		
	}
	
	// set by database workers for batch submissions
	bool alarm_batch_success;
	
	 // FIXME check type returned from pqxx
	std::vector<uint32_t> devconfig_version_nums;
	std::vector<uint32_t> runconfig_version_nums;
	std::vector<uint32_t> calibration_version_nums;
	std::vector<uint32_t> plotlyplot_version_nums;
	std::vector<uint32_t> rootplot_version_nums;
	
//	// convert to zmq message on return path by workers
//	void setsuccess(uint32_t succeeded){
//		for(ZmqQuery& q : queries) q.setsuccess(succeeded);
//	}
//	void setversionnums(){
//		for(size_t i=0; i<ZmqQuery.size(); ++i) queries[i].setversionnum(version_nums[i]);
//	}
	// loop over all queries, move over status and version num using a tracking set of indices for each type
	
};

#endif
