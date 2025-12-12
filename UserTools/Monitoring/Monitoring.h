#ifndef Monitoring_H
#define Monitoring_H

#include <string>
#include <iostream>
#include <sstream>
#include <chrono>

#include "Tool.h"


/**
* \class Monitoring
*
* This Tool sends out statistics to assist with performance monitoring and debugging
*
* $Author: Marcus O'Flaherty $
* $Date: 2025/12/11 $
* Contact: marcus.o-flaherty@warwick.ac.uk
*/

struct PubReceiver_args : public Thread_args {
	
	DataModel* m_data;
	std::chrono::time_point<std::chrono::steady_clock> last_send;
	std::chrono::milliseconds monitoring_period_ms;
	std::stringstream ss;
	
}

class Monitoring: public Tool {
	public:
	Monitoring(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Execute function used to perform Tool purpose.
	bool Finalise(); ///< Finalise function used to clean up resources.
	
	private:
	Thread_args thread_args;
	
};


#endif
