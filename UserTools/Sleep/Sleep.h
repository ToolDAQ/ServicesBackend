#ifndef Sleep_H
#define Sleep_H

#include <string>
#include <iostream>

#include "Tool.h"
#include "DataModel.h"

/**
* \class Sleep
*
* This Tool simply sleeps to throttle the rate of the main ToolChain Execute loop, to prevent the main thread pegging a CPU core. It may be useful for highly threaded toolchains which do minimal work in Execute functions.
*
* $Author: Marcus O'Flaherty $
* $Date: 2026/09/01 $
*/

class Sleep: public Tool {
	
	public:
	Sleep(); ///< Simple constructor
	bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resorces. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
	bool Execute(); ///< Execute function used to perform Tool purpose
	bool Finalise(); ///< Finalise function used to clean up resources.
	
	private:
	std::chrono::time_point<std::chrono::steady_clock> last_execute;
	std::chrono::milliseconds toolchain_period_ms;
	
};


#endif
