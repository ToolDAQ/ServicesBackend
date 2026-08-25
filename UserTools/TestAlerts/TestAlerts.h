#ifndef TestAlerts_H
#define TestAlerts_H

#include <string>
#include <iostream>
#include <fstream>

#include "Tool.h"
#include "DataModel.h"

/**
* \class TestAlerts
*
* This is a simple tool to test the application is receiving alerts by logging alerts received to a file.
* Note these must be dummy alerts, since we cannot yet register multiple callbacks to alerts,
*
* $Author: M.O'Flaherty $
* $Date: 2026/07/06 $
*/

class TestAlerts: public Tool {


 public:

  TestAlerts(); ///< Simple constructor
  bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resources. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
  bool Execute(); ///< Execute function used to perform Tool purpose.
  bool Finalise(); ///< Finalise function used to clean up resources.
  bool LoadConfig();

 private:

 std::string m_configfile;
 bool AlertReceive(const char* alert_name, const char* alert_payload);
 std::ofstream out_file;
 std::chrono::time_point<std::chrono::steady_clock> last_run_start;

};


#endif
