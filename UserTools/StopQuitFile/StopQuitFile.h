#ifndef StopQuitFile_H
#define StopQuitFile_H

#include <string>
#include <iostream>
#include <fstream>

#include "Tool.h"
#include "DataModel.h"

/**
* \class StopQuitFile
*
* Tool to monitor for the presence of a stop and quit file, and terminate the toolchain if found.
*
* $Author: M. O'Flaherty $
* $Date: 2026/07/06 $
*/

class StopQuitFile: public Tool {
  
  public:
  
  StopQuitFile(); ///< Simple constructor
  bool Initialise(std::string configfile,DataModel &data); ///< Initialise Function for setting up Tool resources. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
  bool Execute(); ///< Execute function used to perform Tool purpose.
  bool Finalise(); ///< Finalise function used to clean up resources.
  bool LoadConfig(); ///< Initialise variables from configuration store.
  
  
  private:
  std::string stop_file;
  std::string quit_file;
  std::ifstream test;
  
};


#endif
