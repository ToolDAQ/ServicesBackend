#ifndef GracefulStop_H
#define GracefulStop_H

#include <string>
#include <iostream>

#include "Tool.h"
#include "DataModel.h"

/**
* \class GracefulStop
*
* This is a blank template for a Tool used by the newTool.sh script to generate a new user tool. Please fill out the description and author information.
*
* $Author:  $
* $Date:  $
*/

class GracefulStop: public Tool {


  public:

  GracefulStop(); ///< Simple constructor
  bool Initialise(std::string configfile,DataModel &data); ///< Initialise function for setting up Tool resources. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
  bool Execute(); ///< Execute function used to perform Tool purpose.
  bool Finalise(); ///< Finalise function used to clean up resources.


  private:
  static void stopSignalHandler(int _ignored);  // we need a function to register as the signal handler
  static bool gotStopSignal;
  bool SendCommand(std::string cmd);





};


#endif
