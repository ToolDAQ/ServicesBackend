#ifndef MYTOOL_H
#define MYTOOL_H

#include <string>
#include <iostream>

#include "Tool.h"
#include "DataModel.h"

/**
* \class MyTool
*
* This is a blank template for a Tool used by the newTool.sh script to generate a new user tool. Please fill out the description and author information.
*
* $Author:  $
* $Date:  $
*/

class MyTool: public Tool {


  public:

  MyTool(); ///< Simple constructor
  bool Initialise(std::string configfile,DataModel &data); ///< Initialise function for setting up Tool resources. @param configfile The path and name of the dynamic configuration file to read in. @param data A reference to the transient data class used to pass information between Tools.
  bool Execute(); ///< Execute function used to perform Tool purpose.
  bool Finalise(); ///< Finalise function used to clean up resources.


  private:





};


#endif
