#include "MyTool.h"

MyTool::MyTool():Tool(){}


bool MyTool::Initialise(std::string configfile, DataModel &data){
  
  InitialiseTool(data);
  m_configfile = configfile;
  InitialiseConfiguration(configfile);
  //m_variables.Print();

  //your code here


  ExportConfiguration();

  return true;
}


bool MyTool::Execute(){

  return true;
}


bool MyTool::Finalise(){

  return true;
}
