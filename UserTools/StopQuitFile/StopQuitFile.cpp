#include "StopQuitFile.h"

StopQuitFile::StopQuitFile():Tool(){}


bool StopQuitFile::Initialise(std::string configfile, DataModel &data){
  
  InitialiseTool(data);
  m_configfile = configfile;
  InitialiseConfiguration(configfile);
  //m_variables.Print();
  LoadConfig();
  
  ExportConfiguration();
  
  return true;
}


bool StopQuitFile::Execute(){
  
  test.open(stop_file.c_str());
  if(test.is_open()){
    Log("StopFile found, stopping toolchain",v_warning,m_verbose);
    test.close();
    m_data->vars.Set("StopLoop",1);
  }
 
  test.open(quit_file.c_str());
  if(test.is_open()){
    Log("QuitFile found, stopping toolchain",v_warning,m_verbose);
    test.close();
    m_data->vars.Set("StopLoop",1);
  }
  
  return true;
}


bool StopQuitFile::Finalise(){
  
  return true;
}

bool StopQuitFile::LoadConfig(){
  
  if(!m_variables.Get("stopfile",stop_file)) stop_file="./stop";
  if(!m_variables.Get("quitfile",quit_file)) quit_file="./quit";
  
  return true;
}
