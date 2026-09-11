#include "SetVerbosity.h"

SetVerbosity::SetVerbosity():Tool(){}

bool SetVerbosity::Initialise(std::string configfile, DataModel &data){
  
  InitialiseTool(data);
  m_configfile = configfile;
  InitialiseConfiguration(configfile);
  
  // yep, a whole Tool just for this, because we can't do it elsewhere
  data.sc_vars.Add("SetVerbosity", SlowControlElementType(VARIABLE),
                   [&data](const char* newval) -> std::string { data.m_logger.SetVerbosity(atoi(newval)); return newval; },
                   [&data](const char*) -> std::string { return std::to_string(data.m_logger.GetVerbosity()); },
                   false,false); // not hidden, not locked
  data.sc_vars["SetVerbosity"]->SetMin(0);
  data.sc_vars["SetVerbosity"]->SetMax(LOG_DEBUG);
  data.sc_vars["SetVerbosity"]->SetStep(1);
  data.sc_vars["SetVerbosity"]->SetValue(m_variables.Get<int>("verbosity"));
  
  return true;
}

bool SetVerbosity::Execute(){
  return true;
}

bool SetVerbosity::Finalise(){
  return true;
}
