#include "DummyTool.h"

DummyTool::DummyTool():Tool(){}


bool DummyTool::Initialise(std::string configfile, DataModel &data){

   if(configfile!="")  m_variables.Initialise(configfile);
   //m_variables.Print();

  m_data= &data;
  m_log= m_data->Log; 

  if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;  
 
  Log("test 1",1,m_verbose);

  return true;
}


bool DummyTool::Execute(){

  // Dummy test of various printout sytles and techniques

  Log("test 2",1,m_verbose);
  Log("test 3",1,5);
  *m_log<<red<<"test 4 "<<green<<"test 5 "<<std::endl<<"test 6 "<<std::endl;
  *m_log<<MsgL(2,6)<<yellow<<"test 7 "<<std::endl<<"test 8 "<<std::endl;
  *m_log<<ML(2)<<pink<<"test 9 "<<std::endl<<"test 10 "<<std::endl;
  *m_log<<blue<<"test 11 "<<std::endl<<"test 12 "<<std::endl;
  MLC();
 
 return true;
}


bool DummyTool::Finalise(){

  return true;
}
