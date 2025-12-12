#include "MyToolThread.h"

MyToolThread_args::MyToolThread_args():Thread_args(){}

MyToolThread_args::~MyToolThread_args(){}


MyToolThread::MyToolThread():Tool(){}


bool MyToolThread::Initialise(std::string configfile, DataModel &data){

  InitialiseTool(data);
  m_configfile = configfile;
  InitialiseConfiguration(configfile);
  //m_variables.Print();

  if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;

  m_util=new Utilities();
  args=new MyToolThread_args();
  
  m_util->CreateThread("test", &Thread, args);

  ExportConfiguration();
  
  return true;
}


bool MyToolThread::Execute(){

  return true;
}


bool MyToolThread::Finalise(){

  m_util->KillThread(args);

  delete args;
  args=0;

  delete m_util;
  m_util=0;

  return true;
}

void MyToolThread::Thread(Thread_args* arg){

  MyToolThread_args* args=reinterpret_cast<MyToolThread_args*>(arg);

}
