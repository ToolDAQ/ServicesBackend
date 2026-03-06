#include "MyToolDynamicMultiThread.h"

MyToolDynamicMultiThread_args::MyToolDynamicMultiThread_args():Thread_args(){}

MyToolDynamicMultiThread_args::~MyToolDynamicMultiThread_args(){}


MyToolDynamicMultiThread::MyToolDynamicMultiThread():Tool(){}


bool MyToolDynamicMultiThread::Initialise(std::string configfile, DataModel &data){

  InitialiseTool(data);
  m_configfile = configfile;
  InitialiseConfiguration(configfile);
  //m_variables.Print();
  
  if(!m_variables.Get("verbose",m_verbose)) m_verbose=1;

  m_util=new Utilities();

  m_threadnum=0;
  CreateThread();
  
  m_freethreads=1;
  
  ExportConfiguration();    
  
  return true;
}


bool MyToolDynamicMultiThread::Execute(){

  for(unsigned int i=0; i<args.size(); i++){
    if(args.at(i)->busy==0){
      *m_log<<"reply="<<args.at(i)->message<<std::endl;
      args.at(i)->message="Hi";
      args.at(i)->busy=1;
      break;
    }

  }

  m_freethreads=0;
  unsigned int lastfree=0;
  for(unsigned int i=0; i<args.size(); i++){
    if(args.at(i)->busy==0){
      m_freethreads++;
      lastfree=i; 
    }
  }

  if(m_freethreads<1) CreateThread();
  if(m_freethreads>1) DeleteThread(lastfree);
  
  *m_log<<ML(1)<<"free threads="<<m_freethreads<<":"<<args.size()<<std::endl;
  MLC();
  
  // sleep(1);  for single tool testing
  
  return true;
}


bool MyToolDynamicMultiThread::Finalise(){

  for(unsigned int i=0;i<args.size();i++) DeleteThread(0);
  
  args.clear();
  
  delete m_util;
  m_util=0;

  return true;
}

void MyToolDynamicMultiThread::CreateThread(){

  MyToolDynamicMultiThread_args* tmparg=new MyToolDynamicMultiThread_args();
  tmparg->busy=0;
  tmparg->message="";
  args.push_back(tmparg);
  std::stringstream tmp;
  tmp<<"T"<<m_threadnum;
  m_util->CreateThread(tmp.str(), &Thread, args.at(args.size()-1));
  m_threadnum++;

}

 void MyToolDynamicMultiThread::DeleteThread(unsigned int pos){

   m_util->KillThread(args.at(pos));
   delete args.at(pos);
   args.at(pos)=0;
   args.erase(args.begin()+(pos));

 }

void MyToolDynamicMultiThread::Thread(Thread_args* arg){

  MyToolDynamicMultiThread_args* args=reinterpret_cast<MyToolDynamicMultiThread_args*>(arg);

  if(!args->busy) usleep(100);
  else{ 

    args->message="Hello";
    sleep(10);

    args->busy=0;
  }

}
