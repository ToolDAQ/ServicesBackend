#include "StopQuitFile.h"

StopQuitFile::StopQuitFile():Tool(){}

bool StopQuitFile::Initialise(std::string configfile, DataModel &data){
  
  InitialiseTool(data);
  m_configfile = configfile;
  InitialiseConfiguration(configfile);
  logger=m_data->logger;
  //m_variables.Print();
  LoadConfig();
  
  ExportConfiguration();
  
  return true;
}


bool StopQuitFile::Execute(){
  
  test.open(stop_file.c_str());
  if(test.is_open()){
    test.close();
    if(!stopped){
      LOG(logger,LOG_NOTICE,"StopFile found, stopping toolchain");
      m_data->vars.Set("StopLoop",1); // this doesn't work in remote mode
      SendCommand("Stop");
      SendCommand("Quit");
      stopped=true;
    }
  } else {
    stopped=false;
  }
  
  test.open(quit_file.c_str());
  if(test.is_open()){
    test.close();
    if(!quitted){
      LOG(logger,LOG_NOTICE,"QuitFile found, stopping toolchain");
      m_data->vars.Set("StopLoop",1);
      SendCommand("Stop");
      SendCommand("Quit");
      quitted=true;
    }
  } else {
    quitted=false;
  }
  
  return true;
}

bool StopQuitFile::SendCommand(std::string command){
  int remote_port;
  m_data->vars.Get("remote_port",remote_port);
  zmq::socket_t sock(*m_data->context, ZMQ_REQ);
  sock.setsockopt(ZMQ_LINGER,0);
  sock.setsockopt(ZMQ_SNDTIMEO,200);
  sock.setsockopt(ZMQ_RCVTIMEO,200);
  std::string endpoint = "tcp://127.0.0.1:"+std::to_string(remote_port);
  sock.connect(endpoint);
  Store tmp;
  tmp.Set("msg_type","Command");
  tmp.Set("msg_value",command);
  tmp >> command;
  zmq::message_t msg(command.length());
  memcpy(msg.data(), command.data(), command.length());
  int ok = sock.send(msg);
  if(!ok){
    LOG(logger,LOG_ERR,"SendCommand failed to send '%s' with %s",command,zmq_strerror(errno));
    return false;
  }
  usleep(1000);
  zmq::message_t rep;
  ok = sock.recv(&rep);
  if(!ok){
    LOG(logger,LOG_ERR,"SendCommand failed to receive reply to '%s' with %s",command,zmq_strerror(errno));
    return false;
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
