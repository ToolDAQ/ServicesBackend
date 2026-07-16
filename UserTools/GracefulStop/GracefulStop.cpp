#include "GracefulStop.h"
#include <signal.h>

GracefulStop::GracefulStop():Tool(){}

bool GracefulStop::gotStopSignal = false;

bool GracefulStop::Initialise(std::string configfile, DataModel &data){
  
  InitialiseTool(data);
  m_configfile = configfile;
  InitialiseConfiguration(configfile);
  //m_variables.Print();
  // nothing to load or export
  //LoadConfig();
  //ExportConfiguration();
  
  if(signal((int) SIGUSR1, GracefulStop::stopSignalHandler) == SIG_ERR){
    Log("Failed to setup SIGUSR1 handler!", v_error, m_verbose);
    return false;
  }

  return true;
}

void GracefulStop::stopSignalHandler(int _ignored){
	// technically we could choose what to do based on the signal type passed, if we registered this function with multliple signals.
	gotStopSignal = true;
}

bool GracefulStop::Execute(){
  if(gotStopSignal){
    Log("Received SIGUSR1, terminating ToolChain",v_error,m_verbose);
    m_data->vars.Set("StopLoop",1); // this doesn't work in remote mode
    SendCommand("Stop");
    SendCommand("Quit");
    gotStopSignal=false;
  }
  return true;
}


bool GracefulStop::Finalise(){

  return true;
}

bool GracefulStop::SendCommand(std::string command){
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
    Log("SendCommand failed to send '"+command+" with "+zmq_strerror(errno),v_warning,m_verbose);
    return false;
  }
  usleep(1000);
  zmq::message_t rep;
  ok = sock.recv(&rep);
  if(!ok){
    Log("SendCommand failed to receive reply to "+command+" with "+zmq_strerror(errno),v_warning,m_verbose);
    return false;
  }
  return true;
}
