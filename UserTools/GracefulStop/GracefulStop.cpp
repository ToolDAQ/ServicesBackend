#include "GracefulStop.h"
#include <signal.h>
#include <fstream>

GracefulStop::GracefulStop():Tool(){}

bool GracefulStop::gotStopSignal = false;

bool GracefulStop::Initialise(std::string configfile, DataModel &data){
  
  InitialiseTool(data);
  m_configfile = configfile;
  InitialiseConfiguration(configfile);
  logger=m_data->logger;
  //m_variables.Print();
  // nothing to load or export
  //LoadConfig();
  //ExportConfiguration();
  
  if(signal((int) SIGUSR1, GracefulStop::stopSignalHandler) == SIG_ERR){
    LOG(logger,LOG_ERR,"Failed to setup SIGUSR1 handler!");
    return false;
  }
  
  m_data->sc_vars.Add("StopAndQuit", SlowControlElementType::BUTTON,
                      std::bind(&GracefulStop::StopAndQuit, this, std::placeholders::_1), nullptr);
  
  return true;
}

void GracefulStop::stopSignalHandler(int _ignored){
  // technically we could choose what to do based on the signal type passed, if we registered this function with multliple signals.
  gotStopSignal = true;
}

bool GracefulStop::Execute(){
  if(gotStopSignal){
    LOG(logger,LOG_NOTICE,"Received SIGUSR1, terminating ToolChain");
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

std::string GracefulStop::StopAndQuit(const char*){
  std::ofstream test("./quit");
  gotStopSignal=true;
  return "Stopping and preventing automatic restart";
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
    LOG(logger,LOG_ERR,"SendCommand failed to send '%s' with %s",command.c_str(),zmq_strerror(errno));
    return false;
  }
  usleep(1000);
  /*
  // RemoteControl commands get received and handled in the main thread - i.e. the same thread
  // this function is running on. That means we're not gonna get a response until the next toolchain
  // execution (which will never happen, as we've asked for a Stop). So no point waiting for the reply.
  // N.B. for StopAndQuit, if we did the send/receive there it would work, since SlowControlCollection
  // runs callbacks on a different thread.
  zmq::message_t rep;
  ok = sock.recv(&rep);
  if(!ok){
    LOG(logger,LOG_ERR,"SendCommand failed to receive reply to '%s' with %s",command.c_str(),zmq_strerror(errno));
    return false;
  }
  */
  return true;
}
