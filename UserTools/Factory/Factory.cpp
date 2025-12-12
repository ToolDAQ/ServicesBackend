#include "Factory.h"

Tool* Factory(std::string tool) {
Tool* ret=0;

// if (tool=="Type") tool=new Type;
if (tool=="DummyTool") ret=new DummyTool;
if (tool=="MulticastReceiver") ret=new MulticastReceiver;
if (tool=="MulticastWorkers") ret=new MulticastWorkers;
if (tool=="DatabaseWorkers") ret=new DatabaseWorkers;
if (tool=="QueueTrimmer") ret=new QueueTrimmer;
if (tool=="WriteQueryReceiver") ret=new WriteQueryReceiver;
if (tool=="ReadQueryReceiverReplySender") ret=new ReadQueryReceiverReplySender;
if (tool=="WriteWorkers") ret=new WriteWorkers;
if (tool=="MiddlemanNegotiate") ret=new MiddlemanNegotiate;
if (tool=="Monitoring") ret=new Monitoring;
if (tool=="SocketManager") ret=new SocketManager;
if (tool=="ResultWorkers") ret=new ResultWorkers;
if (tool=="JobManager") ret=new JobManager;
return ret;
}
