*****************
# Middleman
*****************
The Middleman is an application for managing efficient insertions and retreival from a relational database (currently postgresql) used as part of the [ToolDAQFramework](https://github.com/ToolDAQ/ToolDAQFramework) applications.
The SetupDatabase.sh script defines the schema used, with tables for storing device and detector configurations, logging and monitoring messages, alarms, detector information, and various other information required by a data taking system.

*****************
# What it does
*****************
The middleman defines a ToolChain with separate tools for:
* receiving logging and monitoring data over multicast
* receiving database queries over ZMQ sockets
* combining and batching queries to improve insertion performance
* parsing configurations and relaying them to devices
* monitor and log system status

All Tools make extensive use of parallelisation utilities provided within ToolDAQFramework to support high loads.

*****************
# Who needs it
*****************
Applications developed using ToolDAQFramework that wish to use an external RDB for the above purposes may use the middleman in combination with features within the Services class, however it is not a required part of ToolDAQFramework, nor the only way to achieve this.
Many functions in the [libDAQInterface](https://github.com/ToolDAQ/libDAQInterface/) library for communicating with ToolDAQFramework applications actually communicate with an associated instance of the Middleman, so developers using this library are likely to need an instance.

*****************
# How do I run it?
*****************
The application can be run in a [Docker container](https://hub.docker.com/r/tooldaq/mm_db) as follows:
```
docker run --name=Middleman --net=host -dt tooldaq/mm_db
```
It can then be controlled via the ToolDAQ RemoteControl application, or the [ToolDAQ WebServer](github.com/ToolDAQ/webserver).

*****************
# For those from Hyper-Kamiokande
*****************
The middleman is one of the services run by the Docker compose system provided by the Hyper-Kamiokande fork of the ToolDAQ WebServer, and it is strongly recommended to run it this way. A complete guide on how to do this is provided on the [FD5 TWiki page](https://wiki.hyperk.org/do/view/HyperK/WorkingGroupFD5) under the section 'DAQ/Electronics Interface Testing Containers'. 


