ToolDAQPath=ToolDAQ

CXXFLAGS=  -fPIC -Wpedantic -O3 # -g -lSegFault -rdynamic -DDEBUG
# -Wl,--no-as-needed

ifeq ($(MAKECMDGOALS),debug)
CXXFLAGS+= -O1 -g -lSegFault -rdynamic -DDEBUG
endif

ZMQLib= -L $(ToolDAQPath)/zeromq-4.0.7/lib -lzmq 
ZMQInclude= -I $(ToolDAQPath)/zeromq-4.0.7/include/ 

BoostLib= -L $(ToolDAQPath)/boost_1_66_0/install/lib -lboost_date_time -lboost_serialization -lboost_iostreams
BoostInclude= -I $(ToolDAQPath)/boost_1_66_0/install/include

DataModelInclude = 
DataModelLib = 

MyToolsInclude =
MyToolsLib = 

debug: all

all: lib/libStore.so lib/libLogging.so lib/libDataModel.so include/Tool.h lib/libMyTools.so lib/libServiceDiscovery.so lib/libToolChain.so main RemoteControl  NodeDaemon

main: src/main.cpp | lib/libMyTools.so lib/libStore.so lib/libLogging.so lib/libToolChain.so lib/libDataModel.so lib/libServiceDiscovery.so
	@echo -e "\e[38;5;226m\n*************** Making " $@ "****************\e[0m"
	g++ $(CXXFLAGS) src/main.cpp -o main -I include -L lib -lStore -lMyTools -lToolChain -lDataModel -lLogging -lServiceDiscovery -lpthread $(DataModelInclude) $(DataModelLib) $(MyToolsInclude)  $(MyToolsLib) $(ZMQLib) $(ZMQInclude)  $(BoostLib) $(BoostInclude)


lib/libStore.so: $(ToolDAQPath)/ToolDAQFramework/src/Store/*
	cd $(ToolDAQPath)/ToolDAQFramework && $(MAKE) lib/libStore.so
	@echo -e "\e[38;5;118m\n*************** Copying " $@ "****************\e[0m"
	cp $(ToolDAQPath)/ToolDAQFramework/src/Store/*.h include/
	cp $(ToolDAQPath)/ToolDAQFramework/lib/libStore.so lib/
	#g++ -g -O2 -fPIC -shared  -I include $(ToolDAQPath)/ToolDAQFramework/src/Store/*.cpp -o lib/libStore.so $(BoostLib) $(BoostInclude)


include/Tool.h:  $(ToolDAQPath)/ToolDAQFramework/src/Tool/Tool.h
	@echo -e "\e[38;5;118m\n*************** Copying " $@ "****************\e[0m"
	cp $(ToolDAQPath)/ToolDAQFramework/src/Tool/Tool.h include/
	cp UserTools/*.h include/
	cp UserTools/*/*.h include/
	cp DataModel/*.h include/


lib/libToolChain.so: $(ToolDAQPath)/ToolDAQFramework/src/ToolChain/* | lib/libLogging.so lib/libStore.so lib/libMyTools.so lib/libServiceDiscovery.so lib/libLogging.so lib/libDataModel.so
	@echo -e "\e[38;5;226m\n*************** Making " $@ "****************\e[0m"
	cp $(ToolDAQPath)/ToolDAQFramework/UserTools/Factory/*.h include/
	cp $(ToolDAQPath)/ToolDAQFramework/src/ToolChain/*.h include/
	g++ $(CXXFLAGS) -shared $(ToolDAQPath)/ToolDAQFramework/src/ToolChain/ToolChain.cpp -I include -lpthread -L lib -lStore -lDataModel -lServiceDiscovery -lLogging -lMyTools -o lib/libToolChain.so $(DataModelInclude) $(DataModelLib) $(ZMQLib) $(ZMQInclude) $(MyToolsInclude)  $(BoostLib) $(BoostInclude)


clean: 
	@echo -e "\e[38;5;201m\n*************** Cleaning up ****************\e[0m"
	rm -f include/*.h
	rm -f lib/*.so
	rm -f main
	rm -f RemoteControl
	rm -f NodeDaemon
	rm -f UserTools/*/*.o
	rm -f DataModel/*.o

lib/libDataModel.so: DataModel/* lib/libLogging.so lib/libStore.so $(patsubst DataModel/%.cpp, DataModel/%.o, $(wildcard DataModel/*.cpp))
	@echo -e "\e[38;5;226m\n*************** Making " $@ "****************\e[0m"
	cp DataModel/*.h include/
	#g++ -g -O2 -fPIC -shared DataModel/*.cpp -I include -L lib -lStore  -lLogging  -o lib/libDataModel.so $(DataModelInclude) $(DataModelLib) $(ZMQLib) $(ZMQInclude)  $(BoostLib) $(BoostInclude)
	g++ $(CXXFLAGS) -shared DataModel/*.o -I include -L lib -lStore -lLogging -o lib/libDataModel.so $(DataModelInclude) $(DataModelLib) $(ZMQLib) $(ZMQInclude) $(BoostLib) $(BoostInclude)

lib/libMyTools.so: UserTools/*/* UserTools/* include/Tool.h  lib/libLogging.so lib/libStore.so  $(patsubst UserTools/%.cpp, UserTools/%.o, $(wildcard UserTools/*/*.cpp)) |lib/libDataModel.so
	@echo -e "\e[38;5;226m\n*************** Making " $@ "****************\e[0m"
	cp UserTools/*/*.h include/
	cp UserTools/*.h include/
	#g++ -g -O2 -fPIC -shared  UserTools/Factory/Factory.cpp -I include -L lib -lStore -lDataModel -lLogging -o lib/libMyTools.so $(MyToolsInclude) $(MyToolsLib) $(DataModelInclude) $(DataModelLib) $(ZMQLib) $(ZMQInclude) $(BoostLib) $(BoostInclude)
	g++ $(CXXFLAGS) -shared UserTools/*/*.o -I include -L lib -lStore -lDataModel -lLogging -o lib/libMyTools.so $(MyToolsInclude) $(DataModelInclude) $(MyToolsLib) $(ZMQLib) $(ZMQInclude) $(BoostLib) $(BoostInclude)

RemoteControl:
	cd $(ToolDAQPath)/ToolDAQFramework/ && $(MAKE) RemoteControl
	@echo -e "\e[38;5;118m\n*************** Copying " $@ "****************\e[0m"
	cp $(ToolDAQPath)/ToolDAQFramework/RemoteControl ./

NodeDaemon: 
	cd $(ToolDAQPath)/ToolDAQFramework/ && $(MAKE) NodeDaemon
	@echo -e "\e[38;5;226m\n*************** Copying " $@ "****************\e[0m"
	cp $(ToolDAQPath)/ToolDAQFramework/NodeDaemon ./

lib/libServiceDiscovery.so: $(ToolDAQPath)/ToolDAQFramework/src/ServiceDiscovery/* | lib/libStore.so
	cd $(ToolDAQPath)/ToolDAQFramework && $(MAKE) lib/libServiceDiscovery.so
	@echo -e "\e[38;5;118m\n*************** Copying " $@ "****************\e[0m"
	cp $(ToolDAQPath)/ToolDAQFramework/src/ServiceDiscovery/ServiceDiscovery.h include/
	cp $(ToolDAQPath)/ToolDAQFramework/lib/libServiceDiscovery.so lib/
	#g++ -shared -fPIC -I include $(ToolDAQPath)/ToolDAQFramework/src/ServiceDiscovery/ServiceDiscovery.cpp -o lib/libServiceDiscovery.so -L lib/ -lStore  $(ZMQInclude) $(ZMQLib) $(BoostLib) $(BoostInclude)

lib/libLogging.so:  $(ToolDAQPath)/ToolDAQFramework/src/Logging/* | lib/libStore.so
	cd $(ToolDAQPath)/ToolDAQFramework && $(MAKE) lib/libLogging.so
	@echo -e "\e[38;5;118m\n*************** Copying " $@ "****************\e[0m"
	cp $(ToolDAQPath)/ToolDAQFramework/src/Logging/Logging.h include/
	cp $(ToolDAQPath)/ToolDAQFramework/lib/libLogging.so lib/
	#g++ -shared -fPIC -I include $(ToolDAQPath)/ToolDAQFramework/src/Logging/Logging.cpp -o lib/libLogging.so -L lib/ -lStore $(ZMQInclude) $(ZMQLib) $(BoostLib) $(BoostInclude)

update:
	@echo -e "\e[38;5;51m\n*************** Updating ****************\e[0m"
	cd $(ToolDAQPath)/ToolDAQFramework; git pull
	cd $(ToolDAQPath)/zeromq-4.0.7; git pull
	git pull


UserTools/%.o: UserTools/%.cpp lib/libStore.so include/Tool.h lib/libLogging.so lib/libDataModel.so
	@echo -e "\e[38;5;226m\n*************** Making " $@ "****************\e[0m"
	cp $(shell dirname $<)/*.h include
	-g++ -c $(CXXFLAGS) -o $@ $< -I include -L lib -lStore -lDataModel -lLogging $(MyToolsInclude) $(MyToolsLib) $(DataModelInclude) $(DataModelLib) $(ZMQLib) $(ZMQInclude) $(BoostLib) $(BoostInclude)

target: remove $(patsubst %.cpp, %.o, $(wildcard UserTools/$(TOOL)/*.cpp))

remove:
	echo -e "removing"
	-rm UserTools/$(TOOL)/*.o

DataModel/%.o: DataModel/%.cpp lib/libLogging.so lib/libStore.so
	@echo -e "\e[38;5;226m\n*************** Making " $@ "****************\e[0m"
	cp $(shell dirname $<)/*.h include
	-g++ -c $(CXXFLAGS) -o $@ $< -I include -L lib -lStore -lLogging  $(DataModelInclude) $(DataModelLib) $(ZMQLib) $(ZMQInclude) $(BoostLib) $(BoostInclude)


Docs:
	doxygen Doxyfile
