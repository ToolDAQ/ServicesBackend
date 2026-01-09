#ifndef MonitoringVariables_H
#define MonitoringVariables_H
#include <mutex>
#include <Store.h>

class MonitoringVariables {
	public:
	MonitoringVariables(){};
	virtual ~MonitoringVariables(){};
	virtual std::string toJSON(){ return ""; };
	ToolFramework::Store vars;
	std::mutex mtx;
	void Clear(){
		std::unique_lock<std::mutex> locker(mtx);
		vars.Delete();
		return;
	}
	
	template<typename T>
	void Set(const std::string& key, T val){
		std::unique_lock<std::mutex> locker(mtx);
		vars.Set(key, val);
		return;
	}
	
	std::string GetJSON(){
		std::unique_lock<std::mutex> locker(mtx);
		std::string ret;
		vars >> ret;
		std::string ret2 = toJSON();
		if(ret.length()==2) return ret2; // if nothing in Store, return result from toJSON
		if(!ret2.empty()){
			ret.pop_back(); // remove trailing '}'
			ret2[0]=','; // replace leading '{' with ',' to concatenate the two
			ret += ret2;
		}
		return ret;
	}
	
};

#endif
