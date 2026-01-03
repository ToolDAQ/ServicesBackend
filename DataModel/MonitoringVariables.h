#ifndef MonitoringVariables_H
#define MonitoringVariables_H
#include <mutex>
#include <Store.h>

class MonitoringVariables {
	public:
	MonitoringVariables(){};
	virtual ~MonitoringVariables(){};
	virtual std::string toJSON()=0;
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
	
	std::string GetJson(){
		std::unique_lock<std::mutex> locker(mtx);
		std::string ret;
		vars >> ret;
		ret.pop_back(); // remove trailing '}'
		std::string ret2 = toJSON();
		ret2[0]=','; // replace leading '{' with ',' to concatenate the two
		return ret+ret2;
	}
	
};

#endif
