#include <iostream>
#include <systemd/sd-journal.h>
#include "Services.h"

using ToolFramework::Services;

// XXX note that because we pass these to sd_journal_send as well, the logging levels here
// are syslog levels (error=3), NOT the standard ToolFramework logging levels (error=0)
// for consistency, use LOG_**** defines from syslog.h

/*
// if all MLogger::Log does is printf, we can avoid the need to redefine printf:
#define LOG(logger,severity, fmt, ...) do {\
    sd_journal_send("PRIORITY=%lu", severity, "MESSAGE=" fmt, ##__VA_ARGS__, NULL); \
    if(severity<=logger->GetVerbosity()) printf(fmt "\n", ##__VA_ARGS__); \
} while(0)
*/

#define LOG(logger,severity, fmt, ...) do {\
    sd_journal_send("PRIORITY=%lu", severity, "MESSAGE=" fmt, ##__VA_ARGS__, NULL); \
    logger->Log(severity, fmt "\n", ##__VA_ARGS__); \
} while(0)

// for places where we don't have access to an MLogger instance.
// we similarly have no verbosity level, so printf always prints - only use for errors
#define SLOG(severity, fmt, ...) do {\
    sd_journal_send("PRIORITY=%lu", severity, "MESSAGE=" fmt, ##__VA_ARGS__, NULL); \
    printf(fmt "\n", ##__VA_ARGS__); \
} while(0)

// could do this, then define MLogger::Log(int severity, const char* file, const char* line, const char* fmt, ...)
// then we could pass on file and line to sd_journal_send manually - avoids building the logging message twice
/*
#define FLOG(logger,severity, fmt, ...) do {\
    logger->Log(severity, __builtin_FILE(), __builtin_LINE(), fmt "\n", ##__VA_ARGS__); \
} while(0)
*/

class MLogger {
	
	public:
	MLogger(int verbose=LOG_WARNING) : m_verbosity(verbose){};
	~MLogger(){}
	
	void SetVerbosity(int verbose){ m_verbosity=verbose; }
	int GetVerbosity(){ return m_verbosity; }
	
	void SetServices(Services*& services){ m_services=&services; }
	
	// support printf-style formatting
	void Log(int severity, const char* fmt, ...){
		
		if(severity>m_verbosity) return;
		
		va_list va;
		va_start(va, fmt);
		
		va_list copy;
		va_copy(copy, va);
		size_t nbytes = vsnprintf(NULL, 0, fmt, copy);
		va_end(copy);
		if(nbytes<0){
			printf("MLogger Error formatting message!\n");
			va_end(va);
			return;
		}
		
		if(nbytes > buffer_size) buffer.resize(nbytes);
		vsnprintf((char*)buffer.data(), nbytes+1, fmt, va);
		
		// 1. print to stdout
		printf(buffer.c_str());
		
		// 2. send to systemd journal
		// we could do this here, but we lose message location in meta-info as it always points to this line
		// so we use the preprocessor macro...for now...
		//sd_journal_send("PRIORITY=%lu", severity, "MESSAGE=%s", buffer.c_str(), NULL);
		
		// 3. send over multicast and insert into DB. Yup, we want to do this, both for posterity (into the DB)
		// and so it shows on the website (webstreamer listens directly to multicast)
		(*m_services)->SendLog(buffer, ToolFramework::LogLevel(severity-3));
		
		va_end(va);
		return;
	}
	
	// support streamer operator
	std::ostream nullstream{nullptr};
	std::ostream& operator()(int msg_verb){
		if(msg_verb>m_verbosity) return std::ref(nullstream);
		return std::ref(std::cout);
	} // e.g. logger(0) << "message" << std::endl;
	
	private:
	size_t buffer_size=0;
	std::string buffer;
	int m_verbosity;
	// we fetch the services on DataModel construction, but they aren't actually constructed
	// until later, so we need to retain a pointer to the DataModel's pointer to the services. -_-
	Services** m_services=nullptr;
	/* for reference verbosity levels #defined in syslog.h:
	LOG_EMERG       0       // system is unusable
	LOG_ALERT       1       // action must be taken immediately
	LOG_CRIT        2       // critical conditions
	LOG_ERR         3       // error conditions
	LOG_WARNING     4       // warning conditions
	LOG_NOTICE      5       // normal but significant condition
	LOG_INFO        6       // informational
	LOG_DEBUG       7       // debug-level messages
	*/
	
};


/*
class MLogger {
	public:
	MLogger(int verbose=1) : m_verbosity(verbose){
		
		int fd = sd_journal_stream_fd("middleman", LOG_INFO, 1);
		if(fd<0){
			std::cerr<<"Failed to create logging stream! fd: "<<strerror(-fd)<<std::endl;
			return;
		}
		FILE* log = fdopen(fd, "a");
		if(!log){
			std::cerr<<"Failed to create file object:"<<strerror(errno)<<std::endl;
			close(fd);
			return;
		}
		
		__gnu_cxx::stdio_filebuf<char> filebuf(log, std::ios::out);
		fstr = std::ostream(&filebuf);
	};
	
	~MLogger(){
		if(log) fclose(log);
		log=nullptr;
	}
	
	std::ostream nullstream{nullptr};
	std::ostream& operator()(int msg_verb){
		if(msg_verb>m_verbosity) return std::ref(nullstream);
		// fstr << SD_WARNING " This is a warning!\n";
		return std::ref(fstr);
	} // e.g. logger(0) << "message" << std::endl;
	
	std::ostream& error() { return (*this)(0); }; // e.g. logger.error() << "message" << std::endl;
	std::ostream& warn()  { return (*this)(1); };
	std::ostream& info()  { return (*this)(2); };
	
	private:
	int m_verbosity;
	std::ostream fstr;
	
};
*/

