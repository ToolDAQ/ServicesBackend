#ifndef DATAMODEL_H
#define DATAMODEL_H

#include <vector>
#include <atomic>
#include <mutex>
#include <shared_mutex>

#include "DAQDataModelBase.h"
#include "Pool.h"
#include "JobQueue.h"
#include "QueryBatch.h"
#include "ManagedSocket.h"
#include "query_topics.h"
#include "type_name_as_string.h" // mostly for debug
#include "MLogger.h"
class MonitoringVariables;

/**
* \class DataModel
*
* This class is a transient data model class for your Tools within the ToolChain. If Tools need to communicate they pass all data through the data model. Therefore inter-tool data variables should be defined in this class.
 *
 *
 * $Author: B.Richards $
 * $Date: 2019/05/26 $
 * Contact: benjamin.richards@warwick.ac.uk
 *
*/

// some max size to prevent decompression buffer being resized to something absurd in case of strangeness
const size_t MAX_DECOMPRESSED_MSG_SIZE=655356;

using namespace ToolFramework;

class DataModel : public DAQDataModelBase {
	
	public:
	DataModel(); ///< Simple constructor
	
	Utilities utils; ///< for thread management
	MLogger m_logger;
	MLogger* logger;
	
	bool change_config; ///< signaller for Tools to reload their configuration variables
	
	// Tools can add connections to this and the SocketManager
	// will periodically invoke UpdateConnections to connect clients
	std::map<std::string, ManagedSocket*> managed_sockets;
	std::shared_mutex managed_sockets_mtx;
	
	// when a new config is loaded, we pre-fetch it so that we don't need to
	// go back to the database for every device
	std::map<std::string, std::string> cached_configs;
	
	Pool<Job> job_pool; ///< pool of job structures to encapsulate jobs
	JobQueue job_queue; ///< job queue to submit jobs to job manager
	uint32_t thread_cap; ///< total number of thread cap to use in the program
	std::atomic<uint32_t> num_threads; ///< current number of threads
	unsigned int worker_threads;
	unsigned int max_worker_threads;
	
	std::map<std::string, MonitoringVariables*> monitoring_variables;
	std::mutex monitoring_variables_mtx;
	
	/* ----------------------------------------- */
	/*          MulticastReceiveSender           */
	/* ----------------------------------------- */
	
	// pool of string buffers:
	// the receiver thread grabs a vector from the pool, fills it,
	// the pushes the filled vector into the in_multicast_msg_queue
	// and grabs a new vector from the pool
	// FIXME base pool size on available RAM and struct size / make configurable
	// Pool::Pool(bool in_manage=false, uint16_t period_ms=1000, size_t in_object_cap=1)
	Pool<std::vector<std::string>> multicast_buffer_pool{true, 5000, 100};
	
	// batches of received messages, both logging and monitoring
	// FIXME make these pairs or structs, container+mtx
	// FIXME if instead of just a vector<string> we used MulticastBatch, we could accumulate the length
	// and then reserve in advance the length of the string needed for the combined message....?
	 // XXX actually only if we tracked by topic, as one vector<string> gets turned into 5 topical concat'd strings...
	std::vector<std::vector<std::string>*> in_multicast_msg_queue;
	std::mutex in_multicast_msg_queue_mtx;
	
	// outgoing logging messages
	std::vector<std::string> out_log_msg_queue;
	std::mutex out_log_msg_queue_mtx;
	
	// outgoing monitoring messages
	std::vector<std::string> out_mon_msg_queue;
	std::mutex out_mon_msg_queue_mtx;
	
	// pool is shared between read and write query receivers
	Pool<QueryBatch> querybatch_pool{true, 5000, 100};
	
	/* ----------------------------------------- */
	/*               PubReceiver                 */
	/* ----------------------------------------- */
	std::vector<QueryBatch*> write_msg_queue;
	std::mutex write_msg_queue_mtx;
	
	/* ----------------------------------------- */
	/*                 ReadReply                 */
	/* ----------------------------------------- */
	// TODO Tool monitoring struct?
	std::vector<QueryBatch*> read_msg_queue;
	std::mutex read_msg_queue_mtx;
	std::deque<QueryBatch*> query_replies;
	std::mutex query_replies_mtx;
	
	/* ----------------------------------------- */
	/*              MulticastWorkers             */
	/* ----------------------------------------- */
	// each element is a batch of JSON that can be inserted by the DatabaseWorkers
	// FIXME these strings represent batches of multicast messages, so could be very large.
	// each push_back could require reallocation, which could involve moving a lot of very large message buffers
	// FIXME make these pointers, put the strings (maybe make a struct? maybe just a typedef/alias?) in a pool?
	Pool<std::string> multicast_batch_pool{true, 5000, 100};
	
	std::vector<std::string*> log_query_queue;
	std::mutex log_query_queue_mtx;
	
	std::vector<std::string*> mon_query_queue;
	std::mutex mon_query_queue_mtx;
	
	std::vector<std::string*> rootplot_query_queue;
	std::mutex rootplot_query_queue_mtx;
	
	std::vector<std::string*> plotlyplot_query_queue;
	std::mutex plotlyplot_query_queue_mtx;
	
	/* ----------------------------------------- */
	/*                WriteWorkers               */
	/* ----------------------------------------- */
	std::vector<QueryBatch*> write_query_queue;
	std::mutex write_query_queue_mtx;
	
	/* ----------------------------------------- */
	/*                ReadWorkers               */
	/* ----------------------------------------- */
	std::vector<QueryBatch*> read_query_queue;
	std::mutex read_query_queue_mtx;
	
	/* ----------------------------------------- */
	/*               DatabaseWorkers             */
	/* ----------------------------------------- */
	
	std::vector<QueryBatch*> query_results; // output, awaiting for result conversion
	std::mutex query_results_mtx;
	
	private:
	
};



#endif
