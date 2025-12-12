#ifndef DATAMODEL_H
#define DATAMODEL_H

#include <vector>
#include <atomic>
#include <mutex>

#include "DAQDataModelBase.h"
#include "Pool.h"
#include "JobQueue.h"

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

using namespace ToolFramework;

class DataModel : public DAQDataModelBase {
	
	public:
	DataModel(); ///< Simple constructor
	
	private:
	
	DAQUtilities utils; ///< for thread management
	
	// Tools can add connections to this and the SocketManager
	// will periodically invoke UpdateConnections to connect clients
	std::map<std::string, ManagedSocket*> managed_sockets;
	std::mutex managed_sockets_mtx;
	
	Pool<Job> job_pool; ///< pool of job structures to encapsulate jobs
	JobQueue job_queue; ///< job queue to submit jobs to job manager
	uint32_t thread_cap; ///< total number of thread cap to use in the program
	std::atomic<uint32_t> num_threads; ///< current number of threads
	unsigned int worker_threads;
	unsigned int max_worker_threads;
	
	/* ----------------------------------------- */
	/*          MulticastReceiveSender           */
	/* ----------------------------------------- */
	
	// pool of string buffers:
	// the receiver thread grabs a vector from the pool, fills it,
	// the pushes the filled vector into the in_multicast_msg_queue
	// and grabs a new vector from the pool
	// FIXME base pool size on available RAM and struct size / make configurable
	// Pool::Pool(bool in_manage=false, uint16_t period_ms=1000, size_t in_object_cap=1)
	Pool<std::vector<std::string>> multicast_buffer_pool(true, 5000, 100);
	
	// batches of received messages, both logging and monitoring
	// FIXME make these pairs or structs, container+mtx
	// FIXME if instead of just a vector<string> we used MulticastBatch, we could accumulate the length
	// and then reserve in advance the length of the string needed for the combined message....?
	 // XXX actually only if we tracked by topic, as one vector<string> gets turned into 5 topical concat'd strings...
	std::vector<std::vector<std::string>*> in_multicast_msg_queue;
	std::mutex in_multicast_msg_queue_mtx;
	
	// Logging
	// -------
	// Tracking
	//{ TODO encapsulate in Tool monitoring struct?
	std::atomic<int> log_polls_failed; // error polling socket
	std::atomic<int> log_recv_fails;  // error in recv_from
	std::atomic<int> logs_recvd; // messages successfully received
	std::atomic<int> log_in_buffer_transfers; // transfers of thread-local message vector to datamodel
	std::atomic<int> log_out_buffer_transfers; // transfers of thread-local message vector to datamodel
	std::atomic<int> log_thread_crashes; // restarts of logging thread (main thread found reader thread 'running' was false)
	//}
	// outgoing logging messages
	std::vector<std::string> out_log_msg_queue;
	std::mutex out_log_msg_queue_mtx;
	
	// Monitoring
	// ----------
	//{
	std::atomic<int> mon_polls_failed;
	std::atomic<int> mon_recv_fails;
	std::atomic<int> mons_recvd;
	std::atomic<int> mon_in_buffer_transfers; // transfers of thread-local message vector to datamodel
	std::atomic<int> mon_out_buffer_transfers; // transfers of thread-local message vector to datamodel
	std::atomic<int> mon_thread_crashes;
	//}
	// outgoing monitoring messages
	std::vector<std::string> out_mon_msg_queue;
	std::mutex out_mon_msg_queue_mtx;
	
	// pool is shared between read and write query receivers
	Pool<QueryBatch> querybatch_pool(true, 5000, 100);
	
	/* ----------------------------------------- */
	/*               PubReceiver                 */
	/* ----------------------------------------- */
	// TODO Tool monitoring struct?
	std::vector<QueryBatch*> write_msg_queue;
	std::mutex write_msg_queue_mtx;
	std::atomic<int> write_polls_failed;
	std::atomic<int> write_msgs_rcvd;
	std::atomic<int> write_rcv_fails;
	std::atomic<int> write_bad_msgs;
	std::atomic<int> write_buffer_transfers;
	std::atomic<int> pub_rcv_thread_crashes;
	//}
	
	/* ----------------------------------------- */
	/*                 ReadReply                 */
	/* ----------------------------------------- */
	// TODO Tool monitoring struct?
	std::vector<QueryBatch*> read_msg_queue;
	std::mutex read_msg_queue_mtx;
	std::vector<QueryBatch*> query_replies;
	std::mutex query_replies_mtx;
	
	std::atomic<int> readrep_polls_failed;
	std::atomic<int> readrep_msgs_rcvd;
	std::atomic<int> readrep_rcv_fails;
	std::atomic<int> readrep_bad_msgs;
	std::atomic<int> readrep_reps_sent;
	std::atomic<int> readrep_rep_send_fails;
	std::atomic<int> readrep_in_buffer_transfers;
	std::atomic<int> readrep_out_buffer_transfers;
	std::atomic<int> read_rcv_thread_crashes;
	
	
	/* ----------------------------------------- */
	/*              MulticastWorkers             */
	/* ----------------------------------------- */
	// each element is a batch of JSON that can be inserted by the DatabaseWorkers
	// FIXME these strings represent batches of multicast messages, so could be very large.
	// each push_back could require reallocation, which could involve moving a lot of very large message buffers
	// FIXME make these pointers, put the strings (maybe make a struct? maybe just a typedef/alias?) in a pool?
	std::vector<std::string> log_query_queue;
	std::mutex log_query_queue_mtx;
	
	std::vector<std::string> mon_query_queue;
	std::mutex mon_query_queue_mtx;
	
	std::vector<std::string> rootplot_query_queue;
	std::mutex rootplot_query_queue_mtx;
	
	std::vector<std::string> plotlyplot_query_queue;
	std::mutex plotlyplot_query_queue_mtx;
	
	std::atomic<int> multicast_job_distributor_thread_crashes;
	std::atomic<int> multicast_worker_job_fails;
	std::atomic<int> multicast_worker_job_successes;
	
	/* ----------------------------------------- */
	/*                WriteWorkers               */
	/* ----------------------------------------- */
	std::atomic<int> write_job_distributor_thread_crashes;
	
	std::vector<QueryBatch*> write_query_queue;
	std::mutex write_query_queue_mtx;
	
	std::atomic<int> write_worker_job_fails;
	std::atomic<int> write_worker_job_successes;
	
	/* ----------------------------------------- */
	/*               DatabaseWorkers             */
	/* ----------------------------------------- */
	
	std::atomic<int> database_job_distributor_thread_crashes;
	std::vector<QueryBatch*> read_replies; // output, awaiting for result conversion
	std::mutex read_replies_mtx;
	
	std::atomic<int> db_worker_job_successes; // FIXME add for others
	std::atomic<int> db_worker_job_fails;
	
	/* ----------------------------------------- */
	/*                ResultWorkers              */
	/* ----------------------------------------- */
	std::atomic<int> result_job_distributor_thread_crashes;
	
	std::atomic<int> result_worker_job_fails;
	std::atomic<int> result_worker_job_successes;
	
	/* ----------------------------------------- */
	/*                  Monitoring               */
	/* ----------------------------------------- */
	std::atomic<int> monitoring_thread_crashes;
	
	/* ----------------------------------------- */
	/*                 SocketManager             */
	/* ----------------------------------------- */
	std::atomic<int> socket_manager_thread_crashes;
	
};



#endif
