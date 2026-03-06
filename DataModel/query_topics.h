#ifndef QUERY_TYPES_H
#define QUERY_TYPES_H

// used by MulticastWorkers and DatabaseWorkers
// only write query topics
enum class query_topic : char { alarm='A', dev_config='D', run_config='R', calibration='C', logging='L', monitoring='M', rootplot='T', plotlyplot='P', generic='Q' };

#endif

