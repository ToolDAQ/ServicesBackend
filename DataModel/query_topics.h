#ifndef QUERY_TYPES_H
#define QUERY_TYPES_H

// used by MulticastWorkers and WriteWorkers
// DatabaseWorkers uses cached_config
enum class query_topic : char { alarm='A', dev_config='D', base_config='B', runmode_config='R', cached_config='K', calibration='C', logging='L', monitoring='M', rootplot='T', plotlyplot='P', generic='Q' };

#endif

