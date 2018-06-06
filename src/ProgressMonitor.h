#ifndef _PROGRESS_MONITOR_H_
#define _PROGRESS_MONITOR_H_

#include "Configuration.h"
#include "Redis.h"

namespace cirrus {

class ProgressMonitor {
  constexpr static const char* PROGRESS_COUNTER = "batch_counter";
 public:
    ProgressMonitor(const std::string& ip, int port);

    void increment_batches(int* prev_batch = nullptr);
    int get_number_batches();

 private:
    redisContext* redis_con;
};

} // namespace cirrus

#endif  // _PROGRESS_MONITOR_H_
