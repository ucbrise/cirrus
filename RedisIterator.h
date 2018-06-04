#ifndef _REDIS_ITERATOR_H_
#define _REDIS_ITERATOR_H_

#include <thread>
#include <mutex>
#include <list>
#include <string>
#include "Redis.h"

class RedisIterator {
 public:
    RedisIterator(
        uint64_t left_id, uint64_t right_id, const std::string& IP, int port);

    char* get_next();
    void thread_function();

 private:
  uint64_t left_id;   // index of first value in range
  uint64_t right_id;  // index of one after last value in range
  std::string IP;     // redis ip
  int port;           // redis port
  redisContext* r;    // redis handle

  std::thread* thread;    // thread to prefetch data
  std::mutex ring_lock;   // coordinate access to ring
  uint64_t cur;           // current value in range
  uint64_t last;          // last value to be prefetched
  std::list<char*> ring;  // ring

  uint64_t read_ahead = 5;  // how many values to read ahead
};

#endif  // _REDIS_ITERATOR_H_
