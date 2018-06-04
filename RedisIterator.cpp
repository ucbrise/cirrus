#include "RedisIterator.h"
#include <unistd.h>
#include <vector>
#include <iostream>

RedisIterator::RedisIterator(
        uint64_t left_id, uint64_t right_id,
        const std::string& IP, int port) :
    left_id(left_id), right_id(right_id), IP(IP), port(port) {
  r = redis_connect(IP.c_str(), port);
  if (r == nullptr) {
    std::cout << "Not able to connect" << std::endl;
    exit(-1);
  }

  std::cout << "Creating iterator"
            << " left_id: " << left_id
            << " right_id: " << right_id
            << std::endl;

  cur = left_id;
  last = left_id;  // last is exclusive
  thread = new std::thread(std::bind(&RedisIterator::thread_function, this));
}

char* RedisIterator::get_next() {
  std::cout << "Get next "
    << " cur: " << cur
    << " last: " << last
    << "\n";
  while (1) {
    ring_lock.lock();
    if (ring.empty()) {
      ring_lock.unlock();
      usleep(500);
    } else {
      break;
    }
  }

  char* ret = ring.front();
  ring.pop_front();
  cur++;
  if (cur == right_id) {
    cur = left_id;
  }
  ring_lock.unlock();

  std::cout << "Returning prefetched batch"
    << " cur: " << cur
    << " last: " << last
    << std::endl;
  return ret;
}

void RedisIterator::thread_function() {
  while (1) {
    ring_lock.lock();
    if (ring.size() < read_ahead) {
      // prefetch
      uint64_t to_read = read_ahead - ring.size();
      ring_lock.unlock();
      std::vector<char*> prefetched;
      for (uint64_t i = 0; i < to_read; ++i) {
        char*data = redis_get_numid(r, last);
        if (data == nullptr) {
          std::cout << "Wasn't able to find" << std::endl;
          exit(-1);
        }
        prefetched.push_back(data);
        last++;
        if (last == right_id)
          last = left_id;
      }
      ring_lock.lock();
      for (const auto& p : prefetched) {
        ring.push_back(p);
      }
      ring_lock.unlock();

    } else {
      ring_lock.unlock();
    }
    usleep(500);
  }
}

