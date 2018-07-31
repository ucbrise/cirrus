#ifndef _S3_ITERATOR_H_
#define _S3_ITERATOR_H_

#include "S3.h"
#include "S3Client.h"
#include <Synchronization.h>
#include "Configuration.h"
#include "config.h"

#include <CircularBuffer.h>
#include <thread>
#include <list>
#include <mutex>
#include "Serializers.h"

namespace cirrus {

class S3Iterator {
 public:
    S3Iterator(
        uint64_t left_id, uint64_t right_id,
        const Configuration& c,
        uint64_t s3_rows, uint64_t s3_cols,
        uint64_t minibatch_rows,
        const std::string&);

    const FEATURE_TYPE* get_next_fast();

    void thread_function();

 private:
  void push_samples(const std::shared_ptr<FEATURE_TYPE>& samples);
  void push_samples(std::ostringstream* oss);

  uint64_t left_id;
  uint64_t right_id;

  Configuration conf;

  std::shared_ptr<S3Client> s3_client;

  uint64_t last;
  std::list<std::shared_ptr<FEATURE_TYPE>> ring;

  uint64_t read_ahead = 1;

  std::thread* thread;   // background thread
  std::mutex ring_lock;  // used to synchronize access
  // used to control number of blocks to prefetch
  PosixSemaphore pref_sem;

  uint64_t s3_rows;
  uint64_t s3_cols;
  uint64_t minibatch_rows;

  std::string s3_bucket_name;

  int to_delete = -1;
  sem_t get_s3_data_semaphore;
  int str_version = 0;
  std::map<int, std::string> list_strings; // strings from s3
  CircularBuffer<std::pair<const FEATURE_TYPE*, int>> minibatches_list;
};

} // namespace cirrus

#endif  // _S3_ITERATOR_H_
