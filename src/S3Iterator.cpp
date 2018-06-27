#include "S3Iterator.h"
#include "Utils.h"
#include <unistd.h>
#include <vector>
#include <iostream>

#include <pthread.h>
#include <semaphore.h>

namespace cirrus {

// s3_cad_size nmber of samples times features per sample
S3Iterator::S3Iterator(
        uint64_t left_id, uint64_t right_id,
        const Configuration& c,
        uint64_t s3_rows, uint64_t s3_cols,
        uint64_t minibatch_rows,
        const std::string& s3_bucket_name) :
    left_id(left_id), right_id(right_id),
    conf(c), s3_rows(s3_rows), s3_cols(s3_cols),
    minibatch_rows(minibatch_rows), s3_bucket_name(s3_bucket_name),
    minibatches_list(100000) {
      
  std::cout << "Creating S3Iterator"
    << " left_id: " << left_id
    << " right_id: " << right_id
    << std::endl;

  // initialize s3
  s3_initialize_aws();
  s3_client = std::make_shared<S3Client>();

  last = left_id;  // last is exclusive

  for (uint64_t i = 0; i < read_ahead; ++i) {
    pref_sem.signal();
  }

  sem_init(&get_s3_data_semaphore, 0, 0);

  thread = new std::thread(std::bind(&S3Iterator::thread_function, this));
}

const FEATURE_TYPE* S3Iterator::get_next_fast() {
  // we need to delete entry
  if (to_delete != -1) {
    std::cout << "get_next_fast::Deleting entry: " << to_delete
      << std::endl;
    list_strings.erase(to_delete);
    std::cout << "get_next_fast::Deleted entry: " << to_delete
      << std::endl;
  }
  
  sem_wait(&get_s3_data_semaphore);
  ring_lock.lock();

  auto ret = minibatches_list.pop();
  
  uint64_t ring_size = minibatches_list.size();
  ring_lock.unlock();

  if (ret.second != -1) {
    std::cout << "get_next_fast::ret.second: " << ret.second << std::endl;
  }

  to_delete = ret.second;

  if (ring_size < 20 && pref_sem.getvalue() < (int)read_ahead) {
    std::cout << "get_next_fast::pref_sem.signal!!!" << std::endl;
    pref_sem.signal();
  }

  return ret.first;
}

void S3Iterator::push_samples(std::ostringstream* oss) {
  uint64_t n_minibatches = s3_rows / minibatch_rows;
  uint64_t minibatch_n_entries = minibatch_rows * (s3_cols + 1);

  std::cout << "n_minibatches: " << n_minibatches << std::endl;

  // save s3 object into list of string
  std::chrono::steady_clock::time_point start =
    std::chrono::steady_clock::now();
  list_strings[str_version] = oss->str();
  delete oss;
  std::chrono::steady_clock::time_point finish =
    std::chrono::steady_clock::now();
  uint64_t elapsed_ns =
    std::chrono::duration_cast<std::chrono::nanoseconds>(
        finish-start).count();
  std::cout << "oss->str() time (ns): " << elapsed_ns << std::endl;

  auto str_iter = list_strings.find(str_version);

  ring_lock.lock();
  // create a pointer to each minibatch within s3 object and push it
  for (uint64_t i = 0; i < n_minibatches; ++i) {
    const FEATURE_TYPE* data = reinterpret_cast<const FEATURE_TYPE*>(str_iter->second.c_str()) + i * minibatch_n_entries;

    // if it's the last minibatch in object we mark it so it can be deleted
    int is_last = ((i + 1) == n_minibatches) ? str_version : -1;

    minibatches_list.add(std::make_pair(data, is_last));
    sem_post(&get_s3_data_semaphore);
  }
  ring_lock.unlock();
  
  str_version++;
}

void S3Iterator::thread_function() {
  std::cout << "Building S3 deser. with size: "
    << s3_rows << " x " << (s3_cols + 1) << " = " << (s3_rows * (s3_cols + 1))
    << std::endl;

  uint64_t count = 0;
  while (1) {
    // if we can go it means there is a slot
    // in the ring
    std::cout << "Waiting for pref_sem" << std::endl;
    pref_sem.wait();
    std::cout << "Getting object. count: " << count++ << std::endl;

    std::ostringstream* s3_obj;
try_start:
    try {
      std::cout << "S3Iterator: getting object" << std::endl;
      std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();
      s3_obj = s3_client->s3_get_object_ptr(last, s3_bucket_name);
      std::chrono::steady_clock::time_point finish =
        std::chrono::steady_clock::now();
      uint64_t elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            finish-start).count();

      double MBps = (1.0 * (32812.5*1024.0) / elapsed_ns) / 1024 / 1024 * 1000 * 1000 * 1000;
      std::cout << "Get s3 obj took (us): " << (elapsed_ns / 1000.0)
        << " size (KB): " << 32812.5
        << " bandwidth (MB/s): " << MBps
        << std::endl;
    } catch(...) {
      std::cout << "S3Iterator: error in s3_get_object" << std::endl;
      goto try_start;
    }
    
    // update index
    last++;
    if (last == right_id)
      last = left_id;

    std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();
    push_samples(s3_obj);
    std::chrono::steady_clock::time_point finish2 =
      std::chrono::steady_clock::now();
    uint64_t elapsed_ns2 =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          finish2-start).count();
    std::cout << "pushing took (ns): " << elapsed_ns2 << " at (us) " << get_time_us() << std::endl;
  }
}

} // namespace cirrus
