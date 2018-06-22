#include "S3IteratorText.h"
#include "Utils.h"
#include <unistd.h>
#include <vector>
#include <iostream>

#include <pthread.h>
#include <semaphore.h>

#define FETCH_SIZE (10 * 1024 * 1024) //  size we try to fetch at a time

//#define DEBUG

// The way S3IteratorText works
// groups of minibatches are distributed across multiple files in a range left_id..right_id
// we want to change this so that all samples are in the same file

// example
// imagine input is in libsvm formta
// <label> <index1>:<value1> <index2>:<value2> ...
// at each iteration we read ~10MB of data

namespace cirrus {
  
S3IteratorText::S3IteratorText(
        const Configuration& c,
        uint64_t file_size,
        uint64_t minibatch_rows, // number of samples in a minibatch
        bool use_label,          // whether each sample has a label
        int worker_id,           // id of this worker
        bool random_access) :    // whether to access samples in a random fashion
    left_id(left_id), right_id(right_id),
    conf(c), s3_rows(s3_rows),
    minibatch_rows(minibatch_rows),
    minibatches_list(100000),
    use_label(use_label),
    worker_id(worker_id),
    re(worker_id),
    random_access(random_access)
{
      
  std::cout << "S3IteratorText::Creating S3IteratorText"
    << " left_id: " << left_id
    << " right_id: " << right_id
    << " use_label: " << use_label
    << std::endl;

  // initialize s3
  s3_initialize_aws();
  s3_client.reset(s3_create_client_ptr());

  for (uint64_t i = 0; i < read_ahead; ++i) {
    pref_sem.signal();
  }

  sem_init(&semaphore, 0, 0);

  thread = new std::thread(std::bind(&S3IteratorText::thread_function, this, c));

  // we fix the random seed but make it different for every worker
  // to ensure each worker receives a different minibatch
  if (random_access) {
    srand(42 + worker_id);
  } else {
    current = left_id;
  }
}

const void* S3IteratorText::get_next_fast() {
  // we need to delete entry
  if (to_delete != -1) {
#ifdef DEBUG
    std::cout << "get_next_fast::Deleting entry: " << to_delete
      << std::endl;
#endif
    list_strings.erase(to_delete);
#ifdef DEBUG
    std::cout << "get_next_fast::Deleted entry: " << to_delete
      << std::endl;
#endif
  }
 
  //std::cout << "sem_wait" << std::endl; 
  sem_wait(&semaphore);
  ring_lock.lock();

  // first discard empty queue
  while (minibatches_list.front()->size() == 0) {
    auto queue_ptr = minibatches_list.pop();
    delete queue_ptr; // free memory of empty queue
  }
  auto ret = minibatches_list.front()->front();
  minibatches_list.front()->pop();
  num_minibatches_ready--;
  ring_lock.unlock();

#ifdef DEBUG
  if (ret.second != -1) {
    std::cout << "get_next_fast::ret.second: " << ret.second << std::endl;
  }
#endif

  to_delete = ret.second;

  if (num_minibatches_ready < 200 && pref_sem.getvalue() < (int)read_ahead) {
#ifdef DEBUG
    std::cout << "get_next_fast::pref_sem.signal" << std::endl;
#endif
    pref_sem.signal();
  }

  return ret.first;
}

// XXX we need to build minibatches from S3 objects
// in a better way to allow support for different types
// of minibatches
void S3IteratorText::push_samples(std::ostringstream* oss) {
  uint64_t n_minibatches = s3_rows / minibatch_rows;

  list_strings[str_version] = oss->str();
  delete oss;

  auto str_iter = list_strings.find(str_version);
  print_progress(str_iter->second);

  // parse the contents
  const void* s3_data = reinterpret_cast<const void*>(str_iter->second.c_str());
  int s3_obj_size = load_value<int>(s3_data);
  int num_samples = load_value<int>(s3_data);
  (void)s3_obj_size;
  (void)num_samples;
#ifdef DEBUG
  std::cout
    << "push_samples s3_obj_size: " << s3_obj_size
    << " num_samples: " << num_samples << std::endl;
  assert(s3_obj_size > 0 && s3_obj_size < 100 * 1024 * 1024);
  assert(num_samples > 0 && num_samples < 1000000);
#endif

  // we parse this piece of text
  // this returns a collection of minibatches
  auto dataset = parse_s3_obj_libsvm();

  ring_lock.lock();
  minibatches_list.add(dataset);
  ring_lock.unlock();
  for (uint64_t i = 0; i < n_minibatches; ++i) {
    num_minibatches_ready++;
    sem_post(&semaphore);
  }
  str_version++;
}

/**
  * This function is used to compute the amount of S3 data consumed
  * per second. This might not be the same as the available S3
  * bandwidth if the system is bottleneck somewhere else
  */
void S3IteratorText::print_progress(const std::string& s3_obj) {
  static uint64_t start_time = 0;
  static uint64_t total_received = 0;
  static uint64_t count = 0;

  if (start_time == 0) {
    start_time = get_time_us();
  }
  total_received += s3_obj.size();
  count++;

  double elapsed_sec = (get_time_us() - start_time) / 1000.0 / 1000.0;
  std::cout
    << "Getting object count: " << count
    << " s3 e2e bw (MB/s): " << total_received / elapsed_sec / 1024.0 / 1024
    << std::endl;
}

static int sstream_size(std::ostringstream& ss) {
  return ss.tellp();
}

/**
  * Returns a range of bytes (right side is exclusive)
  */
std::make_pair<uint64_t, uint64_t>
S3IteratorText::get_file_range(uint64_t file_size) {
  // given the size of the file we return a random file index
  if (file_size < FETCH_SIZE) {
    // file is small so we get the whole file
    return std::make_pair(0, file_size);
  }

  // we sample the left side of the range
  std::uniform_int_distribution<int> sampler(0, file_size - 1);
  uint64_t left_index = sampler(re);
  if (file_size - left_index < FETCH_SIZE) {
    // make sure we get a range with size FETCH_SIZE
    left_index = file_size - FETCH_SIZE;
  }

  return std::make_pair(left_index, left_index + FETCH_SIZE);

}

void report_bandwidth() {
  uint64_t elapsed_us = (get_time_us() - start);
  double mb_s = sstream_size(*s3_obj) / elapsed_us
    * 1000.0 * 1000 / 1024 / 1024;
  std::cout << "received s3 obj"
    << " elapsed: " << elapsed_us
    << " size: " << sstream_size(*s3_obj)
    << " BW (MB/s): " << mb_s
    << "\n";

#ifdef DEBUG
      //double MBps = (1.0 * (32812.5*1024.0) / elapsed_us) / 1024 / 1024 * 1000 * 1000;
      //std::cout << "Get s3 obj took (us): " << (elapsed_us)
      //  << " size (KB): " << 32812.5
      //  << " bandwidth (MB/s): " << MBps
      //  << std::endl;
#endif
}

void S3IteratorText::thread_function(const Configuration& config) {
  std::cout << "Building S3 deser. with size: "
    << std::endl;

  uint64_t count = 0;
  while (1) {
    // if we can go it means there is a slot
    // in the ring
    std::cout << "Waiting for pref_sem" << std::endl;
    pref_sem.wait();

    // FIXME we should allow random and non-random
    // random range of bytes to be fetched from dataset
    std::pair<uint64_t, uint64_t> range = get_file_range(file_size);

    std::ostringstream* s3_obj;
try_start:
    try {
      std::cout << "S3IteratorText: getting object " << obj_id_str << std::endl;
      uint64_t start = get_time_us();

      s3_obj = s3_get_object_range_ptr(
          config.get_s3_dataset_key(), *s3_client,
          config.get_s3_bucket(), range);

      report_bandwidth(get_time_us() - start, sstream_size(*s3_obj));
    } catch(...) {
      std::cout
        << "S3IteratorText: error in s3_get_object"
        << " obj_id_str: " << obj_id_str
        << std::endl;
      goto try_start;
      exit(-1);
    }
    push_samples(s3_obj);
  }
}

} // namespace cirrus

