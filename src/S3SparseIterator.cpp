#include "S3SparseIterator.h"
#include "Utils.h"
#include <unistd.h>
#include <vector>
#include <iostream>

#include <pthread.h>
#include <semaphore.h>

//#define DEBUG

namespace cirrus {
  
// s3_cad_size nmber of samples times features per sample
S3SparseIterator::S3SparseIterator(uint64_t left_id,
                                   uint64_t right_id,  // right id is exclusive
                                   const Configuration& c,
                                   uint64_t s3_rows,
                                   uint64_t minibatch_rows,
                                   bool use_label,
                                   int worker_id,
                                   bool random_access,
                                   bool has_labels)
    : S3Iterator(c, has_labels),
      left_id(left_id),
      right_id(right_id),
      s3_rows(s3_rows),
      minibatch_rows(minibatch_rows),
      // pm(REDIS_IP, REDIS_PORT),
      minibatches_list(100000),
      use_label(use_label),
      worker_id(worker_id),
      re(worker_id),
      random_access(random_access) {
  std::cout << "S3SparseIterator::Creating S3SparseIterator"
            << " left_id: " << left_id << " right_id: " << right_id
            << " use_label: " << use_label << " has_labels: " << has_labels
            << std::endl;

  // initialize s3
  s3_client = std::make_shared<S3Client>();

  for (uint64_t i = 0; i < read_ahead; ++i) {
    pref_sem.signal();
  }

  sem_init(&semaphore, 0, 0);

  thread =
      new std::thread(std::bind(&S3SparseIterator::threadFunction, this, c));

  // we fix the random seed but make it different for every worker
  // to ensure each worker receives a different minibatch
  if (random_access) {
    srand(42 + worker_id);
  } else {
    current = left_id;
  }
}

std::shared_ptr<SparseDataset> S3SparseIterator::getNext() {
  // we need to delete entry
  if (to_delete != -1) {
#ifdef DEBUG
    std::cout << "getNext::Deleting entry: " << to_delete << std::endl;
#endif
    list_strings.erase(to_delete);
#ifdef DEBUG
    std::cout << "getNext::Deleted entry: " << to_delete << std::endl;
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
    std::cout << "getNext::ret.second: " << ret.second << std::endl;
  }
#endif

  to_delete = ret.second;

  if (num_minibatches_ready < 200 && pref_sem.getvalue() < (int)read_ahead) {
#ifdef DEBUG
    std::cout << "getNext::pref_sem.signal" << std::endl;
#endif
    pref_sem.signal();
  }

  std::shared_ptr<SparseDataset> ds =
      std::make_shared<SparseDataset>(reinterpret_cast<const char*>(ret.first),
                                      config.get_minibatch_size(), has_labels);

#ifdef DEBUG
  ds.check();
#endif
  return ds;
}

// XXX we need to build minibatches from S3 objects
// in a better way to allow support for different types
// of minibatches
void S3SparseIterator::pushSamples(std::ostringstream* oss) {
  uint64_t n_minibatches = s3_rows / minibatch_rows;

#ifdef DEBUG
  std::cout << "pushSamples n_minibatches: " << n_minibatches << std::endl;
  auto start = get_time_us();
#endif
  // save s3 object into list of string
  list_strings[str_version] = oss->str();
  delete oss;
#ifdef DEBUG
  uint64_t elapsed_us = (get_time_us() - start);
  std::cout << "oss->str() time (us): " << elapsed_us << std::endl;
#endif

  auto str_iter = list_strings.find(str_version);
  printProgress(str_iter->second);
  // create a pointer to each minibatch within s3 object and push it

  const void* s3_data = reinterpret_cast<const void*>(str_iter->second.c_str());
  int s3_obj_size = load_value<int>(s3_data);
  int num_samples = load_value<int>(s3_data);
  (void)s3_obj_size;
  (void)num_samples;
#ifdef DEBUG
  std::cout << "pushSamples s3_obj_size: " << s3_obj_size
            << " num_samples: " << num_samples << std::endl;
  assert(s3_obj_size > 0 && s3_obj_size < 100 * 1024 * 1024);
  assert(num_samples > 0 && num_samples < 1000000);
#endif
  auto new_queue = new std::queue<std::pair<const void*, int>>;
  for (uint64_t i = 0; i < n_minibatches; ++i) {
    // if it's the last minibatch in object we mark it so it can be deleted
    int is_last = ((i + 1) == n_minibatches) ? str_version : -1;

    new_queue->push(std::make_pair(s3_data, is_last));
  
    // advance ptr sample by sample
    for (uint64_t j = 0; j < minibatch_rows; ++j) {
      if (use_label) {
        FEATURE_TYPE label = load_value<FEATURE_TYPE>(s3_data); // read label
        assert(label == 0.0 || label == 1.0);
      }
      int num_values = load_value<int>(s3_data); 
#ifdef DEBUG
      //std::cout << "num_values: " << num_values << std::endl;
#endif
      assert(num_values >= 0 && num_values < 1000000);
    
      // advance until the next minibatch
      // every sample has index and value
      advance_ptr(s3_data, num_values * (sizeof(int) + sizeof(FEATURE_TYPE)));
    }
  }
  ring_lock.lock();
  minibatches_list.add(new_queue);
  ring_lock.unlock();
  for (uint64_t i = 0; i < n_minibatches; ++i) {
    num_minibatches_ready++;
    sem_post(&semaphore);
  }
  str_version++;
}

uint64_t S3SparseIterator::getObjId(uint64_t left, uint64_t right) {
  if (random_access) {
    //std::random_device rd;
    //auto seed = rd();
    //std::default_random_engine re2(seed);

    std::uniform_int_distribution<int> sampler(left, right - 1);
    uint64_t sampled = sampler(re);
    //uint64_t sampled = rand() % right;
    std::cout << "Sampled : " << sampled << " worker_id: " << worker_id << " left: " << left << " right: " << right << std::endl;
    return sampled;
  } else {
    auto ret = current++;
    if (current == right_id)
      current = left_id;
    return ret;
  }
}

/**
  * This function is used to compute the amount of S3 data consumed
  * per second. This might not be the same as the available S3
  * bandwidth if the system is bottleneck somewhere else
  */
void S3SparseIterator::printProgress(const std::string& s3_obj) {
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

static int sstreamSize(std::ostringstream& ss) {
  return ss.tellp();
}

void S3SparseIterator::threadFunction(const Configuration& config) {
  std::cout << "Building S3 deser. with size: "
    << std::endl;

  uint64_t count = 0;
  while (1) {
    // if we can go it means there is a slot
    // in the ring
    std::cout << "Waiting for pref_sem" << std::endl;
    pref_sem.wait();

    std::string obj_id_str = std::to_string(getObjId(left_id, right_id));

    std::ostringstream* s3_obj;
try_start:
    try {
      std::cout << "S3SparseIterator: getting object " << obj_id_str << std::endl;
      uint64_t start = get_time_us();
      s3_obj = s3_client->s3_get_object_ptr(obj_id_str, config.get_s3_bucket());
      uint64_t elapsed_us = (get_time_us() - start);
      double mb_s =
          sstreamSize(*s3_obj) / elapsed_us * 1000.0 * 1000 / 1024 / 1024;
      std::cout << "received s3 obj"
                << " elapsed: " << elapsed_us
                << " size: " << sstreamSize(*s3_obj) << " BW (MB/s): " << mb_s
                << "\n";
      //pm.increment_batches(); // increment number of batches we have processed

#ifdef DEBUG
      //double MBps = (1.0 * (32812.5*1024.0) / elapsed_us) / 1024 / 1024 * 1000 * 1000;
      //std::cout << "Get s3 obj took (us): " << (elapsed_us)
      //  << " size (KB): " << 32812.5
      //  << " bandwidth (MB/s): " << MBps
      //  << std::endl;
#endif
    } catch(...) {
      std::cout << "S3SparseIterator: error in s3GetObject"
                << " obj_id_str: " << obj_id_str << std::endl;
      goto try_start;
    }
    
    uint64_t num_passes = (count / (right_id - left_id));
    if (LIMIT_NUMBER_PASSES > 0 && num_passes == LIMIT_NUMBER_PASSES) {
      exit(0);
    }

    //auto start = get_time_us();
    pushSamples(s3_obj);
    //auto elapsed_us = (get_time_us() - start);
    //std::cout << "pushing took (us): " << elapsed_us << " at (us) " << get_time_us() << std::endl;
  }
}

} // namespace cirrus

