#ifndef _S3_ITERATORTEXT_H_
#define _S3_ITERATORTEXT_H_

#include <S3.h>
#include <S3Iterator.h>
#include <Configuration.h>
#include <config.h>
#include <SparseDataset.h>
#include <Synchronization.h>
#include <Serializers.h>
#include <CircularBuffer.h>

#include <thread>
#include <list>
#include <mutex>
#include <queue>
#include <semaphore.h>

namespace cirrus {

class S3IteratorText : public S3Iterator {
 public:
    S3IteratorText(
        const Configuration& c,
        uint64_t file_size,
        uint64_t minibatch_rows, // number of samples in a minibatch
        bool use_label,          // whether each sample has a label
        int worker_id,           // id of this worker
        bool random_access);     // whether to access samples in a rand. fashion

    const void* get_next_fast();

    void thread_function(const Configuration&);

 private:
  void report_bandwidth(uint64_t elapsed, uint64_t size);
  void push_samples(std::ostringstream* oss);

  bool build_dataset(
    const std::string& data, uint64_t index,
    std::shared_ptr<SparseDataset>& minibatch);

  std::pair<uint64_t, uint64_t> get_file_range(uint64_t);

  uint64_t file_size = 0;

  std::shared_ptr<Aws::S3::S3Client> s3_client;

  uint64_t cur;
  std::list<std::shared_ptr<FEATURE_TYPE>> ring;

  uint64_t read_ahead = 1;

  std::thread* thread;   //< background thread
  std::mutex ring_lock;  //< used to synchronize access
  // used to control number of blocks to prefetch
  PosixSemaphore pref_sem; //<

  uint64_t s3_rows;
  uint64_t minibatch_rows;

  sem_t semaphore;
  // this contains a pointer to memory where a minibatch can be found
  // the int tells whether this is the last minibatch of a block of memory
  CircularBuffer<
    std::vector<std::shared_ptr<SparseDataset>>> minibatches_list;
  std::atomic<int> num_minibatches_ready{0};
  
  bool use_label; // whether the dataset has labels or not
  int worker_id = 0;
  
  std::default_random_engine re;
  bool random_access = true;
  uint64_t current = 0;
};

}

#endif  // _S3_ITERATORTEXT_H_
