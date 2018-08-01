#ifndef _S3_SPARSEITERATOR_H_
#define _S3_SPARSEITERATOR_H_

#include <CircularBuffer.h>
#include <Configuration.h>
#include <S3Client.h>
#include <S3Iterator.h>
#include <Serializers.h>
#include <Synchronization.h>
#include <config.h>

#include <thread>
#include <list>
#include <mutex>
#include <queue>
#include <semaphore.h>

namespace cirrus {

class S3SparseIterator : public S3Iterator {
 public:
    S3SparseIterator(
        uint64_t left_id, uint64_t right_id,
        const Configuration& c,
        uint64_t s3_rows,
        uint64_t minibatch_rows,
        bool use_label = true,
        int worker_id = 0,
        bool random_access = true);

    const void* get_next_fast();

    void thread_function(const Configuration&);

 private:
  void push_samples(std::ostringstream* oss);
  void print_progress(const std::string& s3_obj);
  uint64_t get_obj_id(uint64_t left, uint64_t right);

  uint64_t left_id;
  uint64_t right_id;

  std::shared_ptr<S3Client> s3_client;

  std::list<std::shared_ptr<FEATURE_TYPE>> ring;

  uint64_t read_ahead = 1;

  std::thread* thread;   //< background thread
  std::mutex ring_lock;  //< used to synchronize access
  // used to control number of blocks to prefetch
  PosixSemaphore pref_sem; //<

  uint64_t s3_rows;
  uint64_t minibatch_rows;

  //ProgressMonitor pm;

  sem_t semaphore;
  int str_version = 0;
  std::map<int, std::string> list_strings; // strings from s3

  // this contains a pointer to memory where a minibatch can be found
  // the int tells whether this is the last minibatch of a block of memory
  CircularBuffer<std::queue<std::pair<const void*, int>>*> minibatches_list;
  std::atomic<int> num_minibatches_ready{0};
  
  int to_delete = -1;
  bool use_label; // whether the dataset has labels or not
  int worker_id = 0;
  
  std::default_random_engine re;
  bool random_access = true;
  uint64_t current = 0;

};

}

#endif  // _S3_SPARSEITERATOR_H_
