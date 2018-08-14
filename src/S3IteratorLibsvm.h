#ifndef _S3_ITERATORTEXT_H_
#define _S3_ITERATORTEXT_H_

#include <CircularBuffer.h>
#include <Configuration.h>
#include <S3Client.h>
#include <S3Iterator.h>
#include <Serializers.h>
#include <SparseDataset.h>
#include <Synchronization.h>
#include <config.h>

#include <semaphore.h>
#include <list>
#include <mutex>
#include <queue>
#include <thread>

namespace cirrus {

class S3IteratorLibsvm : public S3Iterator {
 public:
  S3IteratorLibsvm(
      const Configuration& c,
      const std::string& s3_bucket,
      const std::string& s3_key,
      uint64_t file_size,       // FIXME we should calculate this automatically
      uint64_t minibatch_rows,  // number of samples in a minibatch
      int worker_id,            // id of this worker
      bool random_access,       // whether to access samples in a rand. fashion
      bool has_labels = true);

  std::shared_ptr<SparseDataset> getNext() override;

 private:
  void threadFunction(const Configuration&);
  void reportBandwidth(uint64_t elapsed, uint64_t size);
  void pushSamples(std::shared_ptr<std::ostringstream> oss);

  template <class T>
  T readNum(uint64_t& index, std::string& data);

  std::vector<std::shared_ptr<SparseDataset>> parseObjLibsvm(std::string& data);

  bool buildDatasetLibsvm(std::string& data,
                          uint64_t& index,
                          std::shared_ptr<SparseDataset>& minibatch);
  bool buildDatasetCsv(const std::string& data,
                       uint64_t index,
                       std::shared_ptr<SparseDataset>& minibatch);
  bool buildDatasetVowpalWabbit(const std::string& data,
                                uint64_t index,
                                std::shared_ptr<SparseDataset>& minibatch);

  std::pair<uint64_t, uint64_t> getFileRange(uint64_t);

  void readUntilNewline(uint64_t* index, const std::string& data);

  bool ignoreSpacesNotNewline(uint64_t& index, const std::string& data);
  bool ignoreSpaces(uint64_t& index, const std::string& data);

  /**
   * Attributes
   */
  std::string s3_bucket;
  std::string s3_key;

  uint64_t file_size = 0;

  std::shared_ptr<S3Client> s3_client;

  uint64_t read_ahead = 1;

  std::thread* thread;   //< background thread
  std::mutex ring_lock;  //< used to synchronize access
  // used to control number of blocks to prefetch
  PosixSemaphore pref_sem;  //<

  // uint64_t s3_rows;
  uint64_t minibatch_rows;

  sem_t semaphore;
  // this contains a pointer to memory where a minibatch can be found
  // the int tells whether this is the last minibatch of a block of memory
  CircularBuffer<std::vector<std::shared_ptr<SparseDataset>>> minibatches_list;

  // how many minibatches ready to be processed
  std::atomic<int> num_minibatches_ready{0};

  int worker_id = 0;

  std::default_random_engine re;
  bool random_access = true;
  uint64_t cur_index = 0;
};

}  // namespace cirrus

#endif  // _S3_ITERATORTEXT_H_
