#include <Tasks.h>

#include "Serializers.h"
#include "Utils.h"
#include "S3SparseIterator.h"
#include "PSSparseServerInterface.h"
#include "SparseMFModel.h"

#include <pthread.h>

#define DEBUG

namespace cirrus {

void MFNetflixTask::push_gradient(MFSparseGradient& mfg) {
#ifdef DEBUG
  auto before_push_us = get_time_us();
  std::cout << "Publishing gradients" << std::endl;
#endif
  psint->send_mf_gradient(mfg);
#ifdef DEBUG
  std::cout << "Published gradients!" << std::endl;
  auto elapsed_push_us = get_time_us() - before_push_us;
  static uint64_t before = 0;
  if (before == 0)
    before = get_time_us();
  auto now = get_time_us();
  std::cout << "[WORKER] "
      << "Worker task published gradient"
      << " at time (us): " << get_time_us()
      << " took(us): " << elapsed_push_us
      << " bw(MB/s): " << std::fixed <<
         (1.0 * mfg.getSerializedSize() / elapsed_push_us / 1024 / 1024 * 1000 * 1000)
      << " since last(us): " << (now - before)
      << "\n";
  before = now;
#endif
}

// get samples and labels data
bool MFNetflixTask::get_dataset_minibatch(
    std::shared_ptr<SparseDataset>& dataset,
    S3SparseIterator& s3_iter) {
#ifdef DEBUG
  auto start = get_time_us();
#endif

  dataset = s3_iter.getNext();
#ifdef DEBUG
  auto finish1 = get_time_us();
#endif

#ifdef DEBUG
  auto finish2 = get_time_us();
  double bw = 1.0 * dataset->getSizeBytes() /
    (finish2-start) * 1000.0 * 1000 / 1024 / 1024;
  std::cout << "[WORKER] Get Sample Elapsed (S3) "
    << " minibatch size: " << config.get_minibatch_size()
    << " part1(us): " << (finish1 - start)
    << " part2(us): " << (finish2 - finish1)
    << " BW (MB/s): " << bw
    << " at time: " << get_time_us()
    << "\n";
#endif
  return true;
}

void MFNetflixTask::run(const Configuration& config, int worker) {
  std::cout << "Starting MFNetflixTask"
    << std::endl;
  uint64_t num_s3_batches = config.get_limit_samples() / config.get_s3_size();
  this->config = config;

  psint = std::make_unique<PSSparseServerInterface>(ps_ip, ps_port);
  psint->connect();

  mf_model_get = std::make_unique<MFModelGet>(ps_ip, ps_port);

  std::cout << "[WORKER] " << "num s3 batches: " << num_s3_batches
    << std::endl;
  wait_for_start(WORKER_SPARSE_TASK_RANK + worker, nworkers);

  // Create iterator that goes from 0 to num_s3_batches
  std::pair<int, int> train_range = config.get_train_range();

  /** We sequentially iterate over data
    * This is necessary because we need to know the index of each row
    * in the dataset matrix because that tells us which user it belongs to
    * (same doesn't happen with Logistic Regression)
    */

  int l = train_range.first;
  int r = train_range.second;
  uint64_t sample_low = 0;
  uint64_t sample_index = 0;
  uint64_t sample_high = config.get_s3_size() * (config.get_train_range().second + 1);

  if (config.get_netflix_workers()) {
    int range_length = (train_range.second - train_range.first) / config.get_netflix_workers();
    range_length += 1;

    l = worker * range_length;
    r = std::min(l + range_length, r);

    sample_low = l * config.get_s3_size();
    sample_high = std::min(sample_high, (r + 1) * config.get_s3_size());

    sample_index = sample_low;

  }

  S3SparseIterator s3_iter(l, r + 1, config, config.get_s3_size(),
                           config.get_minibatch_size(), false, worker, false,
                           false);

  std::cout << "[WORKER] starting loop" << std::endl;

  while (1) {
    // get data, labels and model
#ifdef DEBUG
    std::cout << "[WORKER] running phase 1" << std::endl;
#endif
    std::shared_ptr<SparseDataset> dataset;
    if (!get_dataset_minibatch(dataset, s3_iter)) {
      continue;
    }
    std::cout << "DS size: " << dataset->num_samples() << std::endl;
#ifdef DEBUG
    std::cout << "[WORKER] phase 1 done" << std::endl;
    dataset->check();
    dataset->print_info();
    auto now = get_time_us();
#endif
    // compute mini batch gradient
    std::unique_ptr<ModelGradient> gradient;

    // we get the model subset with just the right amount of weights
    SparseMFModel model =
      mf_model_get->get_new_model(
              *dataset, sample_index, config.get_minibatch_size());

#ifdef DEBUG
    std::cout << "get model elapsed(us): " << get_time_us() - now << std::endl;
    std::cout << "Checking model" << std::endl;
    std::cout << "Computing gradient" << "\n";
    now = get_time_us();
#endif

    try {
      auto gradient = model.minibatch_grad(*dataset, config, sample_index);
#ifdef DEBUG
      auto elapsed_us = get_time_us() - now;
      std::cout << "[WORKER] Gradient compute time (us): " << elapsed_us
        << " at time: " << get_time_us() << "\n";
#endif
      MFSparseGradient* grad_ptr =
        dynamic_cast<MFSparseGradient*>(gradient.get());
      push_gradient(*grad_ptr);
      sample_index += config.get_minibatch_size();


      if (sample_index + config.get_minibatch_size() > sample_high) {
          sample_index = sample_low;
      }
    } catch(...) {
      std::cout << "There was an error computing the gradient" << std::endl;
      exit(-1);
    }
  }
}

} // namespace cirrus
