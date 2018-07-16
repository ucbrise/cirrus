#include <Tasks.h>

#include "Serializers.h"
#include "config.h"
#include "S3SparseIterator.h"
#include "Utils.h"
#include "SparseLRModel.h"
#include "PSSparseServerInterface.h"
#include "Configuration.h"

#define DEBUG
#define ERROR_INTERVAL_USEC (100000)  // time between error checks

namespace cirrus {

std::unique_ptr<CirrusModel> get_model(const Configuration& config,
        const std::string& ps_ip, uint64_t ps_port) {
  static PSSparseServerInterface* psi;
  static bool first_time = true;
  if (first_time) {
    first_time = false;
    psi = new PSSparseServerInterface(ps_ip, ps_port);

    while (true) {
      try {
        psi->connect();
        break;
      } catch (const std::exception& exc) {
        std::cout << exc.what();
      }
    }
  }

  bool use_col_filtering =
    config.get_model_type() == Configuration::COLLABORATIVE_FILTERING;
  return psi->get_full_model(use_col_filtering);
}

void ErrorSparseTask::run(const Configuration& config) {
  std::cout << "Compute error task connecting to store" << std::endl;

  std::cout << "Creating sequential S3Iterator" << std::endl;

  uint32_t left, right;
  if (config.get_model_type() == Configuration::LOGISTICREGRESSION) {
    left = config.get_test_range().first;
    right = config.get_test_range().second;
  } else if (config.get_model_type() == Configuration::COLLABORATIVE_FILTERING) {
    left = config.get_train_range().first;
    right = config.get_train_range().second;
  } else {
    exit(-1);
  }

  S3SparseIterator s3_iter(left, right, config,
      config.get_s3_size(), config.get_minibatch_size(),
      // use_label true for LR
      config.get_model_type() == Configuration::LOGISTICREGRESSION,
      0, false);

  // get data first
  // what we are going to use as a test set
  std::vector<SparseDataset> minibatches_vec;
  std::cout << "[ERROR_TASK] getting minibatches from "
    << config.get_train_range().first << " to "
    << config.get_train_range().second
    << std::endl;

  uint32_t minibatches_per_s3_obj =
    config.get_s3_size() / config.get_minibatch_size();
  for (uint64_t i = 0; i < (right - left) * minibatches_per_s3_obj; ++i) {
    const void* minibatch_data = s3_iter.get_next_fast();
    SparseDataset ds(reinterpret_cast<const char*>(minibatch_data),
        config.get_minibatch_size(),
        config.get_model_type() == Configuration::LOGISTICREGRESSION);
    minibatches_vec.push_back(ds);
  }

  std::cout << "[ERROR_TASK] Got "
    << minibatches_vec.size() << " minibatches"
    << "\n";
  std::cout << "[ERROR_TASK] Building dataset"
    << "\n";

  wait_for_start(ERROR_SPARSE_TASK_RANK, nworkers);
  uint64_t start_time = get_time_us();

  std::cout << "[ERROR_TASK] Computing accuracies"
    << "\n";
  while (1) {
    usleep(ERROR_INTERVAL_USEC);

    try {
      // first we get the model
#ifdef DEBUG
      std::cout << "[ERROR_TASK] getting the full model"
        << "\n";
#endif
      std::unique_ptr<CirrusModel> model = get_model(config, ps_ip, ps_port);

#ifdef DEBUG
      std::cout << "[ERROR_TASK] received the model" << std::endl;
#endif

      std::cout
        << "[ERROR_TASK] computing loss."
        << std::endl;
      FEATURE_TYPE total_loss = 0;
      FEATURE_TYPE total_accuracy = 0;
      uint64_t total_num_samples = 0;
      uint64_t total_num_features = 0;
      uint64_t start_index = 0;
      for (auto& ds : minibatches_vec) {
        std::pair<FEATURE_TYPE, FEATURE_TYPE> ret =
          model->calc_loss(ds, start_index);
        total_loss += ret.first;
        total_accuracy += ret.second;
        total_num_samples += ds.num_samples();
        total_num_features += ds.num_features();
        start_index += config.get_minibatch_size();
      }

      if (config.get_model_type() == Configuration::LOGISTICREGRESSION) {
        std::cout
          << "[ERROR_TASK] Loss (Total/Avg): " << total_loss
          << "/" << (total_loss / total_num_samples)
          << " Accuracy: " << (total_accuracy / minibatches_vec.size())
          << " time(us): " << get_time_us()
          << " time from start (sec): "
          << (get_time_us() - start_time) / 1000000.0
          << std::endl;
      } else if (config.get_model_type() == Configuration::COLLABORATIVE_FILTERING) {
        std::cout
          << "[ERROR_TASK] RMSE (Total): "
          << std::sqrt(total_loss / total_num_features)
          << " time(us): " << get_time_us()
          << " time from start (sec): "
          << (get_time_us() - start_time) / 1000000.0
          << std::endl;
      }
    } catch(...) {
      std::cout << "run_compute_error_task unknown id" << std::endl;
    }
  }
}

} // namespace cirrus

