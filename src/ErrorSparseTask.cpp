#include <Tasks.h>
#include <thread>

#include "Serializers.h"
#include "config.h"
#include "S3SparseIterator.h"
#include "Utils.h"
#include "SparseLRModel.h"
#include "PSSparseServerInterface.h"
#include "Configuration.h"
#include "Constants.h"

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
  }

  bool use_col_filtering =
    config.get_model_type() == Configuration::COLLABORATIVE_FILTERING;
  return psi->get_full_model(use_col_filtering);
}

void ErrorSparseTask::error_response() {
  int fd;
  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    std::cout << "SOCKET FAILED"
              << std::endl;  // FIXME: Should throw an error instead
    return;
  }

  struct sockaddr_in serveraddr;
  serveraddr.sin_family = AF_INET;
  serveraddr.sin_addr.s_addr = INADDR_ANY;
  serveraddr.sin_port = htons(1338);
  std::memset(serveraddr.sin_zero, 0, sizeof(serveraddr.sin_zero));

  if (bind(fd, (struct sockaddr*) &serveraddr, sizeof(serveraddr)) < 0) {
    std::cout << "SOCKET FAILED 2"
              << std::endl;  // FIXME: Should throw an error instead
    return;
  }

  uint32_t operation;
  int length = 0;
  struct sockaddr_in remaddr;          /* remote address */
  socklen_t addrlen = sizeof(remaddr); /* length of addresses */

  std::cout << "Waiting on message" << std::endl;
  while (true) {
    length = recvfrom(fd, &operation, sizeof(uint32_t), 0,
                      (struct sockaddr*) &remaddr, &addrlen);
    if (length < 0) {
      std::cout << "Failed to read" << std::endl;
      continue;
    }
    printf("Received: %d bytes\n", length);
    std::cout << "Received: " << operation << std::endl;

    if (operation == GET_LAST_TIME_ERROR) {
      double time_error[2];
      time_error[0] = last_time;
      time_error[1] = last_error;

      sendto(fd, time_error, 2 * sizeof(double), 0, (struct sockaddr*) &remaddr,
             addrlen);
    }
  }
}

void ErrorSparseTask::run(const Configuration& config) {
  std::cout << "Creating error response thread" << std::endl;
  std::thread error_thread(std::bind(&ErrorSparseTask::error_response, this));

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

      last_time = (get_time_us() - start_time) / 1000000.0;
      if (config.get_model_type() == Configuration::LOGISTICREGRESSION) {
        last_error = (total_loss / total_num_samples);
        std::cout << "[ERROR_TASK] Loss (Total/Avg): " << total_loss << "/"
                  << last_error
                  << " Accuracy: " << (total_accuracy / minibatches_vec.size())
                  << " time(us): " << get_time_us()
                  << " time from start (sec): " << last_time << std::endl;
      } else if (config.get_model_type() == Configuration::COLLABORATIVE_FILTERING) {
        last_error = std::sqrt(total_loss / total_num_features);
        std::cout << "[ERROR_TASK] RMSE (Total): " << last_error
                  << " time(us): " << get_time_us()
                  << " time from start (sec): " << last_time << std::endl;
      }
    } catch(...) {
      std::cout << "run_compute_error_task unknown id" << std::endl;
    }
  }
}

} // namespace cirrus

