#include <Tasks.h>

#include "Serializers.h"
#include "config.h"
#include "Utils.h"
#include "SparseLRModel.h"
#include "PSSparseServerInterface.h"
#include "Configuration.h"
#include "InputReader.h"

#define DEBUG
#define ERROR_INTERVAL_USEC (100000)  // time between error checks

using namespace cirrus;

Configuration config = Configuration("configs/test_config.cfg");

std::unique_ptr<CirrusModel> get_model(const Configuration& config,
                                       const std::string& ps_ip,
                                       uint64_t ps_port) {
  static PSSparseServerInterface* psi;
  static bool first_time = true;
  if (first_time) {
    first_time = false;
    psi = new PSSparseServerInterface(ps_ip, ps_port);
  }
  return psi->get_full_model(false);
}

int main() {
  // get data first
  // what we are going to use as a test set
  InputReader input;
  SparseDataset test_data = input.read_input_criteo_kaggle_sparse(
      "tests/test_data/test_lr.csv", ",", config);
  SparseLRModel model(1 << config.get_model_bits());

  uint64_t start_time = get_time_us();

  std::cout << "[ERROR_TASK] Computing accuracies"
            << "\n";
  FEATURE_TYPE avg_loss = 0;
  while(1) {
    usleep(ERROR_INTERVAL_USEC);
    try {
#ifdef DEBUG
      std::cout << "[ERROR_TASK] getting the full model"
                << "\n";
#endif
      std::unique_ptr<CirrusModel> model = get_model(config, "127.0.0.1", 1337);

#ifdef DEBUG
      std::cout << "[ERROR_TASK] received the model" << std::endl;
#endif

      std::cout << "[ERROR_TASK] computing loss." << std::endl;
      FEATURE_TYPE total_loss = 0;
      FEATURE_TYPE total_accuracy = 0;
      uint64_t total_num_samples = 0;
      uint64_t total_num_features = 0;
      uint64_t start_index = 0;
      std::pair<FEATURE_TYPE, FEATURE_TYPE> ret =
          model->calc_loss(test_data, start_index);
      total_loss += ret.first;
      total_accuracy += ret.second;
      total_num_samples += test_data.num_samples();
      total_num_features += test_data.num_features();
      start_index += config.get_minibatch_size();

      std::cout << "[ERROR_TASK] Loss (Total/Avg): " << total_loss << "/"
                << (total_loss / total_num_samples)
                << " Accuracy: " << (total_accuracy)
                << " time(us): " << get_time_us() << " time from start (sec): "
                << (get_time_us() - start_time) / 1000000.0 << std::endl;
      avg_loss = (total_loss / total_num_samples);
    } catch (...) {
      break;
    }
  }
  if (avg_loss < 0.1) {
    return 0;
  } else {
    throw std::runtime_error("Fail");
  }
}
