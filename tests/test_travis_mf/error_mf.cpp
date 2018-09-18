#include <Tasks.h>
#include <signal.h>
#include "Configuration.h"
#include "InputReader.h"
#include "PSSparseServerInterface.h"
#include "Serializers.h"
#include "SparseMFModel.h"
#include "Utils.h"
#include "config.h"

//#define DEBUG
#define ERROR_INTERVAL_USEC (100000)  // time between error checks

using namespace cirrus;

Configuration config = Configuration("configs/jester.cfg");
FEATURE_TYPE avg_loss = 0;

std::unique_ptr<CirrusModel> get_model(const Configuration& config,
                                       const std::string& ps_ip,
                                       uint64_t ps_port) {
  static PSSparseServerInterface* psi;
  static bool first_time = true;
  if (first_time) {
    first_time = false;
    psi = new PSSparseServerInterface(ps_ip, ps_port);
    psi->connect();
  }
  return psi->get_full_model(true);
}

void signal_callback_handler(int signum) {
  if (avg_loss < 0.6) {
    exit(EXIT_SUCCESS);
  } else {
    exit(EXIT_FAILURE);
  }
}

int main() {
  // get data first
  // what we are going to use as a test set
  // catch sigpipe
  signal(SIGPIPE, signal_callback_handler);
  InputReader input;
  int nusers, nitems;
  SparseDataset test_data = input.read_jester_ratings(
      "tests/test_data/jester_test.csv", &nusers, &nitems);
  int nfactors = 10;
  SparseMFModel model(nusers, nitems, nfactors);

  uint64_t start_time = get_time_us();

  for (int i = 0; i < 100; i++) {
    usleep(ERROR_INTERVAL_USEC);
    try {
#ifdef DEBUG
      std::cout << "[ERROR_TASK] getting the full model"
                << "\n";
#endif
      std::unique_ptr<CirrusModel> model = get_model(config, "127.0.0.1", 1338);

#ifdef DEBUG
      std::cout << "[ERROR_TASK] received the model" << std::endl;
#endif
      FEATURE_TYPE total_loss = 0;
      uint64_t total_num_samples = 0;
      uint64_t total_num_features = 0;
      uint64_t start_index = 0;
      uint64_t count = 0;
      std::pair<FEATURE_TYPE, FEATURE_TYPE> ret =
          model->calc_loss(test_data, start_index);
      total_loss += ret.first;
      count += ret.second;
      total_num_samples += test_data.num_samples();
      total_num_features += test_data.num_features();
      start_index += config.get_minibatch_size();
      avg_loss = std::sqrt(total_loss / count);
      std::cout << "[ERROR_TASK] Iteration: " << i << std::endl;
      std::cout << "[ERROR_TASK] Loss (Total/RMSE): " << total_loss << "/"
                << avg_loss << " time(us): " << get_time_us()
                << " time from start (sec): "
                << (get_time_us() - start_time) / 1000000.0 << std::endl;
      if (avg_loss < 0.6) {
        break;
      }
    } catch (...) {
      // Suppress for now.
      // throw std::runtime_error("Error");
    }
  }
  if (avg_loss < 0.6) {
    exit(EXIT_SUCCESS);
  } else {
    exit(EXIT_FAILURE);
  }
}
