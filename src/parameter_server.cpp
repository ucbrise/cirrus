#include <stdlib.h>
#include <cstdint>
#include <string>
#include "Utils.h"
#include "Configuration.h"

#include <Tasks.h>

#include "config.h"

#include <gflags/gflags.h>

#define BILLION (1000000000ULL)
#define MILLION (1000000ULL)
#define SAMPLE_BASE   (0)
#define MODEL_BASE    (1 * BILLION)
#define GRADIENT_BASE (2 * BILLION)
#define LABEL_BASE    (3 * BILLION)
#define START_BASE    (4 * BILLION)

DEFINE_int64(nworkers, -1, "number of workers");
DEFINE_int64(rank, -1, "rank");
DEFINE_string(config, "", "config");

static const uint64_t GB = (1024*1024*1024);
static const uint32_t SIZE = 1;

void run_tasks(int rank, int nworkers, 
    int batch_size, const cirrus::Configuration& config) {

  std::cout << "Run tasks rank: " << rank << std::endl;
  int features_per_sample = config.get_num_features();
  int samples_per_batch = config.get_minibatch_size();

  if (rank == PERFORMANCE_LAMBDA_RANK) {
    cirrus::PerformanceLambdaTask lt(features_per_sample, MODEL_BASE,
        LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
        batch_size, samples_per_batch, features_per_sample,
        nworkers, rank);
    lt.run(config);
    cirrus::sleep_forever();
  } else if (rank == PS_SPARSE_SERVER_TASK_RANK) {
    cirrus::PSSparseServerTask st((1 << config.get_model_bits()) + 1, MODEL_BASE,
        LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
        batch_size, samples_per_batch, features_per_sample,
        nworkers, rank);
    st.run(config);
    //sleep_forever();
  } else if (rank >= WORKERS_BASE && rank < WORKERS_BASE + nworkers) {
    /**
     * Worker tasks run here
     * Number of tasks is determined by the value of nworkers
     */
    if (config.get_model_type() == cirrus::Configuration::LOGISTICREGRESSION) {
      cirrus::LogisticSparseTaskS3 lt(features_per_sample, MODEL_BASE,
          LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, rank);
      lt.run(config, rank - WORKERS_BASE);
    } else if(config.get_model_type() == cirrus::Configuration::COLLABORATIVE_FILTERING) {
      cirrus::MFNetflixTask lt(0, MODEL_BASE,
          LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, rank);
      lt.run(config, rank - WORKERS_BASE);
    } else {
      exit(-1);
    }
  /**
    * SPARSE tasks
    */
  } else if (rank == ERROR_SPARSE_TASK_RANK) {
    cirrus::ErrorSparseTask et((1 << config.get_model_bits()), MODEL_BASE,
        LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
        batch_size, samples_per_batch, features_per_sample,
        nworkers, rank);
    et.run(config);
    cirrus::sleep_forever();
  } else if (rank == LOADING_SPARSE_TASK_RANK) {
    if (config.get_model_type() == cirrus::Configuration::LOGISTICREGRESSION) {
      cirrus::LoadingSparseTaskS3 lt((1 << config.get_model_bits()), MODEL_BASE,
          LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, rank);
      lt.run(config);
    } else if(config.get_model_type() == cirrus::Configuration::COLLABORATIVE_FILTERING) {
      cirrus::LoadingNetflixTask lt(0, MODEL_BASE,
          LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, rank);
      lt.run(config);
    } else {
      exit(-1);
    }
  } else {
    throw std::runtime_error("Wrong task rank: " + std::to_string(rank));
  }
}

void print_arguments() {
  // nworkers is the number of processes computing gradients
  // rank starts at 0
  std::cout << "./parameter_server config_file nworkers rank" << std::endl;
}

cirrus::Configuration load_configuration(const std::string& config_path) {
  cirrus::Configuration config;
  std::cout << "Loading configuration"
    << std::endl;
  config.read(config_path);
  std::cout << "Configuration read"
    << std::endl;
  config.check();
  return config;
}

void print_hostname() {
  char name[200];
  gethostname(name, 200);
  std::cout << "MPI multi task test running on hostname: " << name
    << std::endl;
}

int main(int argc, char** argv) {
  std::cout << "Starting parameter server" << std::endl;

  if (argc != 4) {
    print_arguments();
    throw std::runtime_error("Wrong number of arguments");
  }

  print_hostname();

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if ((FLAGS_nworkers < 0) || (FLAGS_rank < 0) || (FLAGS_config == "")) {
      throw std::runtime_error("Some flags not specified");
  }

  int nworkers = FLAGS_nworkers;
  std::cout << "Running parameter server with: "
    << nworkers << " workers"
    << std::endl;

  int rank = FLAGS_rank;
  std::cout << "Running parameter server with: "
    << rank << " rank"
    << std::endl;

  auto config = load_configuration(FLAGS_config);
  config.print();

  // from config we get
  int batch_size = config.get_minibatch_size() * config.get_num_features();

  std::cout
    << "samples_per_batch: " << config.get_minibatch_size()
    << " features_per_sample: " << config.get_num_features()
    << " batch_size: " << config.get_minibatch_size()
    << std::endl;

  // call the right task for this process
  std::cout << "Running task" << std::endl;
  run_tasks(rank, nworkers, batch_size, config);

  std::cout << "Test successful" << std::endl;

  return 0;
}

