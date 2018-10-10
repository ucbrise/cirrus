#include <Configuration.h>
#include <S3.h>
#include <Tasks.h>
#include <Utils.h>
#include <config.h>

#include <stdlib.h>
#include <cstdint>
#include <string>

#include <S3.h>
#include <gflags/gflags.h>

DEFINE_int64(nworkers, -1, "number of workers");
DEFINE_int64(rank, -1, "rank");
DEFINE_string(config, "", "config");

DEFINE_string(ps_ip, PS_IP, "parameter server ips comma separated");
DEFINE_string(ps_port, "1337", "parameter server ports comma separated");

static const uint64_t GB = (1024*1024*1024);
static const uint32_t SIZE = 1;

void run_tasks(int rank,
               int nworkers,
               int batch_size,
               const cirrus::Configuration& config,
               std::vector<std::string> ps_ips,
               std::vector<uint64_t> ps_ports) {
  std::cout << "Run tasks rank: " << rank << std::endl;
  int features_per_sample = config.get_num_features();
  int samples_per_batch = config.get_minibatch_size();


  if (rank == PS_SPARSE_SERVER_TASK_RANK) {
    cirrus::PSSparseServerTask st((1 << config.get_model_bits()) + 1,
        batch_size, samples_per_batch, features_per_sample,
        nworkers, rank, ps_ip, ps_port);
    st.run(config);
  } else if (rank >= WORKERS_BASE && rank < WORKERS_BASE + nworkers) {
    /**
     * Worker tasks run here
     * Number of tasks is determined by the value of nworkers
     */
    if (config.get_model_type() == cirrus::Configuration::LOGISTICREGRESSION) {
      if (ps_ips.size() > 1) {
        cirrus::LogisticSparseTaskS3 lt(features_per_sample, batch_size,
                                        samples_per_batch, features_per_sample,
                                        nworkers, rank, ps_ips, ps_ports);
        lt.run(config, rank - WORKERS_BASE);
      } else {
        cirrus::LogisticSparseTaskS3 lt(features_per_sample, batch_size,
                                        samples_per_batch, features_per_sample,
                                        nworkers, rank, ps_ips[0], ps_ports[0]);
        lt.run(config, rank - WORKERS_BASE);
      }
    } else if (config.get_model_type()
            == cirrus::Configuration::COLLABORATIVE_FILTERING) {
      cirrus::MFNetflixTask lt(0, batch_size, samples_per_batch,
                               features_per_sample, nworkers, rank, ps_ips[0],
                               ps_ports[0]);
      lt.run(config, rank - WORKERS_BASE);
    } else {
      exit(-1);
    }
  /**
    * SPARSE tasks
    */
  } else if (rank == ERROR_SPARSE_TASK_RANK) {
    if (ps_ips.size() > 1) {
      cirrus::ErrorSparseTask et((1 << config.get_model_bits()), batch_size,
                                 samples_per_batch, features_per_sample,
                                 nworkers, rank, ps_ips, ps_ports);
      et.run(config);
      cirrus::sleep_forever();
    } else {
      cirrus::ErrorSparseTask et((1 << config.get_model_bits()), batch_size,
                                 samples_per_batch, features_per_sample,
                                 nworkers, rank, ps_ips[0], ps_ports[0]);
      et.run(config);
      cirrus::sleep_forever();
    }

  } else if (rank == LOADING_SPARSE_TASK_RANK) {
    if (config.get_model_type() == cirrus::Configuration::LOGISTICREGRESSION) {
      cirrus::LoadingSparseTaskS3 lt((1 << config.get_model_bits()), batch_size,
                                     samples_per_batch, features_per_sample,
                                     nworkers, rank, ps_ips[0], ps_ports[0]);
      lt.run(config);

    } else if (config.get_model_type() ==
            cirrus::Configuration::COLLABORATIVE_FILTERING) {
      cirrus::LoadingNetflixTask lt(0, batch_size, samples_per_batch,
                                    features_per_sample, nworkers, rank,
                                    ps_ips[0], ps_ports[0]);
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
  std::cout << "./parameter_server --config config_file "
      << "--nworkers nworkers --rank rank [--ps_ip ps_ip] [--ps_port ps_port]"
      << std::endl
      << " RANKS:" << std::endl
      << "0: load task" << std::endl
      << "1: parameter server" << std::endl
      << "2: error task" << std::endl
      << "3: worker task" << std::endl
      << std::endl;
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

void check_arguments() {
  if (FLAGS_nworkers == -1 || FLAGS_rank == -1 || FLAGS_config == "") {
    print_arguments();
    throw std::runtime_error("Some flags not specified");
  }
}

int main(int argc, char** argv) {
  std::cout << "Starting parameter server" << std::endl;

  print_hostname();

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  check_arguments();

  int nworkers = FLAGS_nworkers;
  std::cout << "Running parameter server with: "
    << nworkers << " workers"
    << std::endl;

  int rank = FLAGS_rank;
  std::cout << "Running parameter server with: "
    << rank << " rank"
    << std::endl;

  std::string ps_ip_string = FLAGS_ps_ip;
  std::vector<std::string> ps_ips;
  std::string tmp;
  std::stringstream ss(ps_ip_string);
  while (ss) {
    if (!getline(ss, tmp, ','))
      break;
    ps_ips.push_back(tmp);
  }

  std::string ps_port_string = FLAGS_ps_port;
  std::vector<uint64_t> ps_ports;
  std::stringstream ss1(ps_port_string);
  while (ss1) {
    if (!getline(ss1, tmp, ','))
      break;
    std::istringstream iss(tmp);
    uint64_t tmp_port;
    iss >> tmp_port;
    ps_ports.push_back(tmp_port);
  }

  assert(ps_ips.size() == ps_ports.size());
  std::cout << "Number of parameter servers: " << ps_ips.size() << std::endl;
  std::cout << "Parameter servers: ";
  for (int i = 0; i < ps_ips.size(); i++) {
    std::cout << ps_ips[i] << ":" << ps_ports[i] << std::endl;
  }

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

  cirrus::s3_initialize_aws();
  run_tasks(rank, nworkers, batch_size, config, FLAGS_ps_ip, FLAGS_ps_port);


  std::cout << "Test successful" << std::endl;

  return 0;
}

