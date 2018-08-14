#include <unistd.h>
#include <cstdlib>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>

#include <Configuration.h>
#include <InputReader.h>
#include <S3SparseIterator.h>
#include <SparseLRModel.h>

#include <Utils.h>
#include <config.h>

const std::string INPUT_PATH = "criteo_data/train.csv_100K_sparse";

void print_info(const auto& samples) {
  std::cout << "Number of samples: " << samples.size() << std::endl;
  std::cout << "Number of cols: " << samples[0].size() << std::endl;
}

void check_error(auto model, auto dataset) {
  auto ret = model->calc_loss(dataset, 0);
  auto total_loss = ret.first;
  auto avg_loss = 1.0 * total_loss / dataset.num_samples();
  auto acc = ret.second;
  std::cout << "time: " << cirrus::get_time_us()
            << " total/avg loss: " << total_loss << "/" << avg_loss
            << " accuracy: " << acc << std::endl;
}

std::mutex model_lock;
std::unique_ptr<cirrus::SparseLRModel> model;
double epsilon = 0.00001;
double learning_rate = 0.001;

void learning_function_once(const cirrus::SparseDataset& dataset,
                            const cirrus::Configuration& conf) {
  cirrus::SparseDataset ds = dataset.random_sample(20);

  auto gradient = model->minibatch_grad(ds, conf);

  model_lock.lock();
  model->sgd_update(learning_rate, gradient.get());
  model_lock.unlock();
}

void learning_function(const cirrus::SparseDataset& dataset,
                       const cirrus::Configuration& conf) {
  for (uint64_t i = 0; 1; ++i) {
    learning_function_once(dataset, conf);
  }
}

#if 0
std::mutex s3_lock;;
void learning_function_from_s3(const cirrus::SparseDataset& dataset, cirrus::S3SparseIterator* s3_iter) {

  for (uint64_t i = 0; 1; ++i) {
    s3_lock.lock();
    const void* data = s3_iter->getNext();
    s3_lock.unlock();
    cirrus::SparseDataset ds(reinterpret_cast<const char*>(data),
        config.get_minibatch_size()); // construct dataset with data from s3

    auto gradient = model->minibatch_grad(ds, conf);

    model_lock.lock();
    model->sgd_update(learning_rate, gradient.get());
    model_lock.unlock();
  }
}
#endif

int main() {
  cirrus::InputReader input;
  cirrus::Configuration config;
  config.s3_size = 50000;
  config.minibatch_size = 20;
  config.load_input_type = "csv";
  config.model_bits = 19;
  config.normalize = 1;
  config.train_set_range = std::make_pair(0, 824);
  config.test_set_range = std::make_pair(825, 840);
  config.use_bias = 1;
  config.limit_samples = 1000000;
  cirrus::SparseDataset dataset =
      input.read_input_criteo_kaggle_sparse(INPUT_PATH, ",", config);
  dataset.check();
  dataset.print_info();

  // cirrus::S3SparseIterator* s3_iter = new cirrus::S3SparseIterator(0, 10,
  // config,
  //    config.get_s3_size(),
  //    config.get_minibatch_size());

  uint64_t model_size = (1 << config.get_model_bits());
  model.reset(new cirrus::SparseLRModel(model_size));

  uint64_t num_threads = 1;
  std::vector<std::shared_ptr<std::thread>> threads;
  for (uint64_t i = 0; i < num_threads; ++i) {
    threads.push_back(
        std::make_shared<std::thread>(learning_function, dataset, config));
  }
  
  while (1) {
    usleep(100000); // 100ms
    model_lock.lock();
    check_error(model.get(), dataset);
    model_lock.unlock();
  }
  return 0;
}

