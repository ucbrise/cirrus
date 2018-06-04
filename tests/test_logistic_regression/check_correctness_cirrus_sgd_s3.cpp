#include <unistd.h>
#include <cstdlib>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>

#include "PSSparseServerInterface.h"
#include <InputReader.h>
#include <SparseLRModel.h>
#include "S3SparseIterator.h"
   

std::unique_ptr<S3SparseIterator> s3_iter;
std::mutex model_lock;
std::unique_ptr<SparseLRModel> model;
Configuration config;
double epsilon = 0.00001;
double learning_rate = 0.00000001;

SparseDataset read_dataset(
    const Configuration& config) {
  InputReader input;

  std::string delimiter;
  if (config.get_input_type() == "csv_space") {
    delimiter = "";
  } else if (config.get_input_type() == "csv_tab") {
    delimiter = "\t";
  } else if (config.get_input_type() == "csv") {
    delimiter = ",";
  } else {
    throw std::runtime_error("unknown input type");
  }

  // READ the kaggle criteo dataset
  return input.read_input_criteo_kaggle_sparse(
      config.get_input_path(),
      delimiter,
      config);
}

void print_info(const auto& samples) {
  std::cout << "Number of samples: " << samples.size() << std::endl;
  std::cout << "Number of cols: " << samples[0].size() << std::endl;
}

void check_error(auto model, auto dataset) {
  auto ret = model->calc_loss(dataset, 0);
  auto loss = ret.first;
  auto avg_loss = loss / dataset.num_samples();
  std::cout << "total loss: " << loss
    << " avg loss: " << avg_loss
    << std::endl;
}

bool get_dataset_minibatch(
    auto& dataset, auto& s3_iter) {
  const void* minibatch = s3_iter->get_next_fast();
  dataset.reset(new SparseDataset(reinterpret_cast<const char*>(minibatch),
        config.get_minibatch_size()));
  return true;
}

void learning_function() {
  while (1) {
    std::unique_ptr<SparseDataset> dataset;
    if (!get_dataset_minibatch(dataset, s3_iter)) {
      continue;
    }
    std::unique_ptr<ModelGradient> gradient;
    // we get the model subset with just the right amount of weights
    gradient = model->minibatch_grad(*dataset, epsilon);
    LRSparseGradient* lrg = dynamic_cast<LRSparseGradient*>(gradient.get());
    model->sgd_update(learning_rate, lrg);
  }
}

int main() {
  config.read("criteo_kaggle.cfg");
  uint64_t num_s3_batches = config.get_limit_samples() / config.get_s3_size();
  
  std::cout << "[WORKER] " << "num s3 batches: " << num_s3_batches
    << std::endl;

  // Create iterator that goes from 0 to num_s3_batches
  auto train_range = config.get_train_range();
  s3_iter = std::make_unique<S3SparseIterator>(
      train_range.first, train_range.second,
      config, config.get_s3_size(), config.get_minibatch_size(),
      true, 0, false); // make this sequential

  model.reset(new SparseLRModel( (1 << config.get_model_bits()) ));

  uint64_t num_threads = 1;
  std::vector<std::shared_ptr<std::thread>> threads;
  for (uint64_t i = 0; i < num_threads; ++i) {
    threads.push_back(std::make_shared<std::thread>(
          learning_function));
  }

  auto dataset = read_dataset(config);
  dataset.check();
  dataset.print_info();
  while (1) {
    usleep(100000); // 100ms
    model_lock.lock();
    check_error(model.get(), dataset);
    model_lock.unlock();
  }
  return 0;
}

