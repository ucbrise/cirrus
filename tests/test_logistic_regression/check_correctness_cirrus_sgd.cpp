#include <unistd.h>
#include <cstdlib>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>

#include <InputReader.h>
#include <LRModel.h>

const std::string INPUT_PATH = "criteo_data/day_1_100k_filtered";

void print_info(const auto& samples) {
  std::cout << "Number of samples: " << samples.size() << std::endl;
  std::cout << "Number of cols: " << samples[0].size() << std::endl;
}

void check_error(auto model, auto dataset) {
  auto ret = model->calc_loss(dataset);
  auto loss = ret.first;
  auto avg_loss = loss / dataset.num_samples();
  std::cout << "total loss: " << loss
    << " avg loss: " << avg_loss
    << std::endl;
}

std::mutex model_lock;
std::unique_ptr<cirrus::LRModel> model;
double epsilon = 0.00001;
double learning_rate = 0.00000001;

void learning_function(const cirrus::Dataset& dataset) {
  for (uint64_t i = 0; 1; ++i) {
    cirrus::Dataset ds = dataset.random_sample(20);

    auto gradient = model->minibatch_grad(ds.samples_,
        const_cast<FEATURE_TYPE*>(ds.labels_.get()),
        ds.num_samples(), epsilon);

    model_lock.lock();
    model->sgd_update(learning_rate, gradient.get());
    model_lock.unlock();
  }
}

int main() {
  cirrus::InputReader input;
  cirrus::Dataset dataset = input.read_input_csv(INPUT_PATH, "\t", 1, 10000, 14,
                                                 true);  // normalize=true
  dataset.check();
  dataset.print_info();

  uint64_t num_cols = 13;
  model.reset(new cirrus::LRModel(num_cols));

  uint64_t num_threads = 20;
  std::vector<std::shared_ptr<std::thread>> threads;
  for (uint64_t i = 0; i < num_threads; ++i) {
    threads.push_back(std::make_shared<std::thread>(
          learning_function, dataset));
  }

  while (1) {
    usleep(100000); // 100ms
    model_lock.lock();
    check_error(model.get(), dataset);
    model_lock.unlock();
  }


  return 0;
}
