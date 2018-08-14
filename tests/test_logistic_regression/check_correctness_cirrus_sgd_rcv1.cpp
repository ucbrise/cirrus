#include <unistd.h>
#include <cstdlib>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>

#include <InputReader.h>
#include <SparseLRModel.h>
#include <S3SparseIterator.h>

#include <Utils.h>
#include <config.h>

//typedef float FEATURE_TYPE;
const std::string INPUT_PATH = "../../data/rcv1/train.vw";

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
std::mutex s3_lock;;
std::unique_ptr<cirrus::SparseLRModel> model;
double epsilon = 0.00001;
double learning_rate = 0.00000001;

void learning_function(const cirrus::SparseDataset& dataset,
                       const cirrus::Configuration& conf) {
  for (uint64_t i = 0; 1; ++i) {
    //std::cout << "iter" << std::endl;
    cirrus::SparseDataset ds = dataset.random_sample(20);

    auto gradient = model->minibatch_grad(ds, conf);

    model_lock.lock();
    model->sgd_update(learning_rate, gradient.get());
    model_lock.unlock();
  }
}

cirrus::Configuration config;
cirrus::S3SparseIterator* s3_iter;
void learning_function_from_s3(const cirrus::SparseDataset& dataset) {
  for (uint64_t i = 0; 1; ++i) {
    s3_lock.lock();
    std::shared_ptr<cirrus::SparseDataset> ds = s3_iter->getNext();
    s3_lock.unlock();

    auto gradient = model->minibatch_grad(*ds, epsilon);

    model_lock.lock();
    model->sgd_update(learning_rate, gradient.get());
    model_lock.unlock();
  }
}

/**
  * Code not working
  */
int main() {
  cirrus::InputReader input;
  cirrus::SparseDataset dataset =
      input.read_input_rcv1_sparse(INPUT_PATH, " ", 100000,
                                   true);  // normalize=true
  dataset.check();
  dataset.print_info();

  uint64_t model_size = (1 << 19);
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

