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
#include <Configuration.h>
#include "SGD.h"

using namespace cirrus;

cirrus::Configuration config =
    cirrus::Configuration("configs/criteo_kaggle.cfg");

int main() {
  InputReader input;
  SparseDataset train_dataset = input.read_input_criteo_kaggle_sparse(
      "tests/test_data/train_lr.csv", ",", config);  // normalize=true
  train_dataset.check();
  train_dataset.print_info();

  SparseLRModel model(1 << config.get_model_bits());
  std::unique_ptr<SparseModelGet> sparse_model_get = std::make_unique<SparseModelGet>("127.0.0.1", 1337);
  int version = 0;
  while (1) {
    SparseDataset minibatch = train_dataset.random_sample(20);
    sparse_model_get->get_new_model_inplace(*dataset, model, config);
    auto gradient = model.minibatch_grad_sparse(*dataset, config);
    gradient->setVersion(version++);
    LRSparseGradient* lrg = dynamic_cast<LRSparseGradient*>(gradient.get());
    push_gradient(lrg);
  }
}
