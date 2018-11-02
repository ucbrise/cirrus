#include <unistd.h>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <Configuration.h>
#include <InputReader.h>
#include <PSSparseServerInterface.h>
#include <SparseLRModel.h>
#include <Tasks.h>
#include "SGD.h"
#include "Serializers.h"
#include "Utils.h"

using namespace cirrus;

cirrus::Configuration config = cirrus::Configuration("configs/test_config.cfg");

int main() {
  InputReader input;
  SparseDataset train_dataset = input.read_input_criteo_kaggle_sparse(
      "tests/test_data/train_lr.csv", ",", config);  // normalize=true
  train_dataset.check();
  train_dataset.print_info();

  SparseLRModel model(1 << config.get_model_bits());
  std::unique_ptr<PSSparseServerInterface> psi =
      std::make_unique<PSSparseServerInterface>("127.0.0.1", 1337);
  psi->connect();
  int version = 0;
  while (1) {
    SparseDataset minibatch = train_dataset.random_sample(20);
    psi->get_lr_sparse_model_inplace(minibatch, model, config);
    auto gradient = model.minibatch_grad_sparse(minibatch, config);
    gradient->setVersion(version++);
    LRSparseGradient* lrg = dynamic_cast<LRSparseGradient*>(gradient.get());
    if (lrg == nullptr) {
      throw std::runtime_error("Error in dynamic cast");
    }
    psi->send_lr_gradient(*lrg);
  }
  return 0;
}
