#include <unistd.h>
#include <cstdlib>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>

#include <InputReader.h>
#include <PSSparseServerInterface.h>
#include <SparseLRModel.h>
#include <Configuration.h>
#include "SGD.h"
#include "Utils.h"
#include "Serializers.h"
#include <Tasks.h>

using namespace cirrus;

cirrus::Configuration config = cirrus::Configuration("configs/jester.cfg");

int main() {
  InputReader input;
  int nusers, njokes;
  SparseDataset train_dataset = input.read_jester_ratings(
      "tests/test_data/jester_train.csv", &nusers, &njokes);
  train_dataset.check();
  train_dataset.print_info();
  int nfactors = 7;
  int batch_size = 20;

  SparseMFModel model(nusers, njokes, nfactors);
  std::unique_ptr<PSSparseServerInterface> psi =
      std::make_unique<PSSparseServerInterface>("127.0.0.1", 1337);
  std::cout << "Pointer to psi" << std::endl;
  psi->connect();
  std::cout << "Connected" << std::endl;
  int version = 0;
  while (1) {
    std::cout << "In while loop" << std::endl;
    for (uint64_t i = 0; i + batch_size < nusers; i += batch_size) {
      SparseDataset ds = train_dataset.sample_from(i, batch_size);
      std::cout << "Sampled train data" << std::endl;
      model = psi->get_sparse_mf_model(ds, i, batch_size);
      std::cout << "Got sparse mf model" << std::endl;
      auto gradient = model.minibatch_grad(ds, config, i);
      std::cout << "Calculated gradient" << std::endl;
      gradient->setVersion(version++);
      MFSparseGradient* mfg = dynamic_cast<MFSparseGradient*>(gradient.get());
      if (mfg == nullptr) {
        throw std::runtime_error("Error in dynamic cast");
      }
      psi->send_mf_gradient(*mfg);
      std::cout << "Send gradient" << std::endl;
    };
    break;
  }
}
