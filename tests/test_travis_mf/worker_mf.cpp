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
#include <SparseMFModel.h>
#include <Tasks.h>
#include "SGD.h"
#include "Serializers.h"
#include "Utils.h"

using namespace cirrus;

cirrus::Configuration config = cirrus::Configuration("configs/jester.cfg");

int main() {
  InputReader input;
  int nusers, njokes;
  SparseDataset train_dataset = input.read_jester_ratings(
      "tests/test_data/jester_train.csv", &nusers, &njokes);
  train_dataset.check();
  train_dataset.print_info();
  int nfactors = 10;
  int batch_size = 200;

  SparseMFModel model(nusers, njokes, nfactors);
  std::unique_ptr<PSSparseServerInterface> psi =
      std::make_unique<PSSparseServerInterface>("127.0.0.1", 1338);
  psi->connect();
  int version = 0;
  while (1) {
    for (int i = 0; i < nusers; i += batch_size) {
      int actual_batch_size = batch_size;
      if (i + batch_size >= nusers) {
        actual_batch_size = nusers - i - 1;
      }
      SparseDataset ds = train_dataset.sample_from(i, actual_batch_size);
      model = psi->get_sparse_mf_model(ds, i, actual_batch_size);
      auto gradient = model.minibatch_grad(ds, config, i);
      gradient->setVersion(version++);
      MFSparseGradient* mfg = dynamic_cast<MFSparseGradient*>(gradient.get());
      if (mfg == nullptr) {
        throw std::runtime_error("Error in dynamic cast");
      }
      psi->send_mf_gradient(*mfg);
    };
  }
}
