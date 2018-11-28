#include <Configuration.h>
#include <LRModel.h>
#include <S3.h>
#include <S3IteratorLibsvm.h>
#include <SparseDataset.h>
#include <SparseLRModel.h>
#include <memory>

// This test requires access to S3

int main() {
  cirrus::s3_initialize_aws();
  cirrus::Configuration config;
  config.read("criteo_libsvm.cfg");
  cirrus::S3IteratorLibsvm iter(config, "criteo-kaggle-libsvm-train",
                                "criteo.kaggle2014.train.svm", 58000000, 20, 0,
                                false);

  cirrus::SparseLRModel model(2 << config.get_model_bits());
  std::shared_ptr<cirrus::SparseDataset> test_data = iter.getNext();
  while (1) {
    std::shared_ptr<cirrus::SparseDataset> mb = iter.getNext();
    auto gradient = model.minibatch_grad(*mb, config);

    model.sgd_update(0.0001, gradient.get());

    // mb->print();

    std::pair<FEATURE_TYPE, FEATURE_TYPE> ret = model.calc_loss(*test_data, 0);
    double total_loss = ret.first;
    double total_accuracy = ret.second;
    double total_num_samples = test_data->num_samples();

    std::cout << "[ERROR_TASK] Loss (Total/Avg): " << total_loss << "/"
              << (total_loss / total_num_samples)
              << " Accuracy: " << (total_accuracy) << std::endl;
  }

  return 0;
}
