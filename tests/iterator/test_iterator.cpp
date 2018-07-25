#include <S3.h>
#include <S3IteratorLibsvm.h>
#include <Configuration.h>
#include <SparseDataset.h>
#include <SparseLRModel.h>
#include <memory>

int main() {
  cirrus::s3_initialize_aws();
  cirrus::Configuration config;
  config.read("criteo_libsvm.cfg");
  cirrus::S3IteratorLibsvm iter(config,
                                "criteo-kaggle2014-train-libsvm-100k",
                                "criteo.kaggle2014.train.svm_100K",
                                58000000, 20, 0, false);

  cirrus::SparseLRModel model(1 << config.get_model_bits());
  while (1) {
    std::shared_ptr<cirrus::SparseDataset> mb = iter.getNext();
    auto gradient = model.minibatch_grad_sparse(*mb, config);

    model.sgd_update(0.0001, gradient.get());

    mb->print();
  }

  return 0;
}
