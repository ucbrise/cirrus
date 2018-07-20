#include <S3.h>
#include <S3IteratorLibsvm.h>
#include <Configuration.h>
#include <SparseDataset.h>
#include <memory>

int main() {
  cirrus::s3_initialize_aws();
  cirrus::Configuration conf;
  cirrus::S3IteratorLibsvm iter(conf,
                                "criteo-kaggle2014-train-libsvm-100k",
                                "criteo.kaggle2014.train.svm_100K",
                                58000000, 20, 0, false);

  while (1) {
    std::shared_ptr<cirrus::SparseDataset> mb = iter.getNext();

    mb->print();
  }

  return 0;
}
