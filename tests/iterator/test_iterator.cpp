#include <S3.h>
#include <S3IteratorLibsvm.h>
#include <Configuration.h>

int main() {
  cirrus::s3_initialize_aws();
  cirrus::Configuration conf;
  cirrus::S3IteratorLibsvm iter(conf,
                                "criteo-kaggle2014-train-libsvm-100k",
                                "criteo.kaggle2014.train.svm_100K",
                                0, 20, 0, false);

  while (1) {
    iter.getNext();
  }

  return 0;
}
