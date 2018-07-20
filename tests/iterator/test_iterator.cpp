#include <S3IteratorLibsvm.h>
#include <Configuration.h>

int main() {
  cirrus::Configuration conf;
  cirrus::S3IteratorLibsvm iter(conf, 0, 0, 0, false);

  while (1) {
    iter.getNext();
  }

  return 0;
}
