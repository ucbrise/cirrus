#include <S3IteratorText.h>
#include <Configuration.h>

int main() {
  cirrus::Configuration conf;
  cirrus::S3IteratorText s3iter(conf, 0, 0, 0, false);
  return 0;
}
