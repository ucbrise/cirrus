#ifndef _S3_ITERATOR_H_
#define _S3_ITERATOR_H_

#include <Configuration.h>

namespace cirrus {

class S3Iterator {
 public:
    S3Iterator(const Configuration& c) :
      config(c) {}

 private:
      Configuration config;
};

} // namespace cirrus

#endif  // _S3_ITERATOR_H_
