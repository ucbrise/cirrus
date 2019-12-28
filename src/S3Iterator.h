#ifndef S3_ITERATOR_H_
#define S3_ITERATOR_H_

#include <Configuration.h>
#include <SparseDataset.h>

namespace cirrus {

class S3Iterator {
 public:
  S3Iterator(const Configuration& c, bool has_labels);
  virtual ~S3Iterator() = default;

  virtual std::shared_ptr<SparseDataset> getNext() = 0;

 protected:
  Configuration config;
  bool has_labels;
};

} // namespace cirrus

#endif  // S3_ITERATOR_H_
