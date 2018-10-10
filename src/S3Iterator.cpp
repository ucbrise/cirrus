#include "S3Iterator.h"

namespace cirrus {

S3Iterator::S3Iterator(const Configuration& c, bool has_labels)
    : config(c), has_labels(has_labels) {}

}  // namespace cirrus
