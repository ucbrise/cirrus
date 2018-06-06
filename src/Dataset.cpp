/**
  * Dataset is a class that is used to manage a dataset
  * where each sample is a vector of type FEATURE_TYPE
  */

#include <Dataset.h>
#include <algorithm>
#include <Utils.h>
#include <Checksum.h>

#include <cassert>

namespace cirrus {

Dataset::Dataset() {
}

Dataset::Dataset(const std::vector<std::vector<FEATURE_TYPE>>& samples,
        const std::vector<FEATURE_TYPE>& labels) :
    samples_(samples) {
    FEATURE_TYPE* l = new FEATURE_TYPE[labels.size()];
    std::copy(labels.data(), labels.data() + labels.size(), l);
    labels_.reset(l, array_deleter<FEATURE_TYPE>);
}

// XXX FIX
Dataset::Dataset(const FEATURE_TYPE* minibatch,
                 uint64_t n_samples,
                 uint64_t n_features) :
    samples_(minibatch, n_samples, n_features, true) {
    
    FEATURE_TYPE* l = new FEATURE_TYPE[n_samples];
    for (uint64_t j = 0; j < n_samples;++j) {
      const FEATURE_TYPE* data = minibatch + j * (n_features + 1);
      l[j] = *data;
    }
    labels_.reset(l, array_deleter<FEATURE_TYPE>);
}

Dataset::Dataset(const FEATURE_TYPE* samples,
                 const FEATURE_TYPE* labels,
                 uint64_t n_samples,
                 uint64_t n_features) :
    samples_(samples, n_samples, n_features) {
    FEATURE_TYPE* l = new FEATURE_TYPE[n_samples];
    std::copy(labels, labels + n_samples, l);
    labels_.reset(l, array_deleter<FEATURE_TYPE>);
}

Dataset::Dataset(std::vector<std::shared_ptr<FEATURE_TYPE>> samples,
                 std::vector<std::shared_ptr<FEATURE_TYPE>> labels,
                 uint64_t samples_per_batch,
                 uint64_t features_per_sample) :
    samples_(samples, samples_per_batch, features_per_sample) {
  assert(labels.size() == samples.size());

  uint64_t num_labels = samples.size() * samples_per_batch;
  FEATURE_TYPE* all_labels = new FEATURE_TYPE[num_labels];

  // copy labels in each minibatch sequentially
  for (uint64_t i = 0; i < labels.size(); ++i) {
    std::memcpy(
        all_labels + i * samples_per_batch,
        labels[i].get(),
        samples_per_batch * sizeof(FEATURE_TYPE));
  }

  labels_.reset(all_labels, array_deleter<FEATURE_TYPE>);
}

uint64_t Dataset::num_features() const {
    return samples_.cols;
}

uint64_t Dataset::num_samples() const {
    return samples_.rows;
}

void Dataset::check() const {
  const FEATURE_TYPE* l = labels_.get();
  for (uint64_t i = 0; i < num_samples(); ++i) {
    if (!FLOAT_EQ(l[i], 1.0) && !FLOAT_EQ(l[i], 0.0)) {
      throw std::runtime_error(
          "Dataset::check_values wrong label value: " + std::to_string(l[i]));
    }
    if (std::isnan(l[i]) || std::isinf(l[i])) {
      throw std::runtime_error(
          "Dataset::check_values nan/inf error in labels");
    }
  }
  samples_.check_values();
}

double Dataset::checksum() const {
    return crc32(labels_.get(), num_samples()) + samples_.checksum();
}

void Dataset::print() const {
    samples_.print();
}

void Dataset::print_info() const {
  std::cout << "Dataset #samples: " << samples_.rows << std::endl;
  std::cout << "Dataset #cols: " << samples_.cols << std::endl;
}

std::shared_ptr<FEATURE_TYPE>
Dataset::build_s3_obj(uint64_t l, uint64_t r) {
  uint64_t num_samples = r - l;
  uint64_t entries_per_sample = samples_.cols + 1;

  std::shared_ptr<FEATURE_TYPE> s3_obj = std::shared_ptr<FEATURE_TYPE>(
      new FEATURE_TYPE[num_samples * entries_per_sample],
      std::default_delete<FEATURE_TYPE[]>());

  std::cout << "entries_per_sample: " << entries_per_sample << std::endl;
  for (uint64_t i = 0; i < num_samples; ++i) {
    FEATURE_TYPE* d = s3_obj.get() + i * entries_per_sample;


    // copy label
    *d = labels_.get()[i];
    if (!FLOAT_EQ(*d, 0.0) && !FLOAT_EQ(*d, 1.0))
      throw std::runtime_error("Erorr in build_s3_obj");
    d++;  // move to features

    // copy features
    const FEATURE_TYPE* start = samples_.row(l + i);
    const FEATURE_TYPE* end = samples_.row(l + i) + samples_.cols + 1;
    std::copy(start, end, d);
  }

  return s3_obj;
}

Dataset Dataset::Dataset::random_sample(uint64_t n_samples) const {
  std::random_device rd;
  std::default_random_engine re(rd());
  std::uniform_int_distribution<int> sampler(0, num_samples());

  std::vector<std::vector<FEATURE_TYPE>> samples;
  std::vector<FEATURE_TYPE> labels;

  for (uint64_t i = 0; i < n_samples; ++i) {
    int index = sampler(re);

    const FEATURE_TYPE* s = sample(index);
    samples.push_back(std::vector<FEATURE_TYPE>(s, s + num_features()));
    labels.push_back(*label(index));
  }

  return Dataset(samples, labels);
}

} // namespace cirrus

