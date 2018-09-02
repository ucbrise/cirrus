/**
  * SparseDataset is a class that is used to manage a sparse dataset
  */

#include <SparseDataset.h>
#include <algorithm>
#include <Utils.h>
#include <Checksum.h>

#include <cassert>
#include <limits>

#define DEBUG

namespace cirrus {

SparseDataset::SparseDataset() {
}

SparseDataset::SparseDataset(
    std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>& samples)
    : data_(samples) {}

SparseDataset::SparseDataset(
    std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>&& samples)
    : data_(std::move(samples)) {}

SparseDataset::SparseDataset(
    std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>&& samples,
    std::vector<FEATURE_TYPE>&& labels)
    : data_(std::move(samples)), labels_(std::move(labels)) {}

SparseDataset::SparseDataset(const char* data, uint64_t n_samples, bool has_labels) {
  const char* data_begin = data;

  data_.reserve(n_samples);
  labels_.reserve(n_samples);

  for (uint64_t i = 0; i < n_samples; ++i) {
    FEATURE_TYPE label;
    if (has_labels) {
      label = load_value<FEATURE_TYPE>(data);
      labels_.push_back(label);
    }
    int num_sample_values = load_value<int>(data);

#ifdef DEBUG
    if (has_labels) {
      assert(FLOAT_EQ(label, 0.0) || FLOAT_EQ(label, 1.0));
    }
    //std::cout << "num_sample_values: " << num_sample_values <<  std::endl;
    assert(num_sample_values >= 0 && num_sample_values < 1000000);
#endif

    std::vector<std::pair<int, FEATURE_TYPE>> sample;
    sample.reserve(num_sample_values);
    for (int j = 0; j < num_sample_values; ++j) {
      int index = load_value<int>(data);
      FEATURE_TYPE value = load_value<FEATURE_TYPE>(data);
      sample.push_back(std::make_pair(index, value));
    }
    data_.push_back(sample);
  }

  size_bytes = std::distance(data_begin, data);
}

SparseDataset::SparseDataset(const char* data, bool from_s3, bool has_labels) {
  int obj_size = 0;
  if (from_s3) { // comes from s3 so get rid of object size
    obj_size = load_value<int>(data); // read object size
  } else {
    throw std::runtime_error("not supported");
  }

  int n_samples = load_value<int>(data);
  std::cout << "SparseDataset constructor"
    << " from_s3: " << from_s3
    << " obj_size: " << obj_size
    << " n_samples " << n_samples
    << std::endl;

  assert(n_samples > 0 && n_samples < 1000000); // sanity check

  for (int i = 0; i < n_samples; ++i) {
    FEATURE_TYPE label;
    if (has_labels) {
      label = load_value<FEATURE_TYPE>(data);
      assert(label == 0.0 || label == 1.0);
    }
    int num_sample_values = load_value<int>(data);

    if (num_sample_values < 0 || num_sample_values > 1000000) {
      std::cout << "num_sample_values: " << num_sample_values << std::endl;
      throw std::runtime_error("num_sample_values not ok");
    }

    std::vector<std::pair<int, FEATURE_TYPE>> sample;
    for (int j = 0; j < num_sample_values; ++j) {
      int index = load_value<int>(data);
      FEATURE_TYPE value = load_value<FEATURE_TYPE>(data);
      sample.push_back(std::make_pair(index, value));
    }
    data_.push_back(sample);
    if (has_labels) {
      labels_.push_back(label);
    }
  }
}

uint64_t SparseDataset::num_samples() const {
    return data_.size();
}

void SparseDataset::check() const {
  for (const auto& w : data_) {
    for (const auto& v : w) {
      // check index value
      if (v.first < 0) {
        throw std::runtime_error("Input error");
      }

      FEATURE_TYPE rating = v.second;
      if (std::isnan(rating) || std::isinf(rating)) {
        throw std::runtime_error(
            "SparseDataset::check_values nan/inf rating");
      }
    }
  }
}

void SparseDataset::check_ratings() const {
  for (const auto& w : data_) {
    for (const auto& v : w) {
      // check index value
      if (v.first < 0) {
        throw std::runtime_error("Input error");
      }

      FEATURE_TYPE rating = v.second;
      if (rating < -100 || rating > 100) {
        throw std::runtime_error(
            "SparseDataset::check_values wrong rating value: " + std::to_string(rating));
      }
      if (std::isnan(rating) || std::isinf(rating)) {
        throw std::runtime_error(
            "SparseDataset::check_values nan/inf rating");
      }
    }
  }
}

void SparseDataset::check_labels() const {
  for (const auto& l : labels_) {
    if (std::isnan(l) || std::isinf(l)) {
      throw std::runtime_error(
          "SparseDataset::check_labels nan/inf rating");
    }
    if (l != 0.0 && l != 1.0) {
      throw std::runtime_error("Label is not 1.0 nor 0.0");
    }
  }
}

void SparseDataset::print() const {
  std::cout << "SparseDataset" << std::endl;
  for (const auto& w : data_) {
    for (const auto& v : w) {
      std::cout << v.first << ":" << v.second << " ";
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;
}

void SparseDataset::print_info() const {
  std::cout << "SparseDataset #samples: " << data_.size() << std::endl;
  std::cout << "SparseDataset #labels: " << labels_.size() << std::endl;

  //double avg = 0;
  //uint64_t count = 0;
  //for (const auto& w : data_) {
  //  for (const auto& v : w) {
  //    avg += v.second;
  //    count++;
  //  }
  //}

  //std::cout << "Average rating: " << (avg / count) << std::endl;

}

/** FORMAT OF S3 object
  * Size of object in bytes (int)
  * Number of samples (int)
  * ------------- With labels (store_labels = true)
  * Sample 1: Label (FEATURE_TYPE) | number of values (int) | index1 (int) | value1 (FEATURE_TYPE) | index2 | ...
  * Sample 2: ...
  * ------------- Without labels (store_labels = false)
  * Sample 1: number of values (int) | index1 (int) | value1 (FEATURE_TYPE) | index2 | ...
  * Sample 2: ...
  */
std::shared_ptr<char> SparseDataset::build_serialized_s3_obj(
    uint64_t l, uint64_t r, uint64_t* obj_size, bool store_labels) {
  // count number of entries in this object

  assert(l < r);

  uint64_t number_entries_obj = 0;
  for (uint64_t i = l; i < r; ++i) {
    for (const auto& v : data_[i]) {
      (void)v;
      number_entries_obj++;
    }
  }

  // for each value we store int (index) and FEATURE_TYPE (value)
  *obj_size = number_entries_obj * (sizeof(FEATURE_TYPE) + sizeof(int));
  // we also store the labels (if store_labels = true)
  uint64_t n_samples = r - l;
  *obj_size += n_samples * ( (store_labels ? sizeof(FEATURE_TYPE) : 0) + sizeof(int));
  *obj_size += sizeof(int) * 2; // we also store the size of the obejct and the number of samples

  // allocate memory for this object
  std::shared_ptr<char> s3_obj = std::shared_ptr<char>(
      new char[*obj_size],
      std::default_delete<char[]>());

  char* s3_obj_ptr = s3_obj.get();
  store_value<int>(s3_obj_ptr, *obj_size);
  store_value<int>(s3_obj_ptr, n_samples);

  // go on each sample and store it
  for (uint64_t i = l; i < r; ++i) {
    // copy label
    if (store_labels) {
      store_value<FEATURE_TYPE>(s3_obj_ptr, labels_[i]);
    }
    store_value<int>(s3_obj_ptr, data_[i].size());

    for (const auto& v : data_[i]) {
      store_value<int>(s3_obj_ptr, v.first);
      store_value<FEATURE_TYPE>(s3_obj_ptr, v.second);    
    }
  }

  return s3_obj;
}

SparseDataset SparseDataset::random_sample(uint64_t n_samples) const {
  std::random_device rd;
  std::default_random_engine re(rd());
  std::uniform_int_distribution<int> sampler(0, num_samples() - 1);

  std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> samples;
  std::vector<FEATURE_TYPE> labels;

  for (uint64_t i = 0; i < n_samples; ++i) {
    int index = sampler(re);
    samples.push_back(data_[index]);
    labels.push_back(labels_[index]);
  }

  return SparseDataset(std::move(samples), std::move(labels));
}

SparseDataset SparseDataset::sample_from(uint64_t start, uint64_t n_samples) const {

  if (start + n_samples > data_.size()) {
    throw std::runtime_error("Start goes over size of dataset");
  }

  std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> samples;
  for (uint64_t i = start; i < start + n_samples; ++i) {
    samples.push_back(data_[i]);
  }

  return std::move(SparseDataset(samples));
}

void SparseDataset::normalize(uint64_t hash_size) {
  std::vector<FEATURE_TYPE> max_val_feature(hash_size);
  std::vector<FEATURE_TYPE> min_val_feature(hash_size,
      std::numeric_limits<FEATURE_TYPE>::max());

  for (const auto& w : data_) {
    for (const auto& v : w) {
      uint64_t index = v.first;
      FEATURE_TYPE value = v.second;

#ifdef DEBUG
      if (index >= hash_size)
        throw std::runtime_error("Index bigger than capacity");
#endif

      max_val_feature[index] = std::max(value, max_val_feature[index]);
      min_val_feature[index] = std::min(value, min_val_feature[index]);
    }
  }
  for (auto& w : data_) {
    for (auto& v : w) {
      int index = v.first;

      // only normalize if there are different values
      // in the same column
      if (max_val_feature[index] != min_val_feature[index]) {
        v.second = (v.second - min_val_feature[index]) / 
          (max_val_feature[index] - min_val_feature[index]);
      }
    }
  }
}

const std::vector<std::pair<int, FEATURE_TYPE>>& SparseDataset::get_row(uint64_t n) const {
  if (n >= data_.size()) {
    throw std::runtime_error("Wrong index");
  }
  return data_[n];
}

uint64_t SparseDataset::num_features() const {
  uint64_t count = 0;
  for (const auto& w : data_) {
    for (const auto& v : w) {
      (void)v;
      count++;
    }
  }
  return count;
}

} // namespace cirrus

