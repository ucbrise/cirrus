#ifndef _SPARSEDATASET_H_
#define _SPARSEDATASET_H_

#include <vector>
#include <cstdint>
#include <memory>
#include <config.h>

namespace cirrus {

/**
  * This class is used to hold a sparse dataset
  * Each sample is a variable size list of pairs <int, FEATURE_TYPE>
  */
class SparseDataset {
  public:
  /**
   * Construct empty dataset
   */
  SparseDataset();

  /**
   * Construct a dataset given a vector of samples and a vector of labels
   * This method copies all the inputs
   * @param samples Vector of samples
   */
  SparseDataset(std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>& samples);
  SparseDataset(std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>&& samples);
  
  /**
   * Construct a dataset given a vector of samples and a vector of labels
   * This method copies all the inputs
   * @param samples Vector of samples
   * @param labels Vector of labels
   */
  SparseDataset(std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>&& samples, std::vector<FEATURE_TYPE>&& labels);
  

  /** Load sparse dataset from serialized format
    */
  SparseDataset(const char*, bool from_s3, bool has_labels = true);
  
  SparseDataset(const char*, uint64_t, bool has_labels = true);

  /**
   * Get the number of samples in this dataset
   * @return Number of samples in the dataset
   */
  uint64_t num_samples() const;

  /**
   * Get the number of features in this dataset
   * @return Number of features in the dataset
   */
  uint64_t num_features() const;

  /**
   * Returns pointer to specific sample in this dataset
   * @param sample Sample index
   * @returns Pointer to dataset sample
   */
  //const FEATURE_TYPE* sample(uint64_t sample) const {
  //  return samples_.row(sample);
  //}

  /**
   * Sanity check values in the dataset
   */
  void check() const;
  void check_ratings() const;
  
  /**
   * Sanity check labels in the dataset
   */
  void check_labels() const;

  /**
   * Print this dataset
   */
  void print() const;

  /**
   * Print some information about the dataset
   */
  void print_info() const;

  /** Build data for S3 object
   * from feature and label data from samples in range [l,r)
   * output size of object in the uint64_t*
   */
  std::shared_ptr<char> build_serialized_s3_obj(uint64_t, uint64_t, uint64_t*, bool store_labels = true);

  /**
   * Return random subset of samples
   * @param n_samples Number of samples to return
   * @return Random subset of samples
   */
  SparseDataset random_sample(uint64_t n_samples) const;
  
  SparseDataset sample_from(uint64_t start, uint64_t n_samples) const;

  void normalize(uint64_t hash_size);

  const std::vector<std::pair<int, FEATURE_TYPE>>& get_row(uint64_t) const;

  uint64_t getSizeBytes() const { return size_bytes; }

  // return train set and test set (in this order)
  std::pair<SparseDataset, SparseDataset> split(double fraction) const;

  public:
  std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> data_;
  std::vector<FEATURE_TYPE> labels_;

  uint64_t size_bytes = 0; // size of data when read from serialized format
};

} // namespace cirrus

#endif  // _SPARSEDATASET_H_
