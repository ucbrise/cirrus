#include <Tasks.h>

#include "Serializers.h"
#include "InputReader.h"
#include "S3.h"
#include "Utils.h"
#include "config.h"

namespace cirrus {

SparseDataset LoadingNetflixTask::read_dataset(
    const Configuration& config,
    int& number_movies, int& number_users) {
  InputReader input;
  SparseDataset dataset = input.read_netflix_ratings(
      config.get_load_input_path(), &number_movies, &number_users);
  std::cout << "Processed netflix dataset."
    << " #movies: " << number_movies
    << " #users: " << number_users
    << std::endl;
  dataset.check();
  dataset.print_info();
  return dataset;
}

/**
  * Check if loading was well done
  */
void LoadingNetflixTask::check_loading(const Configuration& config,
                                       std::unique_ptr<S3Client>& s3_client) {
  std::cout << "[LOADER] Trying to get sample with id: " << 0 << std::endl;

  std::string data =
      s3_client->s3_get_object_value(SAMPLE_BASE, config.get_s3_bucket());

  SparseDataset dataset(data.data(), true, false);
  dataset.check();
  dataset.check_labels();

  const auto& s = dataset.get_row(0);
  std::cout << "[LOADER] " << "Print sample 0 with size: " << s.size() << std::endl;
  for (const auto& feature : s) {
    int index = feature.first;
    FEATURE_TYPE value = feature.second;
    std::cout << index << "/" << value << " ";
  }
}

/**
 * Load the object store with the training dataset
 * It reads from the criteo dataset files and writes to the object store
 * It signals when work is done by changing a bit in the object store
 */
void LoadingNetflixTask::run(const Configuration& config) {
  std::cout << "[LOADER-SPARSE] " << "Reading Netflix input..." << std::endl;

  uint64_t s3_obj_num_samples = config.get_s3_size();
  std::unique_ptr<S3Client> s3_client = std::make_unique<S3Client>();

  int number_movies, number_users;
  SparseDataset dataset = read_dataset(config, number_movies, number_users);
  dataset.check();

  uint64_t num_s3_objs = dataset.num_samples() / s3_obj_num_samples;
  std::cout << "[LOADER-SPARSE] "
    << "Adding " << dataset.num_samples()
    << " #s3 objs: " << num_s3_objs
    << std::endl;
  
  // For each S3 object (group of s3_obj_num_samples samples)
  for (unsigned int i = 0; i < num_s3_objs; ++i) {
    std::cout << "[LOADER-SPARSE] Building s3 batch #" << (i + 1) << std::endl;

    uint64_t first_sample = i * s3_obj_num_samples;
    uint64_t last_sample = (i + 1) * s3_obj_num_samples;

    uint64_t len;
    // this function already returns a nicely packed object
    // we don't store labels
    std::shared_ptr<char> s3_obj =
      dataset.build_serialized_s3_obj(first_sample, last_sample, &len, false);

    std::cout
      << "Putting object in S3 with size: " << len
      << std::endl;
    s3_client->s3_put_object(SAMPLE_BASE + i, config.get_s3_bucket(),
                             std::string(s3_obj.get(), len));
  }
  check_loading(config, s3_client);
  std::cout << "LOADER-SPARSE terminated successfully" << std::endl;
}

} // namespace cirrus

