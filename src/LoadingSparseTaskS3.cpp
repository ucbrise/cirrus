#include <Tasks.h>

#include <InputReader.h>
#include <S3.h>
#include <S3Client.h>
#include <Serializers.h>
#include <Utils.h>
#include <config.h>

namespace cirrus {

#define READ_INPUT_THREADS (10)
SparseDataset LoadingSparseTaskS3::read_dataset(
    const Configuration& config) {
  InputReader input;

  std::string delimiter;
  if (config.get_load_input_type() == "csv_space") {
    delimiter = "";
  } else if (config.get_load_input_type() == "csv_tab") {
    delimiter = "\t";
  } else if (config.get_load_input_type() == "csv") {
    delimiter = ",";
  } else {
    throw std::runtime_error("unknown input type");
  }

  // READ the kaggle criteo dataset
  return input.read_input_criteo_kaggle_sparse(config.get_load_input_path(),
                                               delimiter, config);
}

void LoadingSparseTaskS3::check_label(FEATURE_TYPE label) {
  if (label != 1.0 && label != 0.0) {
    throw std::runtime_error("Wrong label value");
  }
}

/**
  * Check if loading was well done
  */
void LoadingSparseTaskS3::check_loading(const Configuration& config,
                                        std::unique_ptr<S3Client>& s3_client) {
  std::cout << "[LOADER] Trying to get sample with id: " << 0 << std::endl;

  std::string obj_id = std::to_string(SAMPLE_BASE);
  std::string data =
      s3_client->s3_get_object_value(obj_id, config.get_s3_bucket());

  SparseDataset dataset(data.data(), true);
  dataset.check();
  dataset.check_labels();

  //std::cout << "[LOADER] Checking label values.." << std::endl;
  //check_label(sample.get()[0]);

  const auto& s = dataset.get_row(0);
  std::cout << "[LOADER] " << "Print sample 0 with size: " << s.size() << std::endl;
  for (const auto& feature : s) {
    int index = feature.first;
    FEATURE_TYPE value = feature.second;
    std::cout << index << "/" << value << " ";
  }

  for (uint64_t i = 0; i < dataset.num_samples(); ++i) {
    //const auto& s = dataset.get_row(i);
    const auto& label = dataset.labels_[i];
    if (label != 0.0 && label != 1.0) {
      throw std::runtime_error("Wrong label");
    }
  }
}

/**
 * Load the object store with the training dataset
 * It reads from the criteo dataset files and writes to the object store
 * It signals when work is done by changing a bit in the object store
 */
void LoadingSparseTaskS3::run(const Configuration& config) {
  std::cout << "[LOADER-SPARSE] " << "Read criteo input..." << std::endl;

  uint64_t s3_obj_num_samples = config.get_s3_size();
  std::unique_ptr<S3Client> s3_client = std::make_unique<S3Client>();

  SparseDataset dataset = read_dataset(config);
  dataset.check();

  uint64_t num_s3_objs = dataset.num_samples() / s3_obj_num_samples;
  std::cout << "[LOADER-SPARSE] "
    << "Adding " << dataset.num_samples()
    << " #s3 objs: " << num_s3_objs
    << " bucket: " << config.get_s3_bucket()
    << std::endl;

  // For each S3 object (group of s3_obj_num_samples samples)
  for (unsigned int i = 0; i < num_s3_objs; ++i) {
    std::cout << "[LOADER-SPARSE] Building s3 batch #" << (i + 1) << std::endl;

    uint64_t first_sample = i * s3_obj_num_samples;
    uint64_t last_sample = (i + 1) * s3_obj_num_samples;

    uint64_t len;
    // this function already returns a nicely packed object
    std::shared_ptr<char> s3_obj =
      dataset.build_serialized_s3_obj(first_sample, last_sample, &len);

    std::cout << "Putting object in S3 with size: " << len << std::endl;
    // we hash names to help with scaling in S3
    std::string obj_id = std::to_string(SAMPLE_BASE + i);
    s3_client->s3_put_object(obj_id, config.get_s3_bucket(),
                             std::string(s3_obj.get(), len));
  }
  check_loading(config, s3_client);
  std::cout << "LOADER-SPARSE terminated successfully" << std::endl;
}

} // namespace cirrus

