#include <Tasks.h>

#include "Serializers.h"
#include "InputReader.h"
#include "Utils.h"
#include "config.h"

cirrus::SparseDataset read_dataset(
    const cirrus::Configuration& config) {
  cirrus::InputReader input;

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

/**
load_input_path: /mnt/efs/criteo_kaggle/train.csv
load_input_type: csv # for the criteo kaggle train.csv
minibatch_size: 20
s3_size: 50000
learning_rate: 0.01
epsilon: 0.0001
model_type: LogisticRegression
num_classes: 2
num_features: 13
limit_cols: 14
normalize: 1
limit_samples: 50000000
s3_bucket: cirrus-criteo-kaggle-19b-random
model_bits: 19
train_set: 0-824
test_set: 825-840
use_bias: 1
use_grad_threshold: 1
grad_threshold: 0.001
model_bits: 20
*/

/**
 * Load the object store with the training dataset
 * It reads from the criteo dataset files and writes to the object store
 * It signals when work is done by changing a bit in the object store
 */
int main() {
  std::cout << "Reading criteo input..." << std::endl;

  cirrus::Configuration config;
  config.load_input_path = "/mnt/efs/criteo_kaggle/train.csv";
  config.load_input_type = "csv";
  config.s3_bucket_name = "--";
  config.limit_samples = 50000000;
  config.model_bits = 19;
  config.normalize = 1;
  config.check();

  cirrus::SparseDataset dataset = read_dataset(config);
  dataset.check();

  std::ofstream ofs ("/mnt/efs/csv_to_libsvm.txt", std::ofstream::out);

  for (uint32_t i = 0; i < dataset.num_samples(); ++i) {
    const auto label = dataset.labels_[i];
    const auto& features = dataset.data_[i];

    ofs << label;
    for (const auto& feat : features) {
      ofs << " " << feat.first << ":" << feat.second;
    }
    ofs << "\n";
  }
  return 0;
}

