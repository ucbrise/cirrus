#include <Configuration.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <Utils.h>

namespace cirrus {

Configuration::Configuration() :
        learning_rate(-1),
        epsilon(-1)
{}

Configuration::Configuration(const std::string& path) :
        learning_rate(-1),
        epsilon(-1) {
  read(path);
}

/**
  * Read configuration from a specific config file
  * @param path Path to the configuration file
  */
void Configuration::read(const std::string& path) {
    std::ifstream fin(path.c_str(), std::ifstream::in);

    if (!fin) {
        std::cout << "Error opening config file: " << std::endl;
        throw std::runtime_error("Error opening config file: " + path);
    }

    std::string line;
    while (getline(fin, line)) {
      if (line.size() && line[0] == '#') {
        // skip comments
        continue;
      }
      parse_line(line);
    }

    print();
    check();
}

void Configuration::print() const {
    std::cout << "Printing configuration: " << std::endl;
    std::cout << "input_path: " << get_input_path() << std::endl;
    std::cout << "Minibatch size: " << get_minibatch_size() << std::endl;
    std::cout << "S3 size: " << get_s3_size() << std::endl;
    std::cout << "learning rate: " << get_learning_rate() << std::endl;
    std::cout << "limit_samples: " << get_limit_samples() << std::endl;
    std::cout << "epsilon: " << epsilon << std::endl;
    std::cout << "s3_bucket_name: " << s3_bucket_name << std::endl;
    std::cout << "use_bias: " << use_bias << std::endl;
    std::cout << "momentum_beta: " << momentum_beta << std::endl;
    std::cout << "use_grad_threshold: " << use_grad_threshold << std::endl;
    std::cout << "opt_method: " << opt_method << std::endl;
    std::cout << "grad_threshold: " << grad_threshold << std::endl;
    std::cout << "model_bits: " << model_bits << std::endl;
    std::cout << "netflix_workers: " << netflix_workers << std::endl;
    std::cout << "train_set: "
      << train_set_range.first << "-" << train_set_range.second << std::endl;
    std::cout << "test_set: "
      << test_set_range.first << "-" << test_set_range.second << std::endl;
    std::cout << "checkpoint_frequency: " << checkpoint_frequency << std::endl;
    std::cout << "checkpoint_s3_bucket: " << checkpoint_s3_bucket << std::endl;
    std::cout << "checkpoint_s3_keyname: " << checkpoint_s3_keyname << std::endl;
    if (nusers || nitems) {
      std::cout
        << "users: " << nusers << std::endl
        << " items: " << nitems << std::endl;
    }
}

void Configuration::check() const {
  if (s3_bucket_name == "") {
    throw std::runtime_error("S3 bucket name missing from config file");
  }
  if (test_set_range.first && model_type == COLLABORATIVE_FILTERING) {
    throw std::runtime_error(
            "Can't use test range with COLLABORATIVE_FILTERING");
  }
  if (use_grad_threshold && grad_threshold == 0) {
    throw std::runtime_error("Can't use a 0 for grad threshold");
  }
  if (model_bits == 0) {
    throw std::runtime_error("Model bits can't be 0");
  }
  if (opt_method != "adagrad" && opt_method != "nesterov"
          && opt_method != "momentum" && opt_method != "sgd") {
      throw std::runtime_error(
              "Choose a valid update rule: adagrad, nesterov, momentum, or sgd");
  }
  if (checkpoint_frequency > 0
          && (checkpoint_s3_bucket == "" || checkpoint_s3_keyname == "")) {
      throw std::runtime_error("Wrong checkpoing configuration parameters");
  }
}

/**
  * Parse a specific line in the config file
  * @param line A line from the input file
  */
void Configuration::parse_line(const std::string& line) {
    std::istringstream iss(line);

    std::string s;
    iss >> s;

    std::cout << "Parsing line: " << line << std::endl;

    if (s == "minibatch_size:") {
        iss >> minibatch_size;
        if (s3_size && (s3_size % minibatch_size != 0)) {
          throw std::runtime_error("s3_size not multiple of minibatch_size");
        }
    } else if (s == "s3_size:") {
        iss >> s3_size;
        if (minibatch_size && (s3_size % minibatch_size != 0)) {
          throw std::runtime_error("s3_size not multiple of minibatch_size");
        }
    } else if (s == "num_features:") {
        iss >> num_features;
    } else if (s == "input_path:") {
        iss >> input_path;
    } else if (s == "samples_path:") {
        iss >> samples_path;
    } else if (s == "labels_path:") {
        iss >> labels_path;
    } else if (s == "n_workers:") {
        iss >> n_workers;
    } else if (s == "opt_method:") {
        iss >> opt_method; 
    }  else if (s == "epsilon:") {
        iss >> epsilon;
    } else if (s == "input_type:") {
        iss >> input_type;
    } else if (s == "learning_rate:") {
        iss >> learning_rate;
    } else if (s == "num_classes:") {
        iss >> num_classes;
    } else if (s == "limit_cols:") {
        iss >> limit_cols;
    } else if (s == "limit_samples:") {
        iss >> limit_samples;
    } else if (s == "momentum_beta:") {
        iss >> momentum_beta;  
    } else if (s == "s3_bucket:") {
        iss >> s3_bucket_name;
    } else if (s == "use_bias:") {
        iss >> use_bias;
    } else if (s == "num_users:") {
        iss >> nusers;
    } else if (s == "num_items:") {
        iss >> nitems; 
    } else if (s == "model_bits:") {
        iss >> model_bits;
    } else if (s == "netflix_workers:") {
       iss >> netflix_workers;
    } else if (s == "checkpoint_frequency:") {
       iss >> checkpoint_frequency;
    } else if (s == "checkpoint_s3_bucket:") {
       iss >> checkpoint_s3_bucket;
    } else if (s == "checkpoint_s3_keyname:") {
       iss >> checkpoint_s3_keyname;
    } else if (s == "normalize:") {
      int n;
      iss >> n;
      normalize = (n == 1);
    } else if (s == "model_type:") {
      std::string model;
      iss >> model;
      if (model == "LogisticRegression") {
          model_type = LOGISTICREGRESSION;
      } else if (model == "Softmax") {
          model_type = SOFTMAX;
      } else if (model == "CollaborativeFiltering") {
          model_type = COLLABORATIVE_FILTERING;
      } else {
          throw std::runtime_error(std::string("Unknown model : ") + model);
      }
    } else if (s == "train_set:") {
      std::string range;
      iss >> range;
      size_t index = range.find("-");
      if (index == std::string::npos) {
        throw std::runtime_error("Wrong index");
      }
      std::string left = range.substr(0, index);
      std::string right = range.substr(index + 1);
      train_set_range = std::make_pair(
          string_to<int>(left),
          string_to<int>(right));
    } else if (s == "test_set:") {
      std::string range;
      iss >> range;
      size_t index = range.find("-");
      if (index == std::string::npos) {
        throw std::runtime_error("Wrong index");
      }
      std::string left = range.substr(0, index);
      std::string right = range.substr(index + 1);
      test_set_range = std::make_pair(
          string_to<int>(left),
          string_to<int>(right));
    } else if (s == "use_grad_threshold:") {
      std::string b;
      iss >> b;
      if (b != "0" && b != "1") {
        throw std::runtime_error("use_grad_threshold must be 0/1");
      }
      use_grad_threshold = string_to<bool>(b);
    } else if (s == "grad_threshold:") {
      iss >> grad_threshold;
    } else {
        throw std::runtime_error("Unrecognized option: " + line);
    }

    if (iss.fail()) {
        throw std::runtime_error("Error parsing configuration file");
    }
}

std::string Configuration::get_input_path() const {
    if (input_path == "")
        throw std::runtime_error("input path not loaded");
    return input_path;
}

std::string Configuration::get_samples_path() const {
    if (samples_path == "")
        throw std::runtime_error("samples path not loaded");
    if (input_type != "double_binary")
        throw std::runtime_error("mismatch between paths and input type");
    return samples_path;
}

std::string Configuration::get_labels_path() const {
    if (labels_path == "")
        throw std::runtime_error("labels path not loaded");
    if (input_type != "double_binary")
        throw std::runtime_error("mismatch between paths and input type");
    return labels_path;
}

double Configuration::get_learning_rate() const {
    if (learning_rate == -1)
        throw std::runtime_error("learning rate not loaded");
    return learning_rate;
}

double Configuration::get_epsilon() const {
    if (epsilon == -1)
        throw std::runtime_error("epsilon not loaded");
    return epsilon;
}

uint64_t Configuration::get_minibatch_size() const {
    if (minibatch_size == 0)
        throw std::runtime_error("Minibatch size not loaded");
    return minibatch_size;
}

uint64_t Configuration::get_s3_size() const {
    if (s3_size == 0)
        throw std::runtime_error("Minibatch size not loaded");
    return s3_size;
}

std::string Configuration::get_input_type() const {
    if (input_type == "")
        throw std::runtime_error("input_type not loaded");
    return input_type;
}

/**
  * Get the format of the input dataset from the config file
  */
Configuration::ModelType Configuration::get_model_type() const {
    if (model_type == UNKNOWN) {
        throw std::runtime_error("model_type not loaded");
    }
    return model_type;
}

/**
  * Get the number of classes we use in this workload/dataset/algorithm
  */
uint64_t Configuration::get_num_classes() const {
    if (num_classes == 0) {
        throw std::runtime_error("num_classes not loaded");
    }
    return num_classes;
}

/**
  * Get the maximum number of features/columns to read from each sample
  */
uint64_t Configuration::get_limit_cols() const {
    if (limit_cols == 0) {
      std::cout << "limit_cols not loaded" << std::endl;
    }
    return limit_cols;
}

/**
  * Get the flag saying whether the dataset should be normalized or not
  */
bool Configuration::get_normalize() const {
    return normalize;
}

/**
  * Get number of training input samples
  */
uint64_t Configuration::get_limit_samples() const {
    return limit_samples;
}

/**
  * Get number of training input samples
  */
uint64_t Configuration::get_num_features() const {
    return num_features;
}

/**
  * Get S3 bucket name
  */
std::string Configuration::get_s3_bucket() const {
  return s3_bucket_name;
}

std::pair<int, int> Configuration::get_train_range() const {
  return train_set_range;
}

std::pair<int, int> Configuration::get_test_range() const {
  return test_set_range;
}

bool Configuration::get_use_bias() const {
  return use_bias;
}

int Configuration::get_users() const {
  return nusers;
}

int Configuration::get_items() const {
  return nitems;
}

bool Configuration::get_grad_threshold_use() const {
  return use_grad_threshold;
}

double Configuration::get_grad_threshold() const {
  return grad_threshold;
}

uint64_t Configuration::get_model_bits() const {
  return model_bits;
}

uint64_t Configuration::get_netflix_workers() const {
  return netflix_workers;
}

uint64_t Configuration::get_checkpoint_frequency() const {
  return checkpoint_frequency;
}

std::string Configuration::get_checkpoint_s3_bucket() const {
  return checkpoint_s3_bucket;
}

std::string Configuration::get_opt_method() const {
  return opt_method;
}

double Configuration::get_momentum_beta() const {
    return momentum_beta;
}

std::string Configuration::get_checkpoint_s3_keyname() const {
  return checkpoint_s3_keyname;
}

}  // namespace cirrus
