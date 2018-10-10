#ifndef _CONFIGURATION_H_
#define _CONFIGURATION_H_

#include <string>

namespace cirrus {

class Configuration {
 public:
    Configuration();
    Configuration(const std::string& path);

    /**
      * Read configuration file
      * @param path Path to config file
      */
    void read(const std::string& path);

    /**
      * Get learning rate used for SGD
      * @returns Learning rate
      */
    double get_learning_rate() const;

    /**
      * Get regularization rate
      * @returns Regularization rate
      */
    double get_epsilon() const;

    /**
      * Get size of each minibatch
      * @returns minibatch size
      */
    uint64_t get_minibatch_size() const;

    /**
      * Get size of each object in S3
      * @returns s3 object size
      */
    uint64_t get_s3_size() const;

    /**
      * Get number of classes in the dataset
      * @returns Number of sample classes
      */
    uint64_t get_num_classes() const;

    /**
      * Get path to the input file
      */
    std::string get_load_input_path() const;

    /**
      * Get maximum value of features used by the system
      * When set, the system only reads the first X features from the input datasets
      * @params returns the max number of features to use
      */
    uint64_t get_limit_cols() const;

    /**
      * Get the path to the file with the samples data
      */
    std::string get_samples_path() const;

    /**
      * Get the path to the file with the labels data
      */
    std::string get_labels_path() const;

    /**
      * Get the type of input file used
      */
    std::string get_load_input_type() const;

    /**
      * print the configuration parameters
      */
    void print() const;

    enum ModelType {
        UNKNOWN = 0,
        LOGISTICREGRESSION,
        SOFTMAX,
        COLLABORATIVE_FILTERING,
    };

    /**
      * Get the type of model used by the system
      */
    ModelType get_model_type() const;

    /**
      * Return flag indicating whether to normalize the dataset
      */
    bool get_normalize() const;

    /**
      * Get max number of training samples
      */
    uint64_t get_limit_samples() const;

    /**
      * Get max number of training samples
      */
    uint64_t get_num_features() const;

    std::string get_dataset_format() const;
    std::string get_s3_dataset_key() const;
    std::string get_s3_bucket() const;

    void check() const;

    std::pair<int, int> get_train_range() const;
    std::pair<int, int> get_test_range() const;

    bool get_use_bias() const;

    // threshold to filter out gradient values that are too small
    bool get_grad_threshold_use() const;
    double get_grad_threshold() const;

    uint64_t get_model_bits() const;

    /**
      * Model checkpointing
      */
    uint64_t get_checkpoint_frequency() const;
    std::string get_checkpoint_s3_bucket() const;
    std::string get_checkpoint_s3_keyname() const;


    /**
      * Netflix specific
      */
    int get_users() const;
    int get_items() const;

    std::string get_opt_method() const;
    uint64_t get_netflix_workers() const;

    double get_momentum_beta() const;

 public:
    /**
      * Parse a specific line in the config file
      * @param line Configuration line
      */
    void parse_line(const std::string& line);

    uint64_t n = 0;          //< number of samples
    uint64_t d = 0;          //< number of sample features
    uint64_t n_workers = 0;  //< number of system workers

    uint64_t minibatch_size = 0;  //< size of minibatch
    uint64_t s3_size = 0;  //< size of samples chunk stored in each s3 object

    double learning_rate = 0;     //< sgd learning rate
    double epsilon = 0;           //< regularization rate

    uint64_t num_classes = 0;  //< number of sample classes

    std::string load_input_path;  //< path to dataset input
    std::string load_input_type;  //< dataset input format

    std::string samples_path;  //< path to dataset samples
    std::string labels_path;   //< path to dataset labels

    Configuration::ModelType model_type = UNKNOWN;  //< type of the model

    // max number of columns to read from dataset input
    uint64_t limit_cols = 0;
    bool normalize = false;    //< whether to normalize the dataset

    uint64_t limit_samples = 0;  //< max number of training input samples
    uint64_t num_features = 0;   //< number of features in each sample

    std::string dataset_format;  //< format of the dataset in S3
    std::string s3_dataset_key;  //< key name in the s3 bucket
    std::string s3_bucket_name;  //< bucket used for training dataset

    std::pair<int, int> train_set_range; // range of S3 ids for training
    std::pair<int, int> test_set_range;  // range of S3 ids for testing

    bool use_bias = false; // whether to use bias value for every sample

    // Netflix parameters
    int nusers = 0; // number of users
    int nitems = 0; // number of items

    bool use_grad_threshold = false;
    double grad_threshold = 0;

    std::string opt_method = "adagrad";

    uint64_t model_bits = 20;

    uint64_t netflix_workers = 0;

    uint64_t checkpoint_frequency = 0;  // how often (secs) to checkpoint model
    std::string checkpoint_s3_bucket = "";  // s3 bucket where to store model
    std::string checkpoint_s3_keyname = "";  // s3 key where to store model

    double momentum_beta = 0.0;
};

}  // namespace cirrus

#endif  // _CONFIGURATION_H_
