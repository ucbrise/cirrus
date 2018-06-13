#ifndef _INPUT_H_
#define _INPUT_H_

#include <functional>
#include <Dataset.h>
#include <SparseDataset.h>
#include <Configuration.h>
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <map>
#include <config.h>
#include <MurmurHash3.h>

namespace cirrus {

class InputReader {
  public:
  /**
   * Reads criteo dataset in binary format
   * @param samples_input_file Path to file that contains input samples
   * @param labels_input_file Path to file containing labels
   */
  Dataset read_input_criteo(const std::string& samples_input_file,
      const std::string& labels_input_file);
  
  /**
   * Read movielens dataset
   * @param input_file Path to csv file with dataset
   * @returns The dataset
   */
  SparseDataset read_movielens_ratings(const std::string& input_file,
      int *number_users, int* number_movies);
  
  /**
   * Read netflix dataset
   * @param input_file Path to csv file with dataset
   * @returns The dataset
   */
  SparseDataset read_netflix_ratings(const std::string& input_file,
      int* number_movies, int *number_users);

  void read_netflix_input_thread(
    std::ifstream& fin,
    std::mutex& fin_lock,
    std::mutex& map_lock,
    std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>& sparse_ds,
    int& number_movies,
    int& number_users,
    std::map<int,int>& userid_to_realid,
    int& user_index);

  /**
   * Read dataset in csv file with given delimiter (e.g., tab, space)
   * and specific number of threads
   * We assume labels are the first feature in each line
   * @param input_file Path to csv file with dataset
   * @param delimiter Delimiter string between every feature in the csv file
   * @param nthreads Number of threads to read the csv file
   * @param limit_lines Maximum number of samples to read from file
   * @param limit_cols Maximum number of columns/features to read from file
   * @returns The dataset
   */
  Dataset read_input_csv(const std::string& input_file,
      std::string delimiter, uint64_t nthreads,
      uint64_t limit_lines = 0,
      uint64_t limit_cols = 0,
      bool to_normalize = false);

  /**
   * Read a csv file with the mnist dataset
   * Does not split between samples and labels (see split_data_labels)
   * @param input_file Path to input csv dataset file
   * @param delimiter Delimiter string in the csv
   * @returns the mnist data
   */
  std::vector<std::vector<FEATURE_TYPE>> read_mnist_csv(
      const std::string& input_file,
      std::string delimiter);

  /**
   * Splits dataset vector of FEATURE_TYPE into features and labels
   * @param input Dataset in std::vector<std::vector<FEATURE_TYPE>> format
   * @param label_col Column number where labels are
   * @param training_data Output for samples/features
   * @param labels Output for labels data
   */
  void split_data_labels(const std::vector<std::vector<FEATURE_TYPE>>& input,
      unsigned int label_col,
      std::vector<std::vector<FEATURE_TYPE>>& training_data,
      std::vector<FEATURE_TYPE>& labels);

  /**
   * Normalizes a dataset in the std::vector<std::vector<FEATURE_TYPE>>
   * format in place
   * @param data Dataset
   */
  void normalize(std::vector<std::vector<FEATURE_TYPE>>& data);


  SparseDataset read_input_rcv1_sparse(const std::string& input_file,
      const std::string& delimiter,
      uint64_t limit_lines,
      bool /*to_normalize*/);

  SparseDataset read_input_criteo_sparse(const std::string& input_file,
      const std::string& delimiter,
      const Configuration&);
  
  SparseDataset read_input_criteo_kaggle_sparse(const std::string& input_file,
      const std::string& delimiter,
      const Configuration&);

  private:
  /**
   * Thread worker that reads raw lines of text from queue and appends labels and features
   * into formated samples queue
   * @param input_mutex Mutex used to synchronize access to input data
   * @param output_mutex Mutex used to synch access to output store
   * @param delimiter Delimiter between input data entries
   * @param lines Input lines
   * @param samples Output store for samples
   * @param labels Output store for labels
   * @param terminate Indicate whether threads should terminate
   * @param limit_cols Maximum number of columns/features to read from file
   */
  void read_csv_thread(std::mutex& input_mutex, std::mutex& output_mutex,
      const std::string& delimiter,
      std::queue<std::string>& lines,  //< content produced by producer
      std::vector<std::vector<FEATURE_TYPE>>& samples,
      std::vector<FEATURE_TYPE>& labels,
      bool& terminate,
      uint64_t limit_cols = 0);

  /**
   * Prints a single sample
   * @param sample Sample to be printed
   */
  void print_sample(const std::vector<FEATURE_TYPE>& sample) const;

  void process_lines(
      std::vector<std::string>&,
      const std::string&,
      uint64_t,
      std::vector<std::vector<FEATURE_TYPE>>&,
      std::vector<FEATURE_TYPE>&);

  /* Computes mean of a sparse list of features
   */
  double compute_mean(std::vector<std::pair<int, FEATURE_TYPE>>&);
  
  /* Computes stddev of a sparse list of features
   */
  double compute_stddev(double, std::vector<std::pair<int, FEATURE_TYPE>>&);
  
  /* Computes standardizes sparse dataset
   */
  void standardize_sparse_dataset(std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>&);

  /** Parse a training dataset sample that can contain
   * categorical variables
   */
  void parse_criteo_sparse_line(
      const std::string& line, const std::string& delimiter,
      std::vector<std::pair<int, FEATURE_TYPE>>& features,
      FEATURE_TYPE& label, const Configuration&);

  /** Check if feature is categorical (contains character that is not diigt)
    +    */
  bool is_definitely_categorical(const char* s);

  /** Shuffle both samples and labels
    +    */
  void shuffle_samples_labels(
      std::vector<std::vector<FEATURE_TYPE>>& samples,
      std::vector<FEATURE_TYPE>& labels);

  void read_input_criteo_sparse_thread(std::ifstream& fin, std::mutex& lock,
    const std::string& delimiter,
    std::vector<std::vector<std::pair<int,FEATURE_TYPE>>>& samples_res,
    std::vector<FEATURE_TYPE>& labels_res,
    uint64_t limit_lines, std::atomic<unsigned int>&,
    std::function<void(const std::string&, const std::string&,
      std::vector<std::pair<int, FEATURE_TYPE>>&, FEATURE_TYPE&)> fun);
  
  void read_input_rcv1_sparse_thread(std::ifstream& fin, std::mutex& lock,
    const std::string& delimiter,
    std::vector<std::vector<std::pair<int,FEATURE_TYPE>>>& samples_res,
    std::vector<FEATURE_TYPE>& labels_res,
    uint64_t limit_lines, std::atomic<unsigned int>&);

  void parse_criteo_kaggle_sparse_line(
      const std::string& line, const std::string& delimiter,
      std::vector<std::pair<int, FEATURE_TYPE>>& features,
      FEATURE_TYPE& label, const Configuration&);

  void parse_rcv1_vw_sparse_line(
    const std::string& line, const std::string& delimiter,
    std::vector<std::pair<int, FEATURE_TYPE>>& features,
    FEATURE_TYPE& label);
};

} // namespace cirrus

#endif  // _INPUT_H_
