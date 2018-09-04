/**
  * The Input class is used to aggregate all the routines to read datasets
  * We support datasets in CSV and binary
  * We try to make the process of reading data as efficient as possible
  * Note: this code is still pretty slow reading datasets
  */

#include <InputReader.h>
#include <Utils.h>

#include <string>
#include <vector>
#include <thread>
#include <cassert>
#include <memory>
#include <algorithm>
#include <atomic>
#include <map>
#include <iomanip>
#include <functional>
 
#define DEBUG

namespace cirrus {

static const int REPORT_LINES = 10000;    // how often to report readin progress
static const int REPORT_THREAD = 100000;  // how often proc. threads report
static const int MAX_STR_SIZE = 10000;    // max size for dataset line
static const int RCV1_STR_SIZE = 20000;   // max size for dataset line
static const int JESTER_DEFAULT = 10000;  // default size for Jester dataset

Dataset InputReader::read_input_criteo(const std::string& samples_input_file,
    const std::string& labels_input_file) {
  throw std::runtime_error("No longer supported");

  uint64_t samples_file_size = filesize(samples_input_file);
  uint64_t labels_file_size = filesize(samples_input_file);

  std::cout
    << "Reading samples input file: " << samples_input_file
    << " labels input file: " << labels_input_file
    << " samples file size(MB): " << (samples_file_size / 1024.0 / 1024)
    << std::endl;

  // SPECIFIC of criteo dataset
  uint64_t n_cols = 13;
  uint64_t samples_entries =
    samples_file_size / (sizeof(FEATURE_TYPE) * n_cols);
  uint64_t labels_entries = labels_file_size / (sizeof(FEATURE_TYPE) * n_cols);

  if (samples_entries != labels_entries) {
    puts("Number of sample / labels entries do not match");
    exit(-1);
  }

  std::cout << "Reading " << samples_entries << " entries.." << std::endl;

  FEATURE_TYPE* samples = new FEATURE_TYPE[samples_entries * n_cols];
  FEATURE_TYPE* labels  = new FEATURE_TYPE[samples_entries];

  FILE* samples_file = fopen(samples_input_file.c_str(), "r");
  FILE* labels_file = fopen(labels_input_file.c_str(), "r");

  if (!samples_file || !labels_file) {
    throw std::runtime_error("Error opening input files");
  }

  std::cout
    << " Reading " << sizeof(FEATURE_TYPE) * n_cols
    << " bytes"
    << std::endl;

  uint64_t ret = fread(samples, sizeof(FEATURE_TYPE) * n_cols, samples_entries,
      samples_file);
  if (ret != samples_entries) {
    throw std::runtime_error("Did not read enough data");
  }

  ret = fread(labels, sizeof(FEATURE_TYPE), samples_entries, labels_file);
  if (ret != samples_entries) {
    throw std::runtime_error("Did not read enough data");
  }

  fclose(samples_file);
  fclose(labels_file);

  // we transfer ownership of the samples and labels here
  Dataset ds(samples, labels, samples_entries, n_cols);

  delete[] samples;

  return ds;
}

void InputReader::process_lines(
    std::vector<std::string>& thread_lines,
    const std::string& delimiter,
    uint64_t limit_cols,
    std::vector<std::vector<FEATURE_TYPE>>& thread_samples,
    std::vector<FEATURE_TYPE>& thread_labels) {
  char str[MAX_STR_SIZE];
  while (!thread_lines.empty()) {
    std::string line = thread_lines.back();
    thread_lines.pop_back();
    /*
     * We have the line, now split it into features
     */
    assert(line.size() < MAX_STR_SIZE);
    strncpy(str, line.c_str(), MAX_STR_SIZE - 1);
    char* s = str;

    uint64_t k = 0;
    std::vector<FEATURE_TYPE> sample;
    while (char* l = strsep(&s, delimiter.c_str())) {
      FEATURE_TYPE v = string_to<FEATURE_TYPE>(l);
      sample.push_back(v);
      k++;
      if (limit_cols && k == limit_cols)
        break;
    }

    // we assume first column is label
    FEATURE_TYPE label = sample.front();
    sample.erase(sample.begin());

    thread_labels.push_back(label);
    thread_samples.push_back(sample);
  }
}

void InputReader::read_csv_thread(
    std::mutex& input_mutex, std::mutex& output_mutex,
        const std::string& delimiter,
        std::queue<std::string>& lines,  //< content produced by producer
        std::vector<std::vector<FEATURE_TYPE>>& samples,
        std::vector<FEATURE_TYPE>& labels,
        bool& terminate,
        uint64_t limit_cols) {
  uint64_t count_read = 0;
  uint64_t read_at_a_time = 1000;

  while (1) {
    if (terminate)
      break;

    std::vector<std::string> thread_lines;

    // Read up to read_at_a_time limes
    input_mutex.lock();
    while (lines.size() && thread_lines.size() < read_at_a_time) {
      thread_lines.push_back(lines.front());
      lines.pop();
    }

    if (thread_lines.size() == 0) {
      input_mutex.unlock();
      continue;
    }

    input_mutex.unlock();

    std::vector<std::vector<FEATURE_TYPE>> thread_samples;
    std::vector<FEATURE_TYPE> thread_labels;

    // parses samples in thread_lines
    // and pushes labels and features into
    // thread_samples and thread_labels
    process_lines(thread_lines, delimiter,
        limit_cols, thread_samples, thread_labels);

    output_mutex.lock();
    while (thread_samples.size()) {
      samples.push_back(thread_samples.back());
      labels.push_back(thread_labels.back());
      thread_samples.pop_back();
      thread_labels.pop_back();
    }
    output_mutex.unlock();

    if (count_read % REPORT_THREAD == 0) {
      std::cout << "Thread processed line: " << count_read
        << std::endl;
    }
    count_read += read_at_a_time;
  }
}

void InputReader::print_sample(const std::vector<FEATURE_TYPE>& sample) const {
  for (const auto& v : sample) {
    std::cout << " " << v;
  }
  std::cout << std::endl;
}

std::vector<std::vector<FEATURE_TYPE>>
InputReader::read_mnist_csv(const std::string& input_file,
        std::string delimiter) {
    FILE* fin = fopen(input_file.c_str(), "r");
    if (!fin) {
        throw std::runtime_error("Can't open file: " + input_file);
    }

    std::vector<std::vector<FEATURE_TYPE>> samples;

    std::string line;
    char str[MAX_STR_SIZE + 1] = {0};
    while (fgets(str, MAX_STR_SIZE, fin) != NULL) {
      char* s = str;

      std::vector<FEATURE_TYPE> sample;
      while (char* l = strsep(&s, delimiter.c_str())) {
        FEATURE_TYPE v = string_to<FEATURE_TYPE>(l);
        sample.push_back(v);
      }

      samples.push_back(sample);
    }

    fclose(fin);
    return samples;
}

void InputReader::split_data_labels(
    const std::vector<std::vector<FEATURE_TYPE>>& input,
        unsigned int label_col,
        std::vector<std::vector<FEATURE_TYPE>>& training_data,
        std::vector<FEATURE_TYPE>& labels
        ) {
    if (input.size() == 0) {
        throw std::runtime_error("Error: Input data has 0 columns");
    }

    if (input[0].size() < label_col) {
      throw std::runtime_error("Error: label column is too big");
    }


    // for every sample split it into labels and training data
    for (unsigned int i = 0; i < input.size(); ++i) {
      labels.push_back(input[i][label_col]);  // get label

      std::vector<FEATURE_TYPE> left, right;
      // get all data before label
      left = std::vector<FEATURE_TYPE>(input[i].begin(),
          input[i].begin() + label_col);
      // get all data after label
      right = std::vector<FEATURE_TYPE>(input[i].begin() + label_col + 1,
          input[i].end());

      left.insert(left.end(), right.begin(), right.end());
      training_data.push_back(left);
    }
}

void InputReader::shuffle_samples_labels(
    std::vector<std::vector<FEATURE_TYPE>>& samples,
    std::vector<FEATURE_TYPE>& labels) {
  std::srand(42);
  std::random_shuffle(samples.begin(), samples.end());
  std::srand(42);
  std::random_shuffle(labels.begin(), labels.end());
}

Dataset InputReader::read_input_csv(const std::string& input_file,
        std::string delimiter, uint64_t nthreads,
        uint64_t limit_lines, uint64_t limit_cols,
        bool to_normalize) {
  std::cout << "Reading input file: " << input_file << std::endl;

  std::ifstream fin(input_file, std::ifstream::in);
  if (!fin) {
    throw std::runtime_error("Error opening input file");
  }

  std::vector<std::vector<FEATURE_TYPE>> samples;  // final result
  std::vector<FEATURE_TYPE> labels;         // final result
  std::queue<std::string> lines[nthreads];  // input to threads

  std::mutex input_mutex[nthreads];   // mutex to protect queue of raw samples
  std::mutex output_mutex;  // mutex to protect queue of processed samples
  bool terminate = false;   // indicates when worker threads should terminate
  std::vector<std::shared_ptr<std::thread>> threads;  // vec. of worker threads

  for (uint64_t i = 0; i < nthreads; ++i) {
    threads.push_back(
        std::make_shared<std::thread>(
          /**
           * We could also declare read_csv_thread static and
           * avoid this ugliness
           */
          std::bind(&InputReader::read_csv_thread, this,
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3, std::placeholders::_4,
            std::placeholders::_5, std::placeholders::_6,
            std::placeholders::_7, std::placeholders::_8),
          std::ref(input_mutex[i]), std::ref(output_mutex),
          delimiter, std::ref(lines[i]), std::ref(samples),
          std::ref(labels), std::ref(terminate),
          limit_cols));
  }

  const int batch_size = 100;  // we push things into shared queue in batches
  std::vector<std::string> input;
  input.reserve(batch_size);
  uint64_t lines_count = 0;
  uint64_t thread_index = 0;  // we push input to threads in round robin
  while (1) {
    int i;
    for (i = 0; i < batch_size; ++i, lines_count++) {
      std::string line;
      if (!getline(fin, line))
        break;
      // enforce max number of lines read
      if (lines_count && lines_count >= limit_lines)
        break;
      input[i] = line;
    }
    if (i != batch_size)
      break;

    input_mutex[thread_index].lock();
    for (int j = 0; j < i; ++j) {
      lines[thread_index].push(input[j]);
    }
    input_mutex[thread_index].unlock();

    if (lines_count % REPORT_LINES == 0)
      std::cout << "Read: " << lines_count << " lines." << std::endl;
    thread_index = (thread_index + 1) % nthreads;
  }

  while (1) {
    usleep(100000);
    for (thread_index = 0; thread_index < nthreads; ++thread_index) {
      input_mutex[thread_index].lock();
      // check if a thread is still working
      if (!lines[thread_index].empty()) {
        input_mutex[thread_index].unlock();
        break;
      }
      input_mutex[thread_index].unlock();
    }
    if (thread_index == nthreads)
      break;
  }

  terminate = true;
  for (auto thread : threads)
    thread->join();

  assert(samples.size() == labels.size());

  std::cout << "Read " << samples.size() << " samples" << std::endl;
  std::cout << "Printing first sample" << std::endl;
  print_sample(samples[0]);

  if (to_normalize)
    normalize(samples);

  shuffle_samples_labels(samples, labels);

  std::cout << "Printing first sample after normalization" << std::endl;
  print_sample(samples[0]);
  return Dataset(samples, labels);
}

void InputReader::normalize(std::vector<std::vector<FEATURE_TYPE>>& data) {
  std::vector<FEATURE_TYPE> means(data[0].size());
  std::vector<FEATURE_TYPE> sds(data[0].size());

  // calculate mean of each feature
  for (unsigned int i = 0; i < data.size(); ++i) {  // for each sample
    for (unsigned int j = 0; j < data[0].size(); ++j) {  // for each feature
      means[j] += data[i][j] / data.size();
    }
  }

  // calculate standard deviations
  for (unsigned int i = 0; i < data.size(); ++i) {
    for (unsigned int j = 0; j < data[0].size(); ++j) {
      sds[j] += std::pow(data[i][j] - means[j], 2);
    }
  }
  for (unsigned int j = 0; j < data[0].size(); ++j) {
    sds[j] = std::sqrt(sds[j] / data.size());
  }

  for (unsigned i = 0; i < data.size(); ++i) {
    for (unsigned int j = 0; j < data[0].size(); ++j) {
      if (means[j] != 0) {
        data[i][j] = (data[i][j] - means[j]) / sds[j];
      }
      if (std::isnan(data[i][j]) || std::isinf(data[i][j])) {
        std::cout << data[i][j] << " " << means[j]
          << " " << sds[j]
          << std::endl;
        throw std::runtime_error(
            "Value is not valid after normalization");
      }
    }
  }
}

/**
  * MovieLens 20M
  * Size: 50732 users *  131262 ratings
  * Format
  * userId, movieId, rating, timestamp
  */
SparseDataset InputReader::read_movielens_ratings(const std::string& input_file,
   int *number_users, int* number_movies) {
  std::ifstream fin(input_file, std::ifstream::in);
  if (!fin) {
    throw std::runtime_error("Error opening input file " + input_file);
  }

  *number_movies = *number_users = 0;

  std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> sparse_ds;

  // XXX Fix
  sparse_ds.resize(50732);

  std::string line;
  getline(fin, line); // read the header 
  while (getline(fin, line)) {
    char str[MAX_STR_SIZE];
    assert(line.size() < MAX_STR_SIZE - 1);
    strncpy(str, line.c_str(), MAX_STR_SIZE - 1);

    char* s = str;
    char* l = strsep(&s, ",");
    int userId = string_to<int>(l);
    l = strsep(&s, ",");
    int movieId = string_to<int>(l);
    l = strsep(&s, ",");
    FEATURE_TYPE rating = string_to<FEATURE_TYPE>(l);

    sparse_ds[userId - 1].push_back(std::make_pair(movieId, rating));

    *number_users = std::max(*number_users, userId);
    *number_movies = std::max(*number_movies, movieId);
  }

  return SparseDataset(sparse_ds);
}

// TODO: Consolidate with read MovieLens function.
SparseDataset InputReader::read_jester_ratings(const std::string& input_file,
                                               int* number_users,
                                               int* number_jokes) {
  std::ifstream fin(input_file, std::ifstream::in);
  if (!fin) {
    throw std::runtime_error("Error opening input file " + input_file);
  }

  *number_jokes = *number_users = 0;

  std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> sparse_ds;

  sparse_ds.resize(JESTER_DEFAULT);

  std::string line;
  while (getline(fin, line)) {
    char str[MAX_STR_SIZE];
    // Ignore empty or malformed lines.
    if (line.size() < 3)
      continue;
    assert(line.size() < MAX_STR_SIZE - 1);
    strncpy(str, line.c_str(), MAX_STR_SIZE - 1);
    char* s = str;
    char* l = strsep(&s, ",");
    int userId = string_to<int>(l);
    l = strsep(&s, ",");
    int jokeId = string_to<int>(l);
    l = strsep(&s, ",");
    FEATURE_TYPE rating = string_to<FEATURE_TYPE>(l);

    sparse_ds[userId].push_back(std::make_pair(jokeId, rating));

    *number_users = std::max(*number_users, userId);
    *number_jokes = std::max(*number_jokes, jokeId);
  }

  return SparseDataset(sparse_ds);
}

double InputReader::compute_mean(std::vector<std::pair<int, FEATURE_TYPE>>& user_ratings) {
  double mean = 0;

  for (auto& r : user_ratings) {
    mean += r.second;
  }
  mean /= user_ratings.size();

  return mean;
}

double InputReader::compute_stddev(double mean, std::vector<std::pair<int, FEATURE_TYPE>>& user_ratings) {
  double stddev = 0;

  for (const auto& r : user_ratings) {
    stddev += (mean - r.second) * (mean - r.second);
  }
  stddev /= user_ratings.size();
  stddev = std::sqrt(stddev);

  return stddev;
}

void InputReader::standardize_sparse_dataset(std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>& sparse_ds) {
  // for every user we compute the mean and stddev of his ratings
  // then we normalize each entry
  for (auto& sample : sparse_ds) {
    if (sample.size() == 0)
      continue;

    double mean = compute_mean(sample);
    double stddev = compute_stddev(mean, sample);

    // check if all ratings of an user have same value
    // if so we 'discard' this user
    if (stddev == 0.0) {
      //throw std::runtime_error("I think this is not well implemented");
      sample.clear();
      continue;
    }
     
#ifdef DEBUG 
    if (std::isnan(mean) || std::isinf(mean))
      throw std::runtime_error("wrong mean");
    if (std::isnan(stddev) || std::isinf(stddev))
      throw std::runtime_error("wrong stddev");
#endif

    for (auto& v : sample) {
      if (stddev) {
        v.second = (v.second - mean) / stddev;
      } else {
        v.second = mean;
      }
#ifdef DEBUG 
      if (std::isnan(v.second) || std::isinf(v.second)) {
        std::cout 
          << "mean: " << mean
          << " stddev: " << stddev
          << std::endl;
        throw std::runtime_error("wrong rating");
      }
#endif
    }
  }
}



bool InputReader::is_definitely_categorical(const char* s) {
  for (uint64_t i = 0; s[i]; ++i) {
    if (!isdigit(s[i]) && s[i]!='-') {
      return true;
    }
  }
  return false;
}

/**
 * Parse a line from the training dataset
 * containg numerical and/or categorical variables
 */
void InputReader::parse_criteo_sparse_line(
    const std::string& line,
    const std::string& delimiter,
    std::vector<std::pair<int, FEATURE_TYPE>>& output_features,
    FEATURE_TYPE& label,
    const Configuration& config) {
  char str[MAX_STR_SIZE];

  if (line.size() > MAX_STR_SIZE) {
    throw std::runtime_error(
        "Criteo input line is too big: " + std::to_string(line.size()) + " " +
        std::to_string(MAX_STR_SIZE));
  }

  strncpy(str, line.c_str(), MAX_STR_SIZE - 1);
  char* s = str;

  std::map<uint64_t, int> features;

  uint64_t hash_size = (1ULL << config.get_model_bits());

  uint64_t col = 0;
  while (char* l = strsep(&s, delimiter.c_str())) {
    if (col == 0) { // it's label
      label = string_to<FEATURE_TYPE>(l);
    } else if (col >= 14) {
      //assert(is_categorical(l) || std::string(l).size() == 0);
      uint64_t hash = hash_f(l) % hash_size;
      features[hash]++;
    }
    col++;
  }

  for (const auto& feat : features) {
    if (feat.second != 0.0) {
      output_features.push_back(std::make_pair(feat.first, feat.second));
    }
  }
}

void InputReader::read_input_criteo_sparse_thread(std::ifstream& fin, std::mutex& fin_lock,
    const std::string& delimiter,
    std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>& samples_res,
    std::vector<FEATURE_TYPE>& labels_res,
    uint64_t limit_lines, std::atomic<unsigned int>& lines_count,
    std::function<void(const std::string&, const std::string&,
      std::vector<std::pair<int, FEATURE_TYPE>>&, FEATURE_TYPE&)> fun) {
  std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> samples;  // final result
  std::vector<FEATURE_TYPE> labels;                                // final result
  std::string line;
  uint64_t lines_count_thread = 0;
  while (1) {
    fin_lock.lock();
    getline(fin, line);
    fin_lock.unlock();

    // break if we reach end of file
    if (fin.eof())
      break;

    // enforce max number of lines read
    if (lines_count && lines_count >= limit_lines)
      break;

    FEATURE_TYPE label;
    std::vector<std::pair<int, FEATURE_TYPE>> features;
    fun(line, delimiter, features, label);

    samples.push_back(features);
    labels.push_back(label);

    if (lines_count % 100000 == 0) {
      std::cout << "Read: " << lines_count << "/" << lines_count_thread << " lines." << std::endl;
    }
    ++lines_count;
    lines_count_thread++;
  }

  fin_lock.lock(); // XXX fix this
  for (const auto& l : labels) {
    labels_res.push_back(l);
  }
  for (const auto& s : samples) {
    samples_res.push_back(s);
  }
  fin_lock.unlock();
}

/** Handle both numerical and categorical variables
    * For categorical variables we use the hashing trick
    */
SparseDataset InputReader::read_input_criteo_sparse(const std::string& input_file,
    const std::string& delimiter,
    const Configuration& config) {
  std::cout << "Reading input file: " << input_file << std::endl;
  std::cout << "Limit_line: " << config.get_limit_samples() << std::endl;

  std::ifstream fin(input_file, std::ifstream::in);
  if (!fin) {
    throw std::runtime_error("Error opening input file");
  }
  std::mutex fin_lock;
  std::atomic<unsigned int> lines_count(0);
  std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> samples;  // final result
  std::vector<FEATURE_TYPE> labels;                                // final result

  std::vector<std::shared_ptr<std::thread>> threads;
  uint64_t nthreads = 8;
  for (uint64_t i = 0; i < nthreads; ++i) {
    threads.push_back(
        std::make_shared<std::thread>(
          std::bind(&InputReader::read_input_criteo_sparse_thread, this,
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3, std::placeholders::_4,
            std::placeholders::_5, std::placeholders::_6,
            std::placeholders::_7, std::placeholders::_8),
          std::ref(fin), std::ref(fin_lock),
          std::ref(delimiter), std::ref(samples),
          std::ref(labels), config.get_limit_samples(), std::ref(lines_count),
          std::bind(&InputReader::parse_criteo_sparse_line, this, 
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3,
            std::placeholders::_4,
            config)
          ));
  }

  for (auto& t : threads) {
    t->join();
  }

  // process each line
  std::cout << "Read a total of " << labels.size() << " samples" << std::endl;

  SparseDataset ret(std::move(samples), std::move(labels));
  if (config.get_normalize()) {
    // pass hash size
    ret.normalize( (1 << config.get_model_bits()) );
  }
  return ret;
}

void pack_label(FEATURE_TYPE& label, const std::vector<int>& classes) {
  char* ptr = (char*)&label;
  memset(&label, 0, sizeof(FEATURE_TYPE));
  for (uint64_t i = 0; i < std::min(classes.size(), sizeof(FEATURE_TYPE)); ++i) {
    *ptr = (char)classes[i];
    ptr++;
  }
}

std::vector<int> unpack_label(const FEATURE_TYPE& label) {
  std::vector<int> res;
  char* ptr = (char*)&label;
  for (uint64_t i = 0; i < sizeof(FEATURE_TYPE); ++i) {
    if (*ptr == 0)
      break;
    res.push_back((int)*ptr);
    ++ptr;
  }
  return res;
}

/**
 * Parse a line from the training dataset
//9,68 | 33:1.000000 47:1.000000 94:1.000000 104:1.000000 112:3.000000 118:1.000000 141:2.000000 165:2.000000 179:1.000000 251:1.000000 270:1.000000 306:1.000000 307:1.000000 424:1.000000 497:1.000000 529:1.000000 573:1.000000 601:1.000000 626:2.000000 678:2.000000 707:2.000000 710:1.000000 716:3.000000 722:1.000000 773:1.000000 914:1.000000 933:4.000000 1052:1.000000 1067:4.000000 1434:1.000000 1491:1.000000 1586:1.000000 1640:4.000000 1855:1.000000 2674:1.000000 3289:1.000000 3664:1.000000 3806:1.000000 3869:1.000000 4224:1.000000 4346:1.000000 4831:1.000000 15046:1.000000 15688:1.000000 16572:1.000000 29352:1.000000
 */
char rcv1_line[RCV1_STR_SIZE];
void InputReader::parse_rcv1_vw_sparse_line(
    const std::string& line, const std::string& delimiter,
    std::vector<std::pair<int, FEATURE_TYPE>>& features,
    FEATURE_TYPE& label) {

  if (line.size() > RCV1_STR_SIZE) {
    throw std::runtime_error(
        "rcv1 input line is too big: " +
        std::to_string(line.size()) + " " + std::to_string(RCV1_STR_SIZE));
  }

  //static int static_count = 0;
  //std::cout << "Parsing line: " << ++static_count << std::endl;
  strncpy(rcv1_line, line.c_str(), RCV1_STR_SIZE - 1);
  char* s = rcv1_line;

  std::vector<FEATURE_TYPE> num_features; // numerical features
  std::map<uint64_t, int> cat_features;

  uint64_t col = 0;
  while (char* l = strsep(&s, delimiter.c_str())) {
    if (col == 0) { // the label
      std::string label_str(l);
      char str2[RCV1_STR_SIZE];
      strncpy(str2, label_str.c_str(), RCV1_STR_SIZE - 1);
      char* s = str2;
      //std::cout << "label: " << s << std::endl;
      std::vector<int> classes;
      while (char* c = strsep(&s, ",")) {
        classes.push_back(string_to<int>(c) + 1); // we do this to handle class 0
        //std::cout << "class: " << c << std::endl;
      }
      pack_label(label, classes);
#ifdef DEBUG
      std::vector<int> cs = unpack_label(label);
      std::sort(cs.begin(), cs.end());
      std::sort(classes.begin(), classes.end());
      if (cs != classes) {
        if (classes.size() <= sizeof(FEATURE_TYPE)) {
          for (const auto& v : cs) {
            std::cout << "c1: " << v << std::endl;
          }
          for (const auto& v : classes) {
            std::cout << "c2: " << v << std::endl;
          }
        }
      }
      //std::cout << "classes " << static_count++ << " :";
      for (uint64_t i = 0; i < cs.size(); ++i) {//const auto& v : cs) {
        std::cout << (i ? ",":"") << (cs[i]-1);
      }
      std::cout << " |";
#endif
    } else if (col == 1) { // it's the bar between labels and features
    } else {
      std::string feat(l);
      size_t i = feat.find(":");
      std::string index = feat.substr(0, i);
      std::string value = feat.substr(i+1);
      //std::cout << "index: " << index << " value: " << value << std::endl;
      uint64_t hash = string_to<uint64_t>(index);
      cat_features[hash] += string_to<uint64_t>(value);
    }
    col++;
  }

    std::cout.precision(6);
  for (const auto& feat : cat_features) {
    int index = feat.first;
    FEATURE_TYPE value = feat.second;
    features.push_back(std::make_pair(index, value));
    std::cout << std::fixed << " " << index << ":" << value;
  }
  std::cout << "\n";
}

SparseDataset InputReader::read_input_rcv1_sparse(const std::string& input_file,
    const std::string& delimiter,
    uint64_t limit_lines,
    bool to_normalize) {
  std::cout << "Reading RCV1 input file: " << input_file << std::endl;

  std::ifstream fin(input_file, std::ifstream::in);
  if (!fin) {
    throw std::runtime_error("Error opening input file");
  }
  std::mutex fin_lock;
  std::atomic<unsigned int> lines_count(0);
  std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> samples;  // final result
  std::vector<FEATURE_TYPE> labels;                                // final result

  std::vector<std::shared_ptr<std::thread>> threads;
  uint64_t nthreads = 1;
  for (uint64_t i = 0; i < nthreads; ++i) {
    threads.push_back(
        std::make_shared<std::thread>(
          std::bind(&InputReader::read_input_criteo_sparse_thread, this,
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3, std::placeholders::_4,
            std::placeholders::_5, std::placeholders::_6,
            std::placeholders::_7, std::placeholders::_8),
          std::ref(fin), std::ref(fin_lock),
          std::ref(delimiter), std::ref(samples),
          std::ref(labels), limit_lines, std::ref(lines_count),
          std::bind(&InputReader::parse_rcv1_vw_sparse_line, this, 
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3,
            std::placeholders::_4)
          ));
  }

  for (auto& t : threads) {
    t->join();
  }

  // process each line
  std::cout << "Read RCV1 a total of " << labels.size() << " samples" << std::endl;

  SparseDataset ret(std::move(samples), std::move(labels));
  if (to_normalize) {
    // pass hash size
    ret.normalize( (1 << RCV1_HASH_BITS) );
  }
  return ret;
}

/**
 * Parse a line from the training dataset
 * Id,Label,I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,I11,I12,I13,C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13,C14,C15,C16,C17,C18,C19,C20,C21,C22,C23,C24,C25,C26
 */
void InputReader::parse_criteo_kaggle_sparse_line(
    const std::string& line, const std::string& delimiter,
    std::vector<std::pair<int, FEATURE_TYPE>>& output_features,
    FEATURE_TYPE& label, const Configuration& config) {
  char str[MAX_STR_SIZE];

  //std::cout << "line: " << line << std::endl;

  if (line.size() > MAX_STR_SIZE) {
    throw std::runtime_error(
        "Criteo input line is too big: " + std::to_string(line.size()) + " " +
        std::to_string(MAX_STR_SIZE));
  }

  strncpy(str, line.c_str(), MAX_STR_SIZE - 1);
  char* s = str;

  std::map<uint64_t, int> features;

  uint64_t hash_size = (1ULL << config.get_model_bits());

  uint64_t col = 0;
  while (char* l = strsep(&s, delimiter.c_str())) {
    if (col == 0 ) { // it's Id
    } else if (col == 1) { // it's label
      label = string_to<FEATURE_TYPE>(l);
      assert(label == 0.0 || label == 1.0);
    } else {
      uint64_t hash = hash_f(l) % hash_size;
      features[hash]++;
    }
    col++;
  }
  
  if (config.get_use_bias()) { // add bias constant
    uint64_t hash = hash_f("bias") % hash_size;
    features[hash]++;
  }

  /**
    */
  for (const auto& feat : features) {
    if (feat.second != 0.0) {
      output_features.push_back(std::make_pair(feat.first, feat.second));
    }
  }
}

SparseDataset InputReader::read_input_criteo_kaggle_sparse(
    const std::string& input_file,
    const std::string& delimiter,
    const Configuration& config) {
  std::cout << "Reading criteo kaggle sparse input file: " << input_file << std::endl;
  std::cout << "Limit_line: " << config.get_limit_samples() << std::endl;

  assert(delimiter == ",");

  std::ifstream fin(input_file, std::ifstream::in);
  if (!fin) {
    throw std::runtime_error("Error opening input file");
  }

  // we read the first line because it contains the header
  std::string line;
  getline(fin, line);

  std::mutex fin_lock;
  std::atomic<unsigned int> lines_count(0);
  std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> samples;  // final result
  std::vector<FEATURE_TYPE> labels;                                // final result

  std::vector<std::shared_ptr<std::thread>> threads;
  uint64_t nthreads = 8;
  for (uint64_t i = 0; i < nthreads; ++i) {
    threads.push_back(
        std::make_shared<std::thread>(
          std::bind(&InputReader::read_input_criteo_sparse_thread, this,
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3, std::placeholders::_4,
            std::placeholders::_5, std::placeholders::_6,
            std::placeholders::_7, std::placeholders::_8),
          std::ref(fin), std::ref(fin_lock),
          std::ref(delimiter), std::ref(samples),
          std::ref(labels), config.get_limit_samples(), std::ref(lines_count),
          std::bind(&InputReader::parse_criteo_kaggle_sparse_line, this, 
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3,
            std::placeholders::_4,
            config)
          ));
  }

  for (auto& t : threads) {
    t->join();
  }

  // process each line
  std::cout << "Read a total of " << labels.size() << " samples" << std::endl;

  SparseDataset ret(std::move(samples), std::move(labels));
  if (config.get_normalize()) {
    // pass hash size
    ret.normalize( (1 << config.get_model_bits()) );
  }
  return ret;
}

/****************************************
  ***************************************
  ******** NETFLIX **********************
  ***************************************
  ***************************************
  */

void InputReader::read_netflix_input_thread(
    std::ifstream& fin,
    std::mutex& fin_lock,
    std::mutex& map_lock,
    std::vector<std::vector<std::pair<int, FEATURE_TYPE>>>& sparse_ds,
    int& number_movies,
    int& number_users,
    std::map<int,int>& userid_to_realid,
    int& user_index) {
  //getline(fin, line); // read the header 
  std::string line;
  int nummovies_local = 0, numusers_local = 0;
  while (1) {
    fin_lock.lock();
    getline(fin, line);
    if (fin.eof()) {
      fin_lock.unlock();
      break;
    }
    fin_lock.unlock();

    char str[MAX_STR_SIZE];
    assert(line.size() < MAX_STR_SIZE);
    strncpy(str, line.c_str(), MAX_STR_SIZE - 1);

    char* s = str;
    char* l = strsep(&s, ",");
    int userId = string_to<int>(l);
    l = strsep(&s, ",");
    int movieId = string_to<int>(l);
    l = strsep(&s, ",");
    FEATURE_TYPE rating = string_to<FEATURE_TYPE>(l);

    //std::cout
    //  << " line: " << line
    //  << " userId: " << userId
    //  << std::endl;
    map_lock.lock();
    auto iter = userid_to_realid.find(userId);
    int newuser_id = 0;
    if (iter == userid_to_realid.end()) {
      // first time seeing this user
      newuser_id = user_index;
      userid_to_realid[userId] = user_index++;
    } else {
      newuser_id = iter->second;
    }
    sparse_ds.at(newuser_id).push_back(std::make_pair(movieId - 1, rating));
    map_lock.unlock();

    numusers_local = std::max(numusers_local, newuser_id);
    nummovies_local = std::max(nummovies_local, movieId);
  }
  
  fin_lock.lock();
  number_users = std::max(number_users, numusers_local);
  number_movies = std::max(number_movies, nummovies_local);
  fin_lock.unlock();
}

/**
  * Format
  * userId, movieId, rating
  */
SparseDataset InputReader::read_netflix_ratings(const std::string& input_file,
   int* number_movies, int* number_users) {
  std::ifstream fin(input_file, std::ifstream::in);
  if (!fin) {
    throw std::runtime_error("Error opening input file " + input_file);
  }

  // we use this map to map ids in the dataset (these have gaps)
  // to a continuous range (without gaps)
  std::map<int, int> userid_to_realid;
  int user_index = 0;

  *number_movies = *number_users = 0;

  std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> sparse_ds; // result dataset
  
  // CustomerIDs range from 1 to 2649429, with gaps. There are 480189 users.
  sparse_ds.resize(480189); // we assume we read the whole dataset
  //sparse_ds.resize(2649430); // we assume we read the whole dataset
  
  std::vector<std::shared_ptr<std::thread>> threads; // container for threads
  uint64_t nthreads = 8; //  number of threads to use
  std::mutex fin_lock; // lock to access input file
  std::mutex map_lock; // lock to access map
  for (uint64_t i = 0; i < nthreads; ++i) {
    threads.push_back(
        std::make_shared<std::thread>(
          std::bind(&InputReader::read_netflix_input_thread, this,
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3, std::placeholders::_4,
            std::placeholders::_5, std::placeholders::_6,
            std::placeholders::_7, std::placeholders::_8),
          std::ref(fin),
          std::ref(fin_lock),
          std::ref(map_lock),
          std::ref(sparse_ds),
          std::ref(*number_movies),
          std::ref(*number_users),
          std::ref(userid_to_realid),
          std::ref(user_index)
          ));
  }

  for (auto& t : threads) {
    t->join();
  }

  // we standardize the dataset
  // standardize_sparse_dataset(sparse_ds);

  auto ds = SparseDataset(sparse_ds);
  std::cout << "Checking sparse dataset" << std::endl;
  ds.check();
  std::cout << "Checking sparse dataset done" << std::endl;

  for (int i = 0; i < 100; ++i) {
    auto sample = ds.get_row(i);
    std::cout << "sample " << i << " size: " << sample.size() << std::endl;
  }

  return ds;
}

} // namespace cirrus

