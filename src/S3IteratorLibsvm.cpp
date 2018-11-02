#include <S3IteratorLibsvm.h>
#include <Utils.h>
#include <unistd.h>
#include <iostream>
#include <vector>

#include <pthread.h>
#include <semaphore.h>

#define FETCH_SIZE (10 * 1024 * 1024)  //  amount of data to fetch each time

//#define DEBUG

// example
// imagine input is in libsvm formta
// <label> <index1>:<value1> <index2>:<value2> ...
// at each iteration we read ~10MB of data

namespace cirrus {

S3IteratorLibsvm::S3IteratorLibsvm(const Configuration& c,
                                   const std::string& s3_bucket,
                                   const std::string& s3_key,
                                   uint64_t file_size,
                                   uint64_t minibatch_rows,
                                   int worker_id,
                                   bool random_access,
                                   bool has_labels)
    : S3Iterator(c, has_labels),
      s3_bucket(s3_bucket),
      s3_key(s3_key),
      file_size(file_size),
      // s3_rows(s3_rows),
      minibatch_rows(minibatch_rows),
      minibatches_list(100000),
      worker_id(worker_id),
      re(worker_id),
      random_access(random_access),
      cur_index(0) {
  std::cout << "S3IteratorLibsvm::Creating S3IteratorLibsvm" << std::endl;

  // initialize s3
  s3_client = std::make_shared<S3Client>();

  for (uint64_t i = 0; i < read_ahead; ++i) {
    pref_sem.signal();
  }

  sem_init(&semaphore, 0, 0);

  thread =
      new std::thread(std::bind(&S3IteratorLibsvm::threadFunction, this, c));

  // we fix the random seed but make it different for every worker
  // to ensure each worker receives a different minibatch
  if (random_access) {
    srand(42 + worker_id);
  }
}

std::shared_ptr<SparseDataset> S3IteratorLibsvm::getNext() {
  sem_wait(&semaphore);
  ring_lock.lock();

  // first discard empty queue
  while (minibatches_list.front().size() == 0) {
    auto queue_ptr = minibatches_list.pop();
  }
  auto ret = minibatches_list.front().back();
  minibatches_list.front().pop_back();
  num_minibatches_ready--;
  ring_lock.unlock();

  // FIXME this should be calculating the local amount of memory
  if (num_minibatches_ready < 200 && pref_sem.getvalue() < (int) read_ahead) {
#ifdef DEBUG
    std::cout << "getNext::pref_sem.signal" << std::endl;
#endif
    pref_sem.signal();
  }

  return ret;
}

/**
 * Moves index forward while data[index] is a space
 * returns true if it ended on a digit, otherwise returns false
 */
bool S3IteratorLibsvm::ignoreSpaces(uint64_t& index, const std::string& data) {
  while (isspace(data[index])) {
    index++;
  }
  return isdigit(data[index]);
}

/**
 * while data[index] is a space character move forward until newline is found
 */
bool S3IteratorLibsvm::ignoreSpacesNotNewline(uint64_t& index,
                                              const std::string& data) {
  while (data[index] != '\n' && isspace(data[index])) {
    index++;
  }
  return isdigit(data[index]);
}

/** This function reads a number value of type T from data
 * and moves index forward in the process
 * Supports decimal values with dots (e.g., 3.14)
 */
template <class T>
T S3IteratorLibsvm::readNum(uint64_t& index, std::string& data) {
  if (!isdigit(data[index])) {
    throw std::runtime_error("Error in the dataset");
  }

  uint64_t index_fw = index;
  // if float or double also accept dots
  if constexpr (std::is_same<T, double>::value ||
                std::is_same<T, float>::value) {
    while (isdigit(data[index_fw]) || data[index_fw] == '.') {
      index_fw++;
    }
  } else {
    while (isdigit(data[index_fw])) {
      index_fw++;
    }
  }

  char c = data[index_fw];
  // need to temporarily add null termination to use sscanf
  data[index_fw] = 0;

  T result;
  int sscanf_ret = 0;
  if constexpr (std::is_same<T, int>::value) {
    sscanf_ret = sscanf(&data[index], "%d", &result);
  } else if constexpr (std::is_same<T, double>::value) {
    sscanf_ret = sscanf(&data[index], "%lf", &result);
  } else if constexpr (std::is_same<T, float>::value) {
    sscanf_ret = sscanf(&data[index], "%f", &result);
  } else if constexpr (std::is_same<T, uint64_t>::value) {
    sscanf_ret = sscanf(&data[index], "%lu", &result);
  } else {
    throw std::runtime_error("Data type not supported");
  }

  if (sscanf_ret == EOF) {
    throw std::runtime_error("Error reading number with sscanf");
  }

  // fix null-termination issue
  data[index_fw] = c;  // repair
  index = index_fw;
  return result;
}

bool S3IteratorLibsvm::buildDatasetCsv(
    const std::string& data,
    uint64_t index,
    std::shared_ptr<SparseDataset>& minibatch) {
  return false;
}

bool S3IteratorLibsvm::buildDatasetVowpalWabbit(
    const std::string& data,
    uint64_t index,
    std::shared_ptr<SparseDataset>& minibatch) {
  return false;
}

/**
 * Build minibatch from text in libsvm format
 * We assume index is at a start of a line
 * index moves forward
 */
bool S3IteratorLibsvm::buildDatasetLibsvm(
    std::string& data,
    uint64_t& index,
    std::shared_ptr<SparseDataset>& minibatch) {
#ifdef DEBUG
  std::cout << "building dataset libsvm index: " << index << std::endl;
#endif
  // libsvm format
  // <label> <index1>:<value1> <index2>:<value2>

  try {
    std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> samples;
    std::vector<FEATURE_TYPE> labels;

    samples.resize(minibatch_rows);
    labels.resize(minibatch_rows);

    for (uint64_t sample = 0; sample < minibatch_rows; ++sample) {
      // ignore spaces
      if (!ignoreSpaces(index, data)) {
#ifdef DEBUG
        std::cout << "ignoreSpaces did not find dig at index: " << index
                  << std::endl;
#endif
        // did not end up in a digit
        if (data[index] == 0) {
          // we found a partially cut sample
          return false;
        } else {
          // there should be a digit here
          throw std::runtime_error("Expecting a digit here");
        }
      }
      int label = readNum<int>(index, data);
#ifdef DEBUG
      std::cout << "read label: " << label << std::endl;
#endif

      // read pairs
      while (1) {
#ifdef DEBUG
        std::cout << "index: " << index << " reading new pair " << std::endl;
#endif
        if (!ignoreSpacesNotNewline(index, data)) {
          if (data[index] == '\n') {
#ifdef DEBUG
            std::cout << "read line index: " << index << std::endl;
#endif
            break;  // move to next sample
          } else if (data[index] == 0) {
#ifdef DEBUG
            std::cout << "end of text index: " << index << std::endl;
#endif
            return false;  // end of text
          } else {
            throw std::runtime_error("Error parsing while reading pairs");
          }
        }
        uint64_t ind = readNum<uint64_t>(index, data);
#ifdef DEBUG
        // std::cout << "index: " << index << " ind: " << ind << std::endl;
#endif
        if (data[index] != ':') {
          return false;
        }
        index++;

#ifdef DEBUG
        std::string num_data = data.substr(index, 10);
        // std::cout << "reading value: " << num_data << std::endl;
#endif

        FEATURE_TYPE value = readNum<FEATURE_TYPE>(index, data);
#ifdef DEBUG
        // std::cout << "index: " << index << " value: " << value << std::endl;
#endif

        samples[sample].push_back(std::make_pair(ind, value));
      }
      labels[sample] = label;
    }

#ifdef DEBUG
    std::cout << "new minibatch" << std::endl;
#endif
    minibatch.reset(new SparseDataset(std::move(samples), std::move(labels)));
    return true;
  } catch (...) {
    // readNum throws exception if it can't find a digit right away
    throw std::runtime_error("Error parsing");
  }
}

void S3IteratorLibsvm::readUntilNewline(uint64_t* index,
                                        const std::string& data) {
#ifdef DEBUG
  std::cout << "reading until new line index: " << (*index) << std::endl;
#endif
  while (1) {
    if (*index >= data.size()) {
      throw std::runtime_error("Error parsing: *index >= data.size()");
    }
    if (data[*index] == '\n') {
      (*index)++;
      break;
    }
    (*index)++;
  }
}

std::vector<std::shared_ptr<SparseDataset>> S3IteratorLibsvm::parseObjLibsvm(
    std::string& data) {
#ifdef DEBUG
  std::cout << "parseObjLibsvm data size: " << data.size() << std::endl;
#endif
  std::vector<std::shared_ptr<SparseDataset>> result;
  // find first sample
  uint64_t index = 0;
  readUntilNewline(&index, data);

  if (index >= data.size()) {
    throw std::runtime_error("Error parsing: index >= data.size()");
  }

  // create minibatches until we ran out of data
  while (1) {
    std::shared_ptr<SparseDataset> minibatch;

    if (!buildDatasetLibsvm(data, index, minibatch)) {
#ifdef DEBUG
      std::cout << "Finished text returning " << result.size() << " minibatches"
                << std::endl;
#endif
      return result;
    }

#ifdef DEBUG
    uint64_t elapsed_us = get_time_us() - start;
    std::cout << "buildDatasetLibsvm elapsed: " << elapsed_us << std::endl;
#endif

    result.push_back(minibatch);
  }
}

void S3IteratorLibsvm::pushSamples(std::shared_ptr<std::ostringstream> oss) {
#ifdef DEBUG
  std::cout << "pushing samples.." << std::endl;
#endif

  // uint64_t n_minibatches = s3_rows / minibatch_rows;

  // we parse this piece of text
  // this returns a collection of minibatches
  auto data = oss->str();
  std::vector<std::shared_ptr<SparseDataset>> dataset = parseObjLibsvm(data);

#ifdef DEBUG
  std::cout << "adding minibatches to ring and setting sem.." << std::endl;
#endif
  ring_lock.lock();
  minibatches_list.add(dataset);
  ring_lock.unlock();
  for (const auto& d : dataset) {
    (void) d;
    num_minibatches_ready++;
    sem_post(&semaphore);
  }
}

static int sstreamSize(std::ostringstream& ss) {
  return ss.tellp();
}

/**
 * Returns a range of bytes (right side is exclusive)
 */
std::pair<uint64_t, uint64_t> S3IteratorLibsvm::getFileRange(
    uint64_t file_size) {
  // given the size of the file we return a random file index
  if (file_size < FETCH_SIZE) {
    // file is small so we get the whole file
    // XXX we should cache file in these cases
    return std::make_pair(0, file_size);
  }

  if (random_access) {
    // we sample the left side of the range
    std::uniform_int_distribution<int> sampler(0, file_size - 1);
    uint64_t left_index = sampler(re);
    if (file_size - left_index < FETCH_SIZE) {
      // make sure we get a range with size FETCH_SIZE
      left_index = file_size - FETCH_SIZE;
    }
    return std::make_pair(left_index, left_index + FETCH_SIZE);
  } else {
#ifdef DEBUG
    std::cout << "cur_index: " << cur_index << " file_size: " << file_size
              << std::endl;
#endif
    if (cur_index >= file_size) {
      // we reached the end
      cur_index = 0;
    }
    // we return <cur, cur + FETCH_SIZE>
    auto ret =
        std::make_pair(cur_index, std::min(cur_index + FETCH_SIZE, file_size));
    cur_index += FETCH_SIZE;
    cur_index = std::min(cur_index, file_size);
    return ret;
  }
}

void S3IteratorLibsvm::reportBandwidth(uint64_t elapsed_us, uint64_t size) {
#ifdef DEBUG
  double mb_s = size / elapsed_us * 1000.0 * 1000 / 1024 / 1024;
  std::cout << "received s3 obj"
            << " elapsed: " << elapsed_us << " size: " << size
            << " BW (MB/s): " << mb_s << "\n";
#endif
}

void S3IteratorLibsvm::threadFunction(const Configuration& config) {
  std::cout << "Building S3 deser. with size: " << std::endl;

  while (1) {
    // if we can go it means there is a slot
    // in the ring
    std::cout << "Waiting for pref_sem" << std::endl;
    pref_sem.wait();

    std::pair<uint64_t, uint64_t> range = getFileRange(file_size);

    std::shared_ptr<std::ostringstream> s3_obj;
  try_start:
    try {
      std::cout << "S3IteratorLibsvm: getting object" << std::endl;
      uint64_t start = get_time_us();

      s3_obj = s3_client->s3_get_object_range_ptr(s3_key, s3_bucket, range);

#ifdef DEBUG
      std::cout << "Read object with size: " << sstreamSize(*s3_obj)
                << std::endl;
#endif

      reportBandwidth(get_time_us() - start, sstreamSize(*s3_obj));
    } catch (...) {
      std::cout << "S3IteratorLibsvm: error in s3_get_object" << std::endl;
      goto try_start;
      exit(-1);
    }
    pushSamples(s3_obj);
  }
}

}  // namespace cirrus
