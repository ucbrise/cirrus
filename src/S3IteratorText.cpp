#include <S3IteratorText.h>
#include <Utils.h>
#include <unistd.h>
#include <vector>
#include <iostream>

#include <pthread.h>
#include <semaphore.h>

#define FETCH_SIZE (10 * 1024 * 1024) //  size we try to fetch at a time

//#define DEBUG

// example
// imagine input is in libsvm formta
// <label> <index1>:<value1> <index2>:<value2> ...
// at each iteration we read ~10MB of data

namespace cirrus {
  
S3IteratorText::S3IteratorText(
        const Configuration& c,
        // FIXME we should pass the filename and bucket instead
        // FIXME we should figure out file_size from that
        uint64_t file_size,
        uint64_t minibatch_rows,
        int worker_id,
        bool random_access) :
  S3Iterator(c),
  file_size(file_size),
  s3_rows(s3_rows),
  minibatch_rows(minibatch_rows),
  minibatches_list(100000),
  worker_id(worker_id),
  re(worker_id),
  random_access(random_access),
  cur_index(0)
{
      
  std::cout << "S3IteratorText::Creating S3IteratorText"
    << std::endl;

  // initialize s3
  s3_client = std::make_shared<S3Client>();

  for (uint64_t i = 0; i < read_ahead; ++i) {
    pref_sem.signal();
  }

  sem_init(&semaphore, 0, 0);

  thread = new std::thread(std::bind(&S3IteratorText::thread_function, this, c));

  // we fix the random seed but make it different for every worker
  // to ensure each worker receives a different minibatch
  if (random_access) {
    srand(42 + worker_id);
  }
}

std::shared_ptr<SparseDataset> S3IteratorText::get_next_fast() {
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
  if (num_minibatches_ready < 200 && pref_sem.getvalue() < (int)read_ahead) {
#ifdef DEBUG
    std::cout << "get_next_fast::pref_sem.signal" << std::endl;
#endif
    pref_sem.signal();
  }

  return ret;
}

/**
  * Moves index forward while data[index] is a space
  * returns true if it ended on a digit, otherwise returns false
  */
bool ignore_spaces(uint64_t& index, const std::string& data) {
  while (data[index] && data[index] == ' ') {
    index++;
  }
  return isdigit(data[index]);
}

template <class T>
T S3IteratorText::read_num(uint64_t& index, std::string& data) {
  if (!isdigit(data[index])) {
    throw std::runtime_error("Error in the dataset");
  }
  
  uint64_t index_fw = index;
  while (isdigit(data[index_fw])) {
    index_fw++;
  }

  char c = data[index_fw];
  data[index_fw] = 0;

  T result;
  if constexpr (std::is_same<T, int>::value) {
      sscanf(&data[index], "%d", &result);
  } else if constexpr (std::is_same<T, double>::value) {
      sscanf(&data[index], "%lf", &result);
  } else if constexpr (std::is_same<T, float>::value) {
      sscanf(&data[index], "%f", &result);
  } else if constexpr (std::is_same<T, uint64_t>::value) { 
      sscanf(&data[index], "%lu", &result);
  } else {
    throw std::runtime_error("Data type not supported");
  }

  data[index_fw] = c; // repair
  index = index_fw;
  return result;
}

bool S3IteratorText::build_dataset_csv(
    const std::string& data, uint64_t index,
    std::shared_ptr<SparseDataset>& minibatch) {
    return false;
}

bool S3IteratorText::build_dataset_vowpal_wabbit(
    const std::string& data, uint64_t index,
    std::shared_ptr<SparseDataset>& minibatch) {
    return false;
}

/**
  * Build minibatch from text in libsvm format
  * We assume index is at a start of a line
  */
bool S3IteratorText::build_dataset_libsvm(
    std::string& data, uint64_t index,
    std::shared_ptr<SparseDataset>& minibatch) {
  // libsvm format
  // <label> <index1>:<value1> <index2>:<value2>

  try {
    std::vector<std::vector<std::pair<int, FEATURE_TYPE>>> samples;
    std::vector<FEATURE_TYPE> labels;

    samples.resize(minibatch_rows);
    labels.resize(minibatch_rows);

    for (uint64_t sample = 0; sample < minibatch_rows; ++sample) {
      // ignore spaces
      if (!ignore_spaces(index, data)) {
        // did not end up in a digit
        return false;
      }
      int label = read_num<int>(index, data);

      // read pairs
      while (1) {
        if (!ignore_spaces(index, data)) {
          if (data[index] == '\n') break; // move to next sample
          else if (data[index] == 0) return false; // end of text
          else throw std::runtime_error("Error parsing");
        }
        uint64_t ind = read_num<uint64_t>(index, data);
        if (data[index] != ':') {
          return false;
        }
        index++;
        FEATURE_TYPE value = read_num<FEATURE_TYPE>(index, data);

        samples[sample].push_back(std::make_pair(ind, value));
      }
      labels[sample] = label;
    }

    minibatch.reset(new SparseDataset(std::move(samples), std::move(labels)));
    return true;
  } catch (...) {
    // read_num throws exception if it can't find a digit right away
    return false;
  }
}

void S3IteratorText::read_until_newline(uint64_t* index, const std::string& data) {
  while (1) {
    if (*index >= data.size()) {
      throw std::runtime_error("Error parsing");
    }
    if (data[*index] == '\n') {
      (*index)++;
      break;
    }
    (*index)++;
  }
}

std::vector<std::shared_ptr<SparseDataset>>
S3IteratorText::parse_obj_libsvm(std::string& data) {
  std::vector<std::shared_ptr<SparseDataset>> result;
  // find first sample
  uint64_t index = 0;
  read_until_newline(&index, data);

  if (index >= data.size()) {
    throw std::runtime_error("Error parsing data");
  }

  while (1) {
    std::shared_ptr<SparseDataset> minibatch;
    if (!build_dataset_libsvm(data, index, minibatch)) {
      // could not build full minibatch
      return result;
    }
    result.push_back(minibatch);
  }
}

void S3IteratorText::push_samples(std::ostringstream* oss) {
  uint64_t n_minibatches = s3_rows / minibatch_rows;

  // we parse this piece of text
  // this returns a collection of minibatches
  auto data = oss->str();
  std::vector<std::shared_ptr<SparseDataset>> dataset =
    parse_obj_libsvm(data);

  ring_lock.lock();
  minibatches_list.add(dataset);
  ring_lock.unlock();
  for (uint64_t i = 0; i < n_minibatches; ++i) {
    num_minibatches_ready++;
    sem_post(&semaphore);
  }
}

static int sstream_size(std::ostringstream& ss) {
  return ss.tellp();
}

/**
  * Returns a range of bytes (right side is exclusive)
  */
std::pair<uint64_t, uint64_t>
S3IteratorText::get_file_range(uint64_t file_size) {
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
    if (cur_index >= file_size) {
      // we reached the end
      cur_index = 0;
    }
    // we return <cur, cur + FETCH_SIZE>
    auto ret = std::make_pair(cur_index, std::min(cur_index + FETCH_SIZE, file_size));
    cur_index += FETCH_SIZE;
    cur_index = std::min(cur_index, file_size);
    return ret;
  }
}

void S3IteratorText::report_bandwidth(uint64_t elapsed, uint64_t size) {
#if 0
  uint64_t elapsed_us = (get_time_us() - start);
  double mb_s = sstream_size(*s3_obj) / elapsed_us
    * 1000.0 * 1000 / 1024 / 1024;
  std::cout << "received s3 obj"
    << " elapsed: " << elapsed_us
    << " size: " << sstream_size(*s3_obj)
    << " BW (MB/s): " << mb_s
    << "\n";
#endif
}

void S3IteratorText::thread_function(const Configuration& config) {
  std::cout << "Building S3 deser. with size: "
    << std::endl;

  uint64_t count = 0;
  while (1) {
    // if we can go it means there is a slot
    // in the ring
    std::cout << "Waiting for pref_sem" << std::endl;
    pref_sem.wait();

    std::pair<uint64_t, uint64_t> range = get_file_range(file_size);

    std::ostringstream* s3_obj = nullptr;
try_start:
    try {
      std::cout << "S3IteratorText: getting object" << std::endl;
      uint64_t start = get_time_us();

      s3_obj = s3_client->s3_get_object_range_ptr(
          config.get_s3_dataset_key(),
          config.get_s3_bucket(), range);

      report_bandwidth(get_time_us() - start, sstream_size(*s3_obj));
    } catch(...) {
      std::cout
        << "S3IteratorText: error in s3_get_object"
        << std::endl;
      goto try_start;
      exit(-1);
    }
    push_samples(s3_obj);
  }
}

} // namespace cirrus

