#include <SparseLRModel.h>
#include <Utils.h>
#include <MlUtils.h>
#include <Eigen/Dense>
#include <Checksum.h>
#include <algorithm>
#include <map>
#include <unordered_map>

//#define DEBUG

namespace cirrus {

SparseLRModel::SparseLRModel(uint64_t d) {
    weights_.resize(d);
    weights_hist_.resize(d);
}

SparseLRModel::SparseLRModel(const FEATURE_TYPE* w, uint64_t d) {
    weights_.resize(d);
    weights_hist_.resize(d);
    std::copy(w, w + d, weights_.begin());
}

uint64_t SparseLRModel::size() const {
  return weights_.size();
}

/**
  * Serialization / deserialization routines
  */

/** FORMAT
  * weights
  */
std::unique_ptr<CirrusModel> SparseLRModel::deserialize(void* data, uint64_t size) const {
  throw std::runtime_error("not supported");
  uint64_t d = size / sizeof(FEATURE_TYPE);
  std::unique_ptr<SparseLRModel> model = std::make_unique<SparseLRModel>(
      reinterpret_cast<FEATURE_TYPE*>(data), d);
  return model;
}

std::pair<std::unique_ptr<char[]>, uint64_t>
SparseLRModel::serialize() const {
    throw std::runtime_error("Fix. Not implemented1");
    std::pair<std::unique_ptr<char[]>, uint64_t> res;
    uint64_t size = getSerializedSize();
    res.first.reset(new char[size]);

    res.second = size;
    std::memcpy(res.first.get(), weights_.data(), getSerializedSize());

    return res;
}

void SparseLRModel::serializeTo(void* mem) const {
#ifdef DEBUG 
  //std::cout << "Num weights size: " << weights_.size() << std::endl;
#endif
  store_value<int>(mem, weights_.size());
  std::copy(weights_.data(), weights_.data() + weights_.size(),
      reinterpret_cast<FEATURE_TYPE*>(mem));
}

uint64_t SparseLRModel::getSerializedSize() const {
  auto ret = size() * sizeof(FEATURE_TYPE) + sizeof(int);
  return ret;
}

/** FORMAT
  * number of weights (int)
  * list of weights: weight1 (FEATURE_TYPE) | weight2 (FEATURE_TYPE) | ..
  */
void SparseLRModel::loadSerialized(const void* data) {
  int num_weights = load_value<int>(data);
#ifdef DEBUG
  std::cout << "num_weights: " << num_weights << std::endl;
#endif
  assert(num_weights > 0 && num_weights < 10000000);

  char* data_begin = (char*)data;

  weights_.resize(num_weights);
  std::copy(reinterpret_cast<FEATURE_TYPE*>(data_begin),
      (reinterpret_cast<FEATURE_TYPE*>(data_begin)) + num_weights,
      weights_.data());
}

/***
   *
   */

void SparseLRModel::randomize() {
  // Xavier initialization
    for (auto& w : weights_) {
        w = 0;
    }
}

std::unique_ptr<CirrusModel> SparseLRModel::copy() const {
    std::unique_ptr<SparseLRModel> new_model =
        std::make_unique<SparseLRModel>(weights_.data(), size());
    return new_model;
}

void SparseLRModel::sgd_update_adagrad(double learning_rate,
    const ModelGradient* gradient) {
  const LRSparseGradient* grad =
    dynamic_cast<const LRSparseGradient*>(gradient);

  if (grad == nullptr) {
    throw std::runtime_error("Error in dynamic cast");
  }

  double adagrad_epsilon = 10e-8;

  for (const auto& w : grad->weights) {
    int index = w.first;
    FEATURE_TYPE value = w.second;

    // update history
    FEATURE_TYPE& weight_hist = weights_hist_[index];
    weight_hist += value * value;
    weights_[index] += learning_rate * value /
      (adagrad_epsilon + std::sqrt(weight_hist));
  }
}

void SparseLRModel::sgd_update_momentum(double learning_rate, double momentum_beta,
        const ModelGradient* gradient) {
    const LRSparseGradient* grad =
        dynamic_cast<const LRSparseGradient*>(gradient);

    if (grad == nullptr) {
        throw std::runtime_error("Error in dynamic cast");
    }

    for (const auto& w : grad->weights) {
        int index = w.first;
        FEATURE_TYPE value = w.second;
        if (momentum_avg == 0.0) {
            momentum_avg = value;
        } else {
            momentum_avg = momentum_beta * momentum_avg + (1.0 - momentum_beta) * learning_rate * value;
        }
        weights_[index] +=  momentum_avg;
    }
}
   

void SparseLRModel::sgd_update(double learning_rate,
    const ModelGradient* gradient) {
  const LRSparseGradient* grad =
    dynamic_cast<const LRSparseGradient*>(gradient);

  if (grad == nullptr) {
    throw std::runtime_error("Error in dynamic cast");
  }

  for (const auto& w : grad->weights) {
    int index = w.first;
    FEATURE_TYPE value = w.second;
    weights_[index] += learning_rate * value;
  }
}

double SparseLRModel::dot_product(
    const std::vector<std::pair<int, FEATURE_TYPE>>& v1,
    const std::vector<FEATURE_TYPE>& weights_) const {
  double res = 0;
  for (const auto& feat : v1) {
    int index = feat.first;
    FEATURE_TYPE value = feat.second;
    if ((uint64_t)index >= weights_.size()) {
      std::cerr << "index: " << index << " weights.size: " << weights_.size()
                << std::endl;
      throw std::runtime_error("Index too high");
    }
    assert(index >= 0 && (uint64_t)index < weights_.size());
    res += value * weights_[index];
#ifdef DEBUG
    if (std::isnan(res) || std::isinf(res)) {
      std::cout << "res: " << res << std::endl;
      std::cout << "i: " << i << std::endl;
      std::cout << "index: " << index << " value: " << value << std::endl;
      std::cout << "weights_[index]: " << weights_[index] << std::endl;
      exit(-1);
    }
#endif
  }
  return res;
}

std::unique_ptr<ModelGradient> SparseLRModel::minibatch_grad(
    const SparseDataset& dataset,
    const Configuration& config) const {
  if (is_sparse_) {
    throw std::runtime_error("This model is sparse");
  }
#ifdef DEBUG
    std::cout << "<Minibatch grad" << std::endl;
    dataset.check();
    //print();
    auto start = get_time_us();
#endif

    // For each sample compute the dot product with the model
    FEATURE_TYPE part2[dataset.num_samples()];
    for (uint64_t i = 0; i < dataset.num_samples(); ++i) {
      double part1_i = dot_product(dataset.get_row(i), weights_);
      part2[i] = dataset.labels_[i] - s_1(part1_i);
    }
#ifdef DEBUG
    auto after_1 = get_time_us();
#endif

    std::unordered_map<uint64_t, FEATURE_TYPE> part3;
    for (uint64_t i = 0; i < dataset.num_samples(); ++i) {
      for (const auto& feat : dataset.get_row(i)) {
        int index = feat.first;
        FEATURE_TYPE value = feat.second;
        part3[index] += value * part2[i];

#ifdef DEBUG
        if (std::isnan(part3[index]) || std::isinf(part3[index])) {
          std::cout << "part3 isnan" << std::endl;
          std::cout << "part2[i]: " << part2[i] << std::endl;
          std::cout << "i: " << i << std::endl;
          std::cout << "value: " << value << std::endl;
          std::cout << "index: " << index << " value: " << value << std::endl;
          exit(-1);
        }
#endif
      }
      part2[i] = 0; // prepare for next call
    }
#ifdef DEBUG
    auto after_2 = get_time_us();
#endif

    std::vector<std::pair<int, FEATURE_TYPE>> res;
    res.reserve(part3.size());
    //std::vector<FEATURE_TYPE> res(weights_);
    for (const auto& v : part3) {
      uint64_t index = v.first;
      FEATURE_TYPE value = v.second;
      res.push_back(std::make_pair(
          index, value + weights_[index] * 2 * config.get_epsilon()));
    }
#ifdef DEBUG
    auto after_3 = get_time_us();
#endif

    std::unique_ptr<LRSparseGradient> ret = std::make_unique<LRSparseGradient>(std::move(res));
#ifdef DEBUG
    auto after_4 = get_time_us();
#endif
    //std::unique_ptr<LRGradient> ret = std::make_unique<LRGradient>(res);

#ifdef DEBUG
    ret->check_values();
    std::cout
      << " Elapsed1: " << (after_1 - start)
      << " Elapsed2: " << (after_2 - after_1)
      << " Elapsed3: " << (after_3 - after_2)
      << " Elapsed4: " << (after_4 - after_3)
      << std::endl;
#endif
    return ret;
}

std::pair<double, double> SparseLRModel::calc_loss(SparseDataset& dataset, uint32_t) const {
  double total_loss = 0;
  auto w = weights_;

#ifdef DEBUG
  dataset.check();
#endif

  // count how many samples are wrongly classified
  uint64_t wrong_count = 0;
  for (uint64_t i = 0; i < dataset.num_samples(); ++i) {
    // get labeled class for the ith sample
    FEATURE_TYPE class_i = dataset.labels_[i];

    //auto r1 = ds.row(i) *  weights_eig;

    const auto& sample = dataset.get_row(i);
    double r1 = 0;
    for (const auto& feat : sample) {
      int index = feat.first;
      FEATURE_TYPE value = feat.second;
      r1 += weights_[index] * value;
    }
    
    double s1 = s_1(r1);
    FEATURE_TYPE predicted_class = 0;
    if (s1 > 0.5) {
      predicted_class = 1.0;
    }
    if (predicted_class != class_i) {
      wrong_count++;
    }

#define CROSS_ENTROPY_LOSS
#ifdef CROSS_ENTROPY_LOSS

    double value = class_i *
      log_aux(s1) +
      (1 - class_i) * log_aux(1 - s1);

    if (value > 0) {
      //std::cout << "ds row: " << std::endl << ds.row(i) << std::endl;
      //std::cout << "weights: " << std::endl << weights_eig << std::endl;
      //std::cout << "Class: " << class_i << " " << v1 << " " << v2
      //  << std::endl;
      throw std::runtime_error("Error: logistic loss is > 0");
    }

  //std::cout << "value: " << value << std::endl;
    total_loss -= value;
#endif
  }

  //std::cout << "wrong_count: " << wrong_count << std::endl;

  if (total_loss < 0) {
    throw std::runtime_error("total_loss < 0");
  }

  FEATURE_TYPE accuracy = (1.0 - (1.0 * wrong_count / dataset.num_samples()));
  if (std::isnan(total_loss) || std::isinf(total_loss))
    throw std::runtime_error("calc_log_loss generated nan/inf");

  return std::make_pair(total_loss, accuracy);
}

uint64_t SparseLRModel::getSerializedGradientSize() const {
    return size() * sizeof(FEATURE_TYPE);
}

std::unique_ptr<ModelGradient> SparseLRModel::loadGradient(void* mem) const {
  throw std::runtime_error("Not supported");
    auto grad = std::make_unique<LRGradient>(size());

    for (uint64_t i = 0; i < size(); ++i) {
        grad->weights[i] = reinterpret_cast<FEATURE_TYPE*>(mem)[i];
    }

    return grad;
}

bool SparseLRModel::is_integer(FEATURE_TYPE n) const {
    return floor(n) == n;
}

double SparseLRModel::checksum() const {
    return crc32(weights_.data(), weights_.size() * sizeof(FEATURE_TYPE));
}

void SparseLRModel::print() const {
    std::cout << "MODEL: ";
    for (const auto& w : weights_) {
        std::cout << " " << w;
    }
    std::cout << std::endl;
}

void SparseLRModel::check() const {
  for (const auto& w : weights_) {
    if (std::isnan(w) || std::isinf(w)) {
      std::cout << "Wrong model weight" << std::endl;
      exit(-1);
    }
  }
}

void SparseLRModel::loadSerializedSparse(const FEATURE_TYPE* weights,
                                         const uint32_t* weight_indices,
                                         uint64_t num_weights,
                                         const Configuration& config) {
  is_sparse_ = true;
  assert(num_weights > 0 && num_weights < 10000000);
  weights_sparse_.reserve((1 << config.get_model_bits()));
  for (uint64_t i = 0; i < num_weights; ++i) {
    uint32_t index = load_value<uint32_t>(weight_indices);
    FEATURE_TYPE value = load_value<FEATURE_TYPE>(weights);
    weights_sparse_[index] = value;
  }
}

void SparseLRModel::ensure_preallocated_vectors(const Configuration& config) const {
  if (unique_indices.capacity() == 0) {
    unique_indices.reserve(500);
  } else {
    unique_indices.clear();
    unique_indices.reserve(500);
  }
  
  if (part3.capacity() == 0) {
    part3.resize(1 << config.get_model_bits());
  }
  
  // value needs to be less than number of samples in minibatch
  if (part2.capacity() == 0) {
    part2.resize(config.get_minibatch_size());
  }
}

std::unique_ptr<ModelGradient> SparseLRModel::minibatch_grad_sparse(
        const SparseDataset& dataset,
        const Configuration& config) const {
  // this method should work regardless of whether model is sparse

  ensure_preallocated_vectors(config);

  for (uint64_t i = 0; i < dataset.num_samples(); ++i) {
    double part1_i = 0;
    for (const auto& feat : dataset.get_row(i)) {
      int index = feat.first;
      FEATURE_TYPE value = feat.second;
#ifdef DEBUG
      if (weights_sparse_.find(index) == weights_sparse_.end()) {
        std::cout << "Needed weight with index: " << index << std::endl;
        throw std::runtime_error("Weight not found");
      }
#endif
      part1_i += value * weights_sparse_[index]; // 25% of the execution time is spent here
    }
    part2[i] = dataset.labels_[i] - s_1(part1_i);
  }

  for (uint64_t i = 0; i < dataset.num_samples(); ++i) {
    for (const auto& feat : dataset.get_row(i)) {
      int index = feat.first;
      FEATURE_TYPE value = feat.second;
      unique_indices.push_back(index);
      part3[index] += value * part2[i];
    }
  }

  std::vector<std::pair<int, FEATURE_TYPE>> res;
  res.reserve(unique_indices.size());
  for (auto& v : unique_indices) {
    uint64_t index = v;//.first;
    FEATURE_TYPE value = part3[index];
    if (value == 0)
      continue;
    // we set this to 0 so that next iteration part3 is all 0s
    else part3[index] = 0;
    double final_grad = value + weights_sparse_[index] * 2 * config.get_epsilon();
    if (!config.get_grad_threshold_use()
        || (config.get_grad_threshold_use() && std::abs(final_grad) > config.get_grad_threshold())) {
      res.push_back(std::make_pair(index, final_grad));
    }
  }
  std::unique_ptr<LRSparseGradient> ret = std::make_unique<LRSparseGradient>(std::move(res));
  return ret;
}

} // namespace cirrus

