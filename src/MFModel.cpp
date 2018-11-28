#include <MFModel.h>
#include <Utils.h>
#include <MlUtils.h>
#include <Eigen/Dense>
#include <Checksum.h>
#include <algorithm>
#include <ModelGradient.h>

//#define DEBUG

namespace cirrus {

MFModel::MFModel(uint64_t users, uint64_t items, uint64_t nfactors) {
    initialize_data(users, items, nfactors);
}

MFModel::MFModel(
    const void* data, uint64_t /*nusers*/, uint64_t /*nitems*/, uint64_t /*nfactors*/) {
  loadSerialized(data);
}

void MFModel::initialize_reg_params() {
  item_fact_reg_ = 0.01;
  user_fact_reg_ = 0.01;

  user_bias_reg_ = 0.01;
  item_bias_reg_ = 0.01;
}

// FORMAT
// Number of users (32bits)
// Number of factors (32bits)
// Sample1: factor 1 (FEATURE_TYPE) | factor 2 | factor 3 
// Sample2 : ...
// ....

void MFModel::initialize_data(uint64_t users, uint64_t items, uint64_t nfactors) {
  user_weights_.resize(users * nfactors);
  item_weights_.resize(items * nfactors);
  global_bias_ = 3.604;

  user_bias_.resize(users);
  item_bias_.resize(items);

  initialize_reg_params();

  nusers_ = users;
  nitems_ = items;
  nfactors_ = nfactors;

  randomize();

  std::cout << "Initializing MFModel nusers: " << nusers_
            << " nitems: " << nitems_
            << std::endl;
}

uint64_t MFModel::size() const {
  throw std::runtime_error("Not implemented");
  return 0;
}

/**
  *
  * Serialization / deserialization
  *
  */

std::unique_ptr<CirrusModel> MFModel::deserialize(void* /*data*/, uint64_t /*size*/) const {
  throw std::runtime_error("Not implemented");
}

std::pair<std::unique_ptr<char[]>, uint64_t>
MFModel::serialize() const {
    std::pair<std::unique_ptr<char[]>, uint64_t> res;
    uint64_t size = getSerializedSize();
    
    res.first.reset(new char[size]);
    res.second = size;

    serializeTo(res.first.get());
    return res;
}

uint64_t MFModel::getSerializedSize() const {
    return sizeof(uint64_t) * 3 + // nusers + nitem + nfactors
      user_bias_.size() * sizeof(FEATURE_TYPE) +
      item_bias_.size() * sizeof(FEATURE_TYPE) +
      user_weights_.size() * sizeof(FEATURE_TYPE) +
      item_weights_.size() * sizeof(FEATURE_TYPE);
}

void MFModel::serializeTo(void* mem) const {
    char* data = reinterpret_cast<char*>(mem);

#ifdef DEBUG
    std::cout
      << "SerializeTo"
      << " nusers: " << nusers_
      << " nitems_: " << nitems_
      << " nfactors_: " << nfactors_
      << std::endl;
#endif

    store_value<uint64_t>(data, nusers_);
    store_value<uint64_t>(data, nitems_);
    store_value<uint64_t>(data, nfactors_);

    for (const auto& v : user_bias_) {
      store_value<FEATURE_TYPE>(data, v);
    }
    for (const auto& v : item_bias_) {
      store_value<FEATURE_TYPE>(data, v);
    }
    for (const auto& v : user_weights_) {
      store_value<FEATURE_TYPE>(data, v);
    }
    for (const auto& v : item_weights_) {
      store_value<FEATURE_TYPE>(data, v);
    }
}

void MFModel::loadSerialized(const void* data) {
  // Read number of samples, number of factors
  nusers_ = load_value<uint64_t>(data);
  nitems_ = load_value<uint64_t>(data);
  nfactors_ = load_value<uint64_t>(data);

#ifdef DEBUG
  std::cout << "loadSerialized"
    << " nusers: " << nusers_
    << " nitems_: " << nitems_
    << " nfactors_: " << nfactors_
    << std::endl;
#endif

  global_bias_ = 3.604;
  user_weights_.resize(nusers_ * nfactors_);
  item_weights_.resize(nitems_ * nfactors_);
  user_bias_.resize(nusers_);
  item_bias_.resize(nitems_);

  // read user bias
  for (uint32_t i = 0; i < nusers_; ++i) {
    FEATURE_TYPE user_bias = load_value<FEATURE_TYPE>(data);
    user_bias_[i] = user_bias;
  }
  // read item bias
  for (uint32_t i = 0; i < nitems_; ++i) {
    FEATURE_TYPE item_bias = load_value<FEATURE_TYPE>(data);
    item_bias_[i] = item_bias;
  }
  // read user weights
  for (uint32_t i = 0; i < nusers_; ++i) {
    for (uint32_t j = 0; j < nfactors_; ++j) {
      FEATURE_TYPE user_weight = load_value<FEATURE_TYPE>(data);
      get_user_weights(i, j) = user_weight;
    }
  }
  // read item weights
  for (uint32_t i = 0; i < nitems_; ++i) {
    for (uint32_t j = 0; j < nfactors_; ++j) {
      FEATURE_TYPE item_weight = load_value<FEATURE_TYPE>(data);
      get_item_weights(i, j) = item_weight;
    }
  }
}

/**
  * We probably want to put 0 in the values we don't know
  */
void MFModel::randomize() {
  std::default_random_engine generator;
  std::normal_distribution<FEATURE_TYPE> distribution(0, 1.0 / nfactors_); // mean 0 and stddev=1
  for (uint64_t i = 0; i < nusers_; ++i) {
    for (uint64_t j = 0; j < nfactors_; ++j) {
      get_user_weights(i, j) = distribution(generator);
    }
  }
  for (uint64_t i = 0; i < nitems_; ++i) {
    for (uint64_t j = 0; j < nfactors_; ++j) {
      get_item_weights(i, j) = distribution(generator);
    }
  }
}

std::unique_ptr<CirrusModel> MFModel::copy() const {
    std::unique_ptr<MFModel> new_model =
        std::make_unique<MFModel>(nusers_, nitems_, nfactors_);
    return new_model;
}

void MFModel::sgd_update(double learning_rate,
        const ModelGradient* gradient) {
  const MFSparseGradient* grad_ptr = dynamic_cast<const MFSparseGradient*>(gradient);
  assert(grad_ptr);

  // apply grad to users_bias_grad
  for (const auto& v : grad_ptr->users_bias_grad) {
      user_bias_[v.first] += v.second;
  }
  for (const auto& v : grad_ptr->items_bias_grad) {
      item_bias_[v.first] += v.second;
  }
  for (const auto& v : grad_ptr->users_weights_grad) {
    int user_id = v.first;
    assert(v.second.size() == NUM_FACTORS);
    for (uint32_t i = 0; i < v.second.size(); ++i) {
      get_user_weights(user_id, i) += v.second[i];
    }
  }
  for (const auto& v : grad_ptr->items_weights_grad) {
    int item_id = v.first;
    assert(v.second.size() == NUM_FACTORS);
    for (uint32_t i = 0; i < v.second.size(); ++i) {
      get_item_weights(item_id, i) += v.second[i];
    }
  }
}

FEATURE_TYPE MFModel::predict(uint32_t userId, uint32_t itemId) const {
#ifdef DEBUG
  FEATURE_TYPE res = global_bias_ + user_bias_.at(userId) + item_bias_.at(itemId);
#else
  FEATURE_TYPE res = global_bias_ + user_bias_[userId] + item_bias_[itemId];
#endif
  for (uint32_t i = 0; i < nfactors_; ++i) {
    res += get_user_weights(userId, i) * get_item_weights(itemId, i);
#ifdef DEBUG
    if (std::isnan(res) || std::isinf(res)) {
      std::cout << "userId: " << userId << " itemId: " << itemId
        << " get_user_weights(userId, i): " << get_user_weights(userId, i)
        << " get_item_weights(itemId, i): " << get_item_weights(itemId, i)
        << std::endl;
      throw std::runtime_error("nan error in predict");
    }
#endif
  }
  return res;
}

std::unique_ptr<ModelGradient> MFModel::minibatch_grad(
        const Matrix&,
        FEATURE_TYPE*,
        uint64_t,
        double) const {
  throw std::runtime_error("Not implemented");
}

FEATURE_TYPE& MFModel::get_user_weights(uint64_t userId, uint64_t factor) {
  return user_weights_.at(userId * nfactors_ + factor);
}

FEATURE_TYPE& MFModel::get_item_weights(uint64_t itemId, uint64_t factor) {
  if (itemId >= nitems_) {
    std::cout << "itemId: " << itemId << " nitems_: " << nitems_ << std::endl;
  }
  return item_weights_.at(itemId * nfactors_ + factor);
}

const FEATURE_TYPE& MFModel::get_user_weights(uint64_t userId, uint64_t factor) const {
  return user_weights_.at(userId * nfactors_ + factor);
}

const FEATURE_TYPE& MFModel::get_item_weights(uint64_t itemId, uint64_t factor) const {
  return item_weights_.at(itemId * nfactors_ + factor);
}

void MFModel::sgd_update(
            double learning_rate,
            uint64_t base_user,
            const SparseDataset& dataset,
            double /*epsilon*/) {
  // iterate all pairs user rating
  for (uint64_t i = 0; i < dataset.data_.size(); ++i) {
    for (uint64_t j = 0; j < dataset.data_[i].size(); ++j) {
      uint64_t user = base_user + i;
      uint64_t itemId = dataset.data_[i][j].first;
      FEATURE_TYPE rating = dataset.data_[i][j].second;

      FEATURE_TYPE pred = predict(user, itemId);
      FEATURE_TYPE error = rating - pred;

#ifdef DEBUG
      std::cout
        << "user: " << user
        << "itemId: " << itemId
        << "rating: " << rating
        << " prediction: " << pred
        << " error: " << error
        << std::endl;
#endif

      if (itemId >= nitems_ || user >= nusers_) {
        std::cout
          << "itemId: " << itemId
          << " nitems_: " << nitems_
          << " user: " << user
          << " nusers_: " << nusers_
          << std::endl;
        throw std::runtime_error("Wrong value here");
      }
      user_bias_[user] += learning_rate * (error - user_bias_reg_ * user_bias_[user]);
      item_bias_[itemId] += learning_rate * (error - item_bias_reg_ * item_bias_[itemId]);

#ifdef DEBUG
      if (std::isnan(user_bias_[user]) || std::isnan(item_bias_[itemId]) ||
          std::isinf(user_bias_[user]) || std::isinf(item_bias_[itemId]))
        throw std::runtime_error("nan in user_bias or item_bias");
#endif

      // update user latent factors
      for (uint64_t k = 0; k < nfactors_; ++k) {
        FEATURE_TYPE delta_user_w =
          learning_rate * (error * get_item_weights(itemId, k) - user_fact_reg_ * get_user_weights(user, k));
        //std::cout << "delta_user_w: " << delta_user_w << std::endl;
        get_user_weights(user, k) += delta_user_w;
#ifdef DEBUG
        if (std::isnan(get_user_weights(user, k)) || std::isinf(get_user_weights(user, k))) {
          throw std::runtime_error("nan in user weight");
        }
#endif
      }

      // update item latent factors
      for (uint64_t k = 0; k < nfactors_; ++k) {
        FEATURE_TYPE delta_item_w =
          learning_rate * (error * get_user_weights(user, k) - item_fact_reg_ * get_item_weights(itemId, k));
        //std::cout << "delta_item_w: " << delta_item_w << std::endl;
        get_item_weights(itemId, k) += delta_item_w;
#ifdef DEBUG
        if (std::isnan(get_item_weights(itemId, k)) || std::isinf(get_item_weights(itemId, k))) {
          std::cout << "error: " << error << std::endl;
          std::cout << "user weight: " << get_user_weights(user, k) << std::endl;
          std::cout << "item weight: " << get_item_weights(itemId, k) << std::endl;
          std::cout << "learning_rate: " << learning_rate << std::endl;
          throw std::runtime_error("nan in item weight");
        }
#endif
      }
    }
  }
}

std::pair<double, double> MFModel::calc_loss(Dataset& /*dataset*/) const {
  throw std::runtime_error("Not implemented");
  return std::make_pair(0.0, 0.0);
}

std::pair<double, double> MFModel::calc_loss(SparseDataset& dataset, uint32_t start_index) const {
  double error = 0;
  uint64_t count = 0;

#ifdef DEBUG
  std::cout
    << "calc_loss() starting"
    << std::endl;
#endif

  for (uint64_t userId = 0; userId < dataset.data_.size(); ++userId) {
    uint64_t off_userId = userId + start_index;
#ifdef DEBUG
      std::cout
        << "off_userId: " << off_userId
        << " userId: " << userId
        << " dataset.data_.size(): " << dataset.data_.size()
        << std::endl;
#endif
    for (uint64_t j = 0; j < dataset.data_.at(userId).size(); ++j) {
      uint64_t movieId = dataset.data_.at(userId).at(j).first;
#ifdef DEBUG
      std::cout
        << " movieId: " << movieId
        << std::endl;
#endif
      FEATURE_TYPE rating = dataset.data_.at(userId).at(j).second;

      FEATURE_TYPE prediction = predict(off_userId, movieId);
      FEATURE_TYPE e = rating - prediction;

      FEATURE_TYPE e_pow_2 = pow(e, 2);
      error += e_pow_2;
#ifdef DEBUG
      std::cout
        << "prediction: " << prediction
        << " rating: " << rating
        << " e: " << e
        << " e_pow_2: " << pow(e, 2)
        << " error: " << error
        << " count: " << count
        << std::endl;
#endif
      if (std::isnan(e) || std::isnan(error)) {
        std::string error = std::string("nan in calc_loss rating: ") + std::to_string(rating) +
          " prediction: " + std::to_string(prediction);
        throw std::runtime_error(error);
      }
      count++;
    }
  }

#ifdef DEBUG
  std::cout << "error: " << error << " count: " << count << std::endl;
#endif

  //error = error / count;
  //error = std::sqrt(error);
  if (std::isnan(error)) {
    throw std::runtime_error("error isnan");
  }
  return std::make_pair(error, count);
}

uint64_t MFModel::getSerializedGradientSize() const {
    return size() * sizeof(FEATURE_TYPE);
}

std::unique_ptr<ModelGradient> MFModel::loadGradient(void* mem) const {
    auto grad = std::make_unique<MFGradient>(10, 10);
    grad->loadSerialized(mem);
    return grad;
}

double MFModel::checksum() const {
  return crc32(user_weights_.data(), user_weights_.size() * sizeof(FEATURE_TYPE));
}

void MFModel::print() const {
    std::cout << "MODEL user weights: ";
    for (uint64_t i = 0; i < user_weights_.size(); ++i) {
      std::cout << " " << user_weights_[i];
    }
    std::cout << std::endl;
}

FEATURE_TYPE& MFModel::get_user_bias(uint64_t userId) {
  if (userId >= user_bias_.size()) {
    throw std::runtime_error("User bias index too large");
  }
  return user_bias_.at(userId);
}

FEATURE_TYPE& MFModel::get_item_bias(uint64_t itemId) {
  if (itemId >= item_bias_.size()) {
    throw std::runtime_error("Item bias index too large");
  }
  return item_bias_.at(itemId);
}

}  // namespace cirrus
