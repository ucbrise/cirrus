#include <SparseMFModel.h>
#include <Utils.h>
#include <MlUtils.h>
#include <Eigen/Dense>
#include <Checksum.h>
#include <algorithm>
#include <ModelGradient.h>

// #define DEBUG

namespace cirrus {

// FORMAT
// Number of users (32bits)
// Number of factors (32bits)
// Sample1: factor 1 (FEATURE_TYPE) | factor 2 | factor 3 
// Sample2 : ...
// ....

void SparseMFModel::initialize_weights(uint64_t users, uint64_t items, uint64_t nfactors) {
  item_fact_reg_ = 0.05;
  user_fact_reg_ = 0.05;

  user_bias_reg_ = 0.05;
  item_bias_reg_ = 0.05;
  global_bias_ = 3.604;

  nusers_ = users;
  nitems_ = items;
  nfactors_ = nfactors;

}

SparseMFModel::SparseMFModel(uint64_t users, uint64_t items, uint64_t nfactors) {
    initialize_weights(users , items, nfactors);
}

SparseMFModel::SparseMFModel(const void* data, uint64_t minibatch_size, uint64_t num_items) {
  initialize_weights(0, 0, 0);
  loadSerialized(data, minibatch_size, num_items);
}

std::unique_ptr<CirrusModel> SparseMFModel::deserialize(void* data, uint64_t /*size*/) const {
  throw std::runtime_error("Not implemented");
  uint32_t* data_p = reinterpret_cast<uint32_t*>(data);
  return std::make_unique<SparseMFModel>(
      reinterpret_cast<void*>(data_p), 10, 10);
}

std::pair<std::unique_ptr<char[]>, uint64_t>
SparseMFModel::serialize() const {
  throw std::runtime_error("not implemented");
    std::pair<std::unique_ptr<char[]>, uint64_t> res;
    uint64_t size = getSerializedSize();
    
    res.first.reset(new char[size]);
    res.second = size;

    serializeTo(res.first.get());
    return res;
}

void SparseMFModel::serializeTo(void* /*mem*/) const {
  throw std::runtime_error("Not implemented");
}

/**
  * We probably want to put 0 in the values we don't know
  */
void SparseMFModel::randomize() {
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

std::unique_ptr<CirrusModel> SparseMFModel::copy() const {
    std::unique_ptr<SparseMFModel> new_model =
        std::make_unique<SparseMFModel>(nusers_, nitems_, nfactors_);
    return new_model;
}

uint64_t SparseMFModel::getSerializedSize() const {
  throw std::runtime_error("Not implemented");
  return 0;
}

void SparseMFModel::loadSerialized(const void* data, uint64_t minibatch_size, uint64_t num_item_ids) {
#ifdef DEBUG
  std::cout << "SparseMFModel::loadSerialized nusers: "
    << nusers_
    << " nitems_: " << nitems_
    << " nfactors_: " << nfactors_
    << std::endl;
#endif
  // data has minibatch_size vectors of size NUM_FACTORS (user weights)
  // followed by the same (item weights)
  nfactors_ = NUM_FACTORS;
  for (uint64_t i = 0; i < minibatch_size; ++i) {
    std::tuple<int, FEATURE_TYPE,
      std::vector<FEATURE_TYPE>> user_model;
    uint32_t user_id = load_value<uint32_t>(data);
    FEATURE_TYPE user_bias = load_value<FEATURE_TYPE>(data);
    std::get<0>(user_model) = user_id;
    std::get<1>(user_model) = user_bias;
    for (uint64_t j = 0; j < NUM_FACTORS; ++j) {
      FEATURE_TYPE user_weight = load_value<FEATURE_TYPE>(data);
      std::get<2>(user_model).push_back(user_weight);
    }
    user_models.push_back(user_model);
  }
  global_bias_ = 3.604;
  // now we read the item vectors
  for (uint64_t i = 0; i < num_item_ids; ++i) {
    std::pair<FEATURE_TYPE,
      std::vector<FEATURE_TYPE>> item_model;
    uint32_t item_id = load_value<uint32_t>(data);
    FEATURE_TYPE item_bias = load_value<FEATURE_TYPE>(data);
    std::get<0>(item_model) = item_bias;
    std::get<1>(item_model).resize(NUM_FACTORS);
    for (uint64_t j = 0; j < NUM_FACTORS; ++j) {
      FEATURE_TYPE item_weight = load_value<FEATURE_TYPE>(data);
      std::get<1>(item_model)[j] = item_weight;
    }
    item_models[item_id] = item_model;
    //std::cout << "item_id: " << item_id << " model size: " << item_model.second.size() << std::endl;
  }

#ifdef DEBUG
  check();
#endif
}

/**
  * userId : 0 to minibatch_size
  */
FEATURE_TYPE SparseMFModel::predict(uint32_t userId, uint32_t itemId) {
  FEATURE_TYPE user_bias = std::get<1>(user_models[userId]);
  FEATURE_TYPE item_bias = item_models[itemId].first;

  FEATURE_TYPE res = global_bias_ + user_bias + item_bias;
  
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

std::unique_ptr<ModelGradient> SparseMFModel::minibatch_grad(
            const SparseDataset& dataset,
            const Configuration& config,
            uint64_t base_user) {
  FEATURE_TYPE learning_rate = config.get_learning_rate();
  auto gradient = std::make_unique<MFSparseGradient>();
  std::vector<FEATURE_TYPE> item_weights_grad_map[17770];
  std::vector<int> item_weights_lst;
  double training_rmse = 0;
  uint64_t training_rmse_count = 0;

  // iterate all pairs user rating
  for (uint64_t user_from_0 = 0; user_from_0 < dataset.data_.size(); ++user_from_0) {
    std::vector<FEATURE_TYPE> user_weights_grad(NUM_FACTORS);
    uint64_t real_user_id = base_user + user_from_0;

    // we have to populate this value in case this user doesn't have any ratings
    // XXX we should probably optimize this
    gradient->users_bias_grad[real_user_id] = 0;
    for (uint64_t j = 0; j < dataset.data_[user_from_0].size(); ++j) {
      // first user matches the model in user_models[0]
      uint64_t itemId = dataset.data_[user_from_0][j].first;
      FEATURE_TYPE rating = dataset.data_[user_from_0][j].second;

      FEATURE_TYPE pred = predict(user_from_0, itemId);

      FEATURE_TYPE error = rating - pred;
      training_rmse += error * error;
      training_rmse_count++;

      // compute gradient for user bias
      FEATURE_TYPE& user_bias = std::get<1>(user_models[user_from_0]);
      float delta = learning_rate * (error - user_bias_reg_ * user_bias);
      gradient->users_bias_grad[real_user_id] += delta;
      user_bias += delta;


      // compute gradient for item bias
      FEATURE_TYPE& item_bias = item_models[itemId].first;
      delta = learning_rate * (error - item_bias_reg_ * item_bias);
      gradient->items_bias_grad[itemId] += delta;
      item_bias += delta;

#ifdef DEBUG
      if (std::isnan(user_bias) || std::isnan(item_bias) ||
          std::isinf(user_bias) || std::isinf(item_bias))
        throw std::runtime_error("nan in user_bias or item_bias");
#endif

      // update user latent factors
      for (uint64_t k = 0; k < nfactors_; ++k) {
        FEATURE_TYPE delta_user_w = 
          learning_rate *
          (error * get_item_weights(itemId, k)
                   - user_fact_reg_ * get_user_weights(user_from_0, k));
        user_weights_grad[k] += delta_user_w;
        std::get<2>(user_models[user_from_0])[k] += delta_user_w;
#ifdef DEBUG
        if (std::isnan(get_user_weights(user_from_0, k)) ||
            std::isinf(get_user_weights(user_from_0, k))) {
          throw std::runtime_error("nan in user weight");
        }
#endif
      }
      //XXX moving this after this inner loop
      //gradient->users_weights_grad.push_back(std::make_pair(real_user_id, std::move(user_weights_grad)));

      // update item latent factors
      for (uint64_t k = 0; k < nfactors_; ++k) {
        //std::cout << "k: " << k << std::endl;
        FEATURE_TYPE delta_item_w =
          learning_rate *
          (error * get_user_weights(user_from_0, k) -
                 item_fact_reg_ * get_item_weights(itemId, k));
        item_models[itemId].second[k] += delta_item_w;

        if (item_weights_grad_map[itemId].size() == 0) {
          item_weights_grad_map[itemId].resize(NUM_FACTORS);
          item_weights_lst.push_back(itemId);
        }
        //std::cout << "UPDATE HERE " << std::endl;
        item_weights_grad_map[itemId][k] += delta_item_w;
#ifdef DEBUG
        if (std::isnan(get_item_weights(itemId, k)) ||
            std::isinf(get_item_weights(itemId, k))) {
          std::cout << "error: " << error << std::endl;
          std::cout << "rating: " << rating << std::endl;
          std::cout << "pred: " << pred << std::endl;
          std::cout << "delta_item_w: " << delta_item_w << std::endl;
          std::cout << "user weight: " << get_user_weights(user_from_0, k) << std::endl;
          std::cout << "item weight: " << get_item_weights(itemId, k) << std::endl;
          std::cout << "learning_rate: " << learning_rate << std::endl;
          throw std::runtime_error("nan in item weight");
        }
#endif
      }
      //gradient->items_weights_grad.push_back(
      //    std::make_pair(itemId, std::move(item_weights_grad)));
    }
    gradient->users_weights_grad.push_back(
        std::make_pair(real_user_id, std::move(user_weights_grad)));
    //std::cout 
    //  << "user weights size: " << gradient->users_weights_grad.size()
    //  << " user bias size: " << gradient->users_bias_grad.size() << std::endl;
  }

  for (const auto& item_id : item_weights_lst) {
    auto& item_weights = item_weights_grad_map[item_id];
    gradient->items_weights_grad.push_back(
        std::make_pair(item_id, std::move(item_weights)));
  }

#ifdef DEBUG
  std::cout << "Training rmse: " << std::sqrt(training_rmse / training_rmse_count) << std::endl;
  gradient->print();
  gradient->check();
#endif

  return gradient;
}

FEATURE_TYPE& SparseMFModel::get_user_weights(uint64_t userId, uint64_t factor) {
  return std::get<2>(user_models[userId])[factor];
}

FEATURE_TYPE& SparseMFModel::get_item_weights(uint64_t itemId, uint64_t factor) {
  return item_models[itemId].second[factor];
}


uint64_t SparseMFModel::getSerializedGradientSize() const {
  throw std::runtime_error("Not implemented");
  return 0;
  //  return size() * sizeof(FEATURE_TYPE);
}

std::unique_ptr<ModelGradient> SparseMFModel::loadGradient(void* mem) const {
    auto grad = std::make_unique<MFGradient>(10, 10);
    grad->loadSerialized(mem);
    return grad;
}

double SparseMFModel::checksum() const {
  return 0;
    //return crc32(user_weights_, nusers_ * nfactors_ * sizeof(FEATURE_TYPE));
}

void SparseMFModel::print() const {
    std::cout << "MODEL user weights: Not implemented";
    std::cout << std::endl;
}

void SparseMFModel::check() const {
  std::cout << "SparseMFModel::check() Not Implemented" << std::endl;
}

void SparseMFModel::serializeFromDense(
    MFModel& mf_model,
    uint32_t base_user_id, uint32_t minibatch_size, uint32_t k_items,
    const char* item_data_ptr, char* holder) const {

  char* data_to_send_ptr = holder;

  // first we store data about users
  for (uint32_t i = base_user_id; i < base_user_id + minibatch_size; ++i) {
    store_value<uint32_t>(data_to_send_ptr, i); // user id
    store_value<FEATURE_TYPE>(
        data_to_send_ptr,
        mf_model.get_user_bias(i));  // bias
    for (uint32_t j = 0; j < NUM_FACTORS; ++j) {
      store_value<FEATURE_TYPE>(
          data_to_send_ptr,
          mf_model.get_user_weights(i, j));
    }   
  }

  // now we store data about items
  for (uint32_t i = 0; i < k_items; ++i) {
    uint32_t item_id = load_value<uint32_t>(item_data_ptr);
    //std::cout << "item_id: " << item_id << std::endl;
    store_value<uint32_t>(data_to_send_ptr, item_id);
    store_value<FEATURE_TYPE>(data_to_send_ptr, mf_model.get_item_bias(item_id));
    for (uint32_t j = 0; j < NUM_FACTORS; ++j) {
      store_value<FEATURE_TYPE>(data_to_send_ptr, mf_model.get_item_weights(item_id, j));
    }
  }

}

} // namespace cirrus
