#include <LRModel.h>
#include <Utils.h>
#include <MlUtils.h>
#include <Eigen/Dense>
#include <Checksum.h>
#include <algorithm>

namespace cirrus {

LRModel::LRModel(uint64_t d) {
    weights_.resize(d);
}

LRModel::LRModel(const FEATURE_TYPE* w, uint64_t d) {
    weights_.resize(d);
    std::copy(w, w + d, weights_.begin());
}

uint64_t LRModel::size() const {
  return weights_.size();
}

/**
  * Serialization / deserialization routines
  */

/** FORMAT
  * weights
  */
std::unique_ptr<CirrusModel> LRModel::deserialize(void* data, uint64_t size) const {
    uint64_t d = size / sizeof(FEATURE_TYPE);
    std::unique_ptr<LRModel> model = std::make_unique<LRModel>(
            reinterpret_cast<FEATURE_TYPE*>(data), d);
    return model;
}

std::pair<std::unique_ptr<char[]>, uint64_t>
LRModel::serialize() const {
    std::pair<std::unique_ptr<char[]>, uint64_t> res;
    uint64_t size = getSerializedSize();
    res.first.reset(new char[size]);

    res.second = size;
    std::memcpy(res.first.get(), weights_.data(), getSerializedSize());

    return res;
}

void LRModel::serializeTo(void* mem) const {
    std::memcpy(mem, weights_.data(), getSerializedSize());
}

uint64_t LRModel::getSerializedSize() const {
    return size() * sizeof(FEATURE_TYPE);
}

void LRModel::loadSerialized(const void* data) {
    const FEATURE_TYPE* v = reinterpret_cast<const FEATURE_TYPE*>(data);
    std::copy(v, v + size(), weights_.begin());
}

/***
   *
   */

void LRModel::randomize() {
    for (auto& w : weights_) {
        w = 0.001 + get_rand_between_0_1();
    }
}

std::unique_ptr<CirrusModel> LRModel::copy() const {
    std::unique_ptr<LRModel> new_model =
        std::make_unique<LRModel>(weights_.data(), size());
    return new_model;
}

void LRModel::sgd_update(double learning_rate,
        const ModelGradient* gradient) {
    const LRGradient* grad = dynamic_cast<const LRGradient*>(gradient);

    if (grad == nullptr) {
        throw std::runtime_error("Error in dynamic cast");
    }

    for (uint64_t i = 0; i < size(); ++i) {
       weights_[i] += learning_rate * grad->weights[i];
    }
}

std::unique_ptr<ModelGradient> LRModel::minibatch_grad(
        const Matrix& dataset,
        FEATURE_TYPE* labels,
        uint64_t labels_size,
        double epsilon) const {
    auto w = weights_;
#ifdef DEBUG
    dataset.check();
#endif

    if (dataset.cols != size() || labels_size != dataset.rows) {
      throw std::runtime_error("Sizes don't match");
    }

    const FEATURE_TYPE* dataset_data = dataset.data.get();
    // create Matrix for dataset
    Eigen::Map<Eigen::Matrix<FEATURE_TYPE, Eigen::Dynamic,
        Eigen::Dynamic, Eigen::RowMajor>>
          ds(const_cast<FEATURE_TYPE*>(dataset_data), dataset.rows, dataset.cols);

    // create weight vector
    Eigen::Map<Eigen::Matrix<FEATURE_TYPE, -1, 1>> tmp_weights(w.data(), size());

    // create vector with labels
    Eigen::Map<Eigen::Matrix<FEATURE_TYPE, -1, 1>> lab(labels, labels_size);

    // apply logistic function to matrix multiplication
    // between dataset and weights
    auto part1_1 = (ds * tmp_weights);
    auto part1 = part1_1.unaryExpr(std::ptr_fun(s_1_float)); // XXX fix this

    Eigen::Map<Eigen::Matrix<FEATURE_TYPE, -1, 1>> lbs(labels, labels_size);

    // compute difference between labels and logistic probability
    auto part2 = lbs - part1;
    auto part3 = ds.transpose() * part2;
    auto part4 = tmp_weights * 2 * epsilon;
    auto res = part4 + part3;

    std::vector<FEATURE_TYPE> vec_res;
    vec_res.resize(res.size());
    Eigen::Matrix<FEATURE_TYPE, -1, 1>::Map(vec_res.data(), res.size()) = res;

    std::unique_ptr<LRGradient> ret = std::make_unique<LRGradient>(vec_res);

#ifdef DEBUG
    ret->check_values();
#endif

    return ret;
}

std::pair<double, double> LRModel::calc_loss(Dataset& dataset) const {
  double total_loss = 0;
  auto w = weights_;

#ifdef DEBUG
  dataset.check();
#endif

  const FEATURE_TYPE* ds_data =
    reinterpret_cast<const FEATURE_TYPE*>(dataset.samples_.data.get());

  Eigen::Map<Eigen::Matrix<FEATURE_TYPE, Eigen::Dynamic,
    Eigen::Dynamic, Eigen::RowMajor>>
      ds(const_cast<FEATURE_TYPE*>(ds_data),
          dataset.samples_.rows, dataset.samples_.cols);

  Eigen::Map<Eigen::Matrix<FEATURE_TYPE, -1, 1>> weights_eig(w.data(), size());

  // count how many samples are wrongly classified
  uint64_t wrong_count = 0;
  for (uint64_t i = 0; i < dataset.num_samples(); ++i) {
    // get labeled class for the ith sample
    FEATURE_TYPE class_i =
      reinterpret_cast<const FEATURE_TYPE*>(dataset.labels_.get())[i];

    assert(is_integer(class_i));

    int predicted_class = 0;

    auto r1 = ds.row(i) *  weights_eig;
    if (s_1((FEATURE_TYPE)r1) > 0.5) {
      predicted_class = 1;
    }
    if (predicted_class != class_i) {
      wrong_count++;
    }

    FEATURE_TYPE v1 = log_aux(1 - s_1((FEATURE_TYPE)(ds.row(i) * weights_eig)));
    FEATURE_TYPE v2 = log_aux(s_1((FEATURE_TYPE)(ds.row(i) *  weights_eig)));

    FEATURE_TYPE value = class_i * v2 + (1 - class_i) * v1;

    // XXX not sure this check is necessary
    if (value > 0 && value < 1e-6)
      value = 0;

    if (value > 0) {
      std::cout << "ds row: " << std::endl << ds.row(i) << std::endl;
      std::cout << "weights: " << std::endl << weights_eig << std::endl;
      std::cout << "Class: " << class_i << " " << v1 << " " << v2
        << std::endl;
      throw std::runtime_error("Error: logistic loss is > 0");
    }

    total_loss -= value;
  }

  if (total_loss < 0) {
    throw std::runtime_error("total_loss < 0");
  }

  FEATURE_TYPE accuracy = (1.0 - (1.0 * wrong_count / dataset.num_samples()));
  if (std::isnan(total_loss) || std::isinf(total_loss))
    throw std::runtime_error("calc_log_loss generated nan/inf");

  return std::make_pair(total_loss, accuracy);
}

uint64_t LRModel::getSerializedGradientSize() const {
    return size() * sizeof(FEATURE_TYPE);
}

std::unique_ptr<ModelGradient> LRModel::loadGradient(void* mem) const {
    auto grad = std::make_unique<LRGradient>(size());

    for (uint64_t i = 0; i < size(); ++i) {
        grad->weights[i] = reinterpret_cast<FEATURE_TYPE*>(mem)[i];
    }

    return grad;
}

bool LRModel::is_integer(FEATURE_TYPE n) const {
    return floor(n) == n;
}

double LRModel::checksum() const {
    return crc32(weights_.data(), weights_.size() * sizeof(FEATURE_TYPE));
}

void LRModel::print() const {
    std::cout << "MODEL: ";
    for (const auto& w : weights_) {
        std::cout << " " << w;
    }
    std::cout << std::endl;
}

} // namespace cirrus
