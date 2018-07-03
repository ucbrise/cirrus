#include "SGD.h"

namespace cirrus {

SGD::SGD(double lr)
  : OptimizationMethod(lr) {}

void SGD::sgd_update(
    std::unique_ptr<SparseLRModel>& lr_model, 
    const ModelGradient* gradient) {
  const LRSparseGradient* grad =
    dynamic_cast<const LRSparseGradient*>(gradient);
  if (grad == nullptr) {
    throw std::runtime_error("Error in dynamic cast");
  }

  for (const auto& w : grad->weights) {
    int index = w.first;
    FEATURE_TYPE value = w.second;
    lr_model->weights_[index] += learning_rate * value;
  }
}

}  // namespace cirrus
