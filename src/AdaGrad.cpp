#include "AdaGrad.h"

namespace cirrus {

AdaGrad::AdaGrad(double lr, double ae):
  OptimizationMethod(lr), adagrad_epsilon(ae) {}

void AdaGrad::sgd_update(
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

    // update history
    FEATURE_TYPE& weight_hist = lr_model->weights_hist_[index];
    weight_hist += value * value;
    lr_model->weights_[index] += learning_rate * value /
      (adagrad_epsilon + std::sqrt(weight_hist));
  }
}

}  // namespace cirrus
