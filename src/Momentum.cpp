#include "Momentum.h"

namespace cirrus {

Momentum::Momentum(double lr, double mb) 
  : OptimizationMethod(lr), momentum_beta(mb){}

void Momentum::sgd_update(
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
    if (momentum_avg == 0.0) {
      momentum_avg = value;
    } else {
      momentum_avg = momentum_beta * momentum_avg + (1.0 - momentum_beta)
        * learning_rate * value;
    }
    lr_model->weights_[index] +=  momentum_avg;
  }
}

}  // namespace cirrus
