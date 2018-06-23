#include "SGD.h"

namespace cirrus {
	SGD::SGD(double lr) : OptimizationMethod(lr) {}
	void SGD::sgd_update(
          std::vector<FEATURE_TYPE>& weights, const ModelGradient* gradient, std::vector<FEATURE_TYPE>& weights_history_) {
       const LRSparseGradient* grad =
         dynamic_cast<const LRSparseGradient*>(gradient);
         if (grad == nullptr) {
           throw std::runtime_error("Error in dynamic cast");
          }

         for (const auto& w : grad->weights) {
           int index = w.first;
           FEATURE_TYPE value = w.second;
           weights[index] += learning_rate * value;
          }
    }
        void SGD::edit_weight(double& weight) {
          return;
        }
}
