#include "SGD.h"

namespace cirrus {
	SGD::SGD(double lr) : OptimizationMethod(lr) {}
	std::vector<FEATURE_TYPE> SGD::sgd_update(
          std::vector<FEATURE_TYPE> weights, const ModelGradient* gradient) {
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
        return weights;
    }
}
