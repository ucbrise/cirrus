#include <vector>
#include <utility>
#include <Model.h>
#include <SparseDataset.h>
#include <ModelGradient.h>
#include <Configuration.h>
#include <unordered_map>
#include <Utils.h>
#include <MlUtils.h>
#include <Eigen/Dense>
#include <Checksum.h>
#include <algorithm>
#include <map>

namespace cirrus {
  class OptimizationMethod {
    public:
      OptimizationMethod(double lr)
        :learning_rate(lr) {}
      virtual std::vector<FEATURE_TYPE> sgd_update(
          std::vector<FEATURE_TYPE> weights, const ModelGradient* gradient) = 0;
    public:
      double learning_rate;
  };

  class SGD : public OptimizationMethod {
    public:
      SGD(double lr):
        OptimizationMethod(lr) {}
      std::vector<FEATURE_TYPE> sgd_update(
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
  };

  class Momentum : public OptimizationMethod {
    public:
      Momentum(double lr, double mb):
        OptimizationMethod(lr), momentum_beta(mb) {}
      std::vector<FEATURE_TYPE> sgd_update(
          std::vector<FEATURE_TYPE> weights, const ModelGradient* gradient) {
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
          weights[index] +=  momentum_avg;
        }
        return weights;
      }
      
    private:
      double momentum_beta;
      double momentum_avg = 0.0;
  };

  class AdaGrad: public OptimizationMethod {
    public:
      AdaGrad(double lr, double ae):
       OptimizationMethod(lr), adagrad_epsilon(ae) {}
      std::vector<FEATURE_TYPE> sgd_update(
          std::vector<FEATURE_TYPE> weights, const ModelGradient* gradient) {
        int64_t size = static_cast<int64_t>(weights_hist_.size());
        if (size == 0) {
          weights_hist_.resize(static_cast<int64_t>(weights.size()));
        }
        const LRSparseGradient* grad =
          dynamic_cast<const LRSparseGradient*>(gradient);

        if (grad == nullptr) {
          throw std::runtime_error("Error in dynamic cast");
        }

        for (const auto& w : grad->weights) {
           int index = w.first;
           FEATURE_TYPE value = w.second;

          // update history
          FEATURE_TYPE& weight_hist = weights_hist_[index];
          weight_hist += value * value;
          weights[index] += learning_rate * value /
          (adagrad_epsilon + std::sqrt(weight_hist));
        }
        return weights;
      }

    private:
      double adagrad_epsilon;
      std::vector<FEATURE_TYPE> weights_hist_;
  };
}

