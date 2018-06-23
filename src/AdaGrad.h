#ifndef _ADAGRAD_H_
#define _ADAGRAD_H_

#include "OptimizationMethod.h"

namespace cirrus {
	class AdaGrad: public OptimizationMethod {
    public:
      AdaGrad(double lr, double ae);
      void sgd_update(
          std::vector<FEATURE_TYPE>& weights, const ModelGradient* gradient, std::vector<FEATURE_TYPE>& weights_hist_);
      void edit_weight(double& weight);
    private:
      double adagrad_epsilon;
  };
}

#endif
