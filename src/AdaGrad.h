#ifndef _ADAGRAD_H_
#define _ADAGRAD_H_

#include "OptimizationMethod.h"

namespace cirrus {
	class AdaGrad: public OptimizationMethod {
    public:
      AdaGrad(double lr, double ae);
      std::vector<FEATURE_TYPE> sgd_update(
          std::vector<FEATURE_TYPE> weights, const ModelGradient* gradient);

    private:
      double adagrad_epsilon;
      std::vector<FEATURE_TYPE> weights_hist_;
  };
}

#endif