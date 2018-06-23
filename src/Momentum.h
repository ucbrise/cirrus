#ifndef _MOMENTUM_H_
#define _MOMENTUM_H_

#include "OptimizationMethod.h"

namespace cirrus {  
  class Momentum : public OptimizationMethod {
    public:
      Momentum(double lr, double mb);
      void sgd_update(
          std::vector<FEATURE_TYPE>& weights, const ModelGradient* gradient, std::vector<FEATURE_TYPE>& weights_hist_);
    private:
      double momentum_beta;
      double momentum_avg = 0.0;
  };
}

#endif
