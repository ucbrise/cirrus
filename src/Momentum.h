#ifndef _MOMENTUM_H_
#define _MOMENTUM_H_

#include "OptimizationMethod.h"

namespace cirrus {  
  class Momentum : public OptimizationMethod {
    public:
      Momentum(double lr, double mb);
      std::vector<FEATURE_TYPE> sgd_update(
          std::vector<FEATURE_TYPE> weights, const ModelGradient* gradient);
    private:
      double momentum_beta;
      double momentum_avg = 0.0;
  };
}

#endif