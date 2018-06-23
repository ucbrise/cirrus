#ifndef _SGD_H_
#define _SGD_H_

#include "OptimizationMethod.h"

namespace cirrus {
  class SGD : public OptimizationMethod {
    public:
      SGD(double lr);
      void sgd_update(
         std::vector<FEATURE_TYPE>& weights, const ModelGradient* gradient, std::vector<FEATURE_TYPE>& weights_hist_);
  };
}

#endif
