#include "OptimizationMethod.h"

namespace cirrus {
  class SGD : public OptimizationMethod {
    public:
      SGD(double lr);
      std::vector<FEATURE_TYPE> sgd_update(
         std::vector<FEATURE_TYPE> weights, const ModelGradient* gradient);
  }
}
