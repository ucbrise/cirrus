#ifndef SGD_H_
#define SGD_H_

#include "OptimizationMethod.h"

namespace cirrus {

class SGD : public OptimizationMethod {
  public:
    SGD(double lr);

    void sgd_update(
        std::unique_ptr<SparseLRModel>& lr_model, 
        const ModelGradient* gradient);
};

}  // namespace cirrus

#endif  // SGD_H_
