#ifndef _SGD_H_
#define _SGD_H_

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

#endif  // _SGD_H_
