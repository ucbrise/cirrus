#ifndef _ADAGRAD_H_
#define _ADAGRAD_H_

#include "OptimizationMethod.h"

namespace cirrus {

class AdaGrad: public OptimizationMethod {
 public:
   AdaGrad(double lr, double ae);
   void sgd_update(
          std::unique_ptr<SparseLRModel>& lr_model, 
          const ModelGradient* gradient);
 
 private:
    double adagrad_epsilon;
};

}  // namespace cirrus

#endif // _ADAGRAD_H_
