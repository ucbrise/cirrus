#ifndef MOMENTUM_H_
#define MOMENTUM_H_

#include "OptimizationMethod.h"

namespace cirrus {  

class Momentum : public OptimizationMethod {
 public:
   Momentum(double lr, double mb);
   void sgd_update(
       std::unique_ptr<SparseLRModel>& lr_model, 
       const ModelGradient* gradient);

 private:
   double momentum_beta;
   double momentum_avg = 0.0;
};

}  // namespace cirrus

#endif  // MOMENTUM_H_
