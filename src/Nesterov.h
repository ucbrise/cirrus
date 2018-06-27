#ifndef _NESTEROV_H_
#define _NESTEROV_H_

#include "OptimizationMethod.h"

namespace cirrus {

class Nesterov : public OptimizationMethod {
 public:
   Nesterov(double lr, double mb);

   void sgd_update(
       std::unique_ptr<SparseLRModel>& lr_model, 
       const ModelGradient* gradient);
   void edit_weight(double& weight);

 private:
   double momentum_beta;
   double momentum_avg = 0.0;
};

}  // namespace cirrus

#endif  // _NESTEROV_H_
