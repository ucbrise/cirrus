#ifndef _OPTMETHOD_H_
#define _OPTMETHOD_H_

#include <vector>
#include <utility>
#include <Model.h>
#include <ModelGradient.h>
#include "SparseLRModel.h"
#include <Utils.h>
#include <MlUtils.h>

namespace cirrus {

class OptimizationMethod {
 public:
   OptimizationMethod(double lr);

   virtual void sgd_update(
      std::unique_ptr<SparseLRModel>& lr_model, 
      const ModelGradient* gradient) = 0;
   virtual void edit_weight(double& weight);

 protected:
   double learning_rate;
};

}  // namespace cirrus

#endif  // _OPTMETHOD_H_
