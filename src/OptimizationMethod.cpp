#include "OptimizationMethod.h"

namespace cirrus {

OptimizationMethod::OptimizationMethod(double lr)
  : learning_rate(lr)
{}
   
void OptimizationMethod::edit_weight(double& weight) {
  return;
}

}  // namespace cirrus
