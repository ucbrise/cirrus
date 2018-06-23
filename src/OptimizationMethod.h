#ifndef _OPTMETHOD_H_
#define _OPTMETHOD_H_

#include <vector>
#include <utility>
#include <Model.h>
#include <SparseDataset.h>
#include <ModelGradient.h>
#include <Configuration.h>
#include <unordered_map>
#include <Utils.h>
#include <MlUtils.h>
#include <Eigen/Dense>
#include <Checksum.h>
#include <algorithm>
#include <map>

namespace cirrus {
  class OptimizationMethod {
    public:
      OptimizationMethod(double lr)
        :learning_rate(lr) {}
      virtual std::vector<FEATURE_TYPE> sgd_update(
          std::vector<FEATURE_TYPE> weights, const ModelGradient* gradient) = 0;
    public:
      double learning_rate;
  };
}

#endif

