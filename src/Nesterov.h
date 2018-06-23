#ifndef _NESTEROV_H_
#define _NESTEROV_H_

#include "OptimizationMethod.h"

namespace cirrus {
	class Nesterov : public OptimizationMethod {
		public:
			Nesterov(double lr, double mb);
			void sgd_update(
				std::vector<FEATURE_TYPE>& weights, const ModelGradient* gradient, std::vector<FEATURE_TYPE>& weights_hist_);
			void edit_weight(double& weight);
		private:
			double momentum_beta;
			double momentum_avg = 0.0;
	};
}

#endif
