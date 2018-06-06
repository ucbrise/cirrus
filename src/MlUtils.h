#ifndef _MLUTILS_H_
#define _MLUTILS_H_

#include <cmath>
#include <string>
#include <stdexcept>

namespace cirrus {

/**
  * Computes safe sigmoid of value x
  * @param x Input value
  * @return Sigmoid of x
  */
float s_1_float(float x);

template<typename T>
T s_1(T x) {
    double res = 1.0 / (1.0 + std::exp(-x));
    if (std::isnan(res) || std::isinf(res)) {
        throw std::runtime_error(
                std::string("s_1 generated nan/inf x: " + std::to_string(x)
                    + " res: " + std::to_string(res)));
    }
    return res;
}

/**
  * Computes logarithm
  * Check for NaN and Inf values
  * Clip values if they are too small (can lead to problems)
  * @param x Input value
  * @return Logarithm of x
  */
double log_aux(double x);

}  // namespace mlutils

#endif  // _MLUTILS_H_
