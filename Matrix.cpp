#include <Matrix.h>
#include <Utils.h>
#include <Checksum.h>

namespace cirrus {

Matrix::Matrix(std::vector<std::vector<FEATURE_TYPE>> m) :
  rows(0), cols(0), data(0) {
    if (!m.size()) {
      throw std::runtime_error("Wrong vector size in Matrix");
    }

    rows = m.size();
    cols = m[0].size();

    FEATURE_TYPE* new_array = new FEATURE_TYPE[rows * cols];

    uint64_t index = 0;
    for (const auto& v : m) {
      for (const auto& vv : v) {
        new_array[index++] = vv;
      }
    }

    // bug here
    data.reset(const_cast<const FEATURE_TYPE*>(new_array),
        std::default_delete<const FEATURE_TYPE[]>());
  }

Matrix::Matrix(const FEATURE_TYPE* d, uint64_t r, uint64_t c) {
  rows = r;
  cols = c;

  // XXX extra copy here
  FEATURE_TYPE* copy = new FEATURE_TYPE[rows * cols];
  memcpy(copy, d, rows * cols * sizeof(FEATURE_TYPE));

  data.reset(copy, std::default_delete<const FEATURE_TYPE[]>());
}

Matrix::Matrix(const FEATURE_TYPE* d, uint64_t r, uint64_t c, bool) {
  rows = r;
  cols = c;

  // XXX extra copy here
  FEATURE_TYPE* copy = new FEATURE_TYPE[rows * cols];
  for (uint64_t j = 0; j < rows; ++j) {
    const FEATURE_TYPE* data = d + j * (cols + 1);
    data++;
    std::copy(data,
        data + cols,
        copy + j * cols);
  }
  data.reset(copy, std::default_delete<const FEATURE_TYPE[]>());
}

Matrix::Matrix(const std::vector<std::shared_ptr<FEATURE_TYPE>> d,
    uint64_t r, uint64_t c) {

  rows = d.size() * r;
  cols = c;

  // XXX extra copy here
  FEATURE_TYPE* copy = new FEATURE_TYPE[rows * cols];
  for (uint64_t i = 0; i < d.size(); ++i) {
    memcpy(
        copy + i * (r * c),
        d[i].get(),
        r * c * sizeof(FEATURE_TYPE));
  }

  data.reset(copy, std::default_delete<const FEATURE_TYPE[]>());
}

const FEATURE_TYPE* Matrix::row(uint64_t l) const {
  const FEATURE_TYPE* data_start = reinterpret_cast<const FEATURE_TYPE*>(data.get());
  return &data_start[l * cols];
}

Matrix Matrix::T() const {
  if (cached_transpose.get()) {
    return *cached_transpose;
  }

  cached_transpose.reset(new Matrix(data.get(), cols, rows));

  return *cached_transpose;
}

uint64_t Matrix::sizeBytes() const {
  return rows * cols * sizeof(FEATURE_TYPE);
}

void Matrix::check_values() const {
  for (uint64_t i = 0; i < rows; ++i) {
    for (uint64_t j = 0; j < cols; ++j) {
      FEATURE_TYPE val = data.get()[i * cols + j];
      if (std::isinf(val) || std::isnan(val)) {
        throw std::runtime_error("Matrix has nans");
      }

#ifdef DEBUG
        // this sanity check may generate false positives
        // though it might help catch bugs
        if (val > 300 || val < -300) {
          throw std::runtime_error("Matrix::check value: "
              + std::to_string(val) + " badly normalized");
        }
#endif
    }
  }
}

double Matrix::checksum() const {
  return crc32(data.get(), rows * cols * sizeof(FEATURE_TYPE));
}

void Matrix::print() const {
  for (uint64_t i = 0; i < rows; ++i) {
    for (uint64_t j = 0; j < cols; ++j) {
      FEATURE_TYPE val = data.get()[i * cols + j];
      std::cout << val << " ";
    }
  }
  std::cout << std::endl;
}

}

