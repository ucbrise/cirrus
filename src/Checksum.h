#ifndef CHECKSUM_H_
#define CHECKSUM_H_

#include <cstdint>
#include <cstddef>
#include <memory>
#include <config.h>

/**
  * Compute crc32 checksum of data in buf and with size length
  * @return crc32 checksum
  */
uint32_t crc32(const void *buf, size_t size);

/**
  * Compute crc32 checksum for array of FEATURE_TYPE with length size
  * @return crc32 checksum
  */
double checksum(FEATURE_TYPE* p, uint64_t size);

#endif  // CHECKSUM_H_
