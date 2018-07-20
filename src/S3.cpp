#include <S3.h>

//#define DEBUG

using namespace Aws::S3;

namespace cirrus {
Aws::SDKOptions options;

static bool called = false;
void s3_initialize_aws() {
  if (called) {
    throw std::runtime_error("S3 already active");
  } else {
    called = true;
  }
  Aws::InitAPI(options);
}

void s3_shutdown_aws() {
  if (called) {
    called = false;
  } else {
    throw std::runtime_error("S3 not active");
  }
  Aws::ShutdownAPI(options);
}

}  // namespace cirrus
