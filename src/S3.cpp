#include "S3.h"

//#define DEBUG

using namespace Aws::S3;

Aws::SDKOptions options;
namespace cirrus {
void s3_initialize_aws() {
  Aws::InitAPI(options);
}

void s3_shutdown_aws() {
  Aws::ShutdownAPI(options);
}
}
