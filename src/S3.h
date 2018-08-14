#ifndef _CIRRUS_S3_H
#define _CIRRUS_S3_H

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/core/client/ClientConfiguration.h>
#include <string>

// #define DEBUG

using namespace Aws::S3;

namespace cirrus {

void s3_initialize_aws();
void s3_shutdown_aws();

}  // namespace cirrus

#endif  // _CIRRUS_S3_H
