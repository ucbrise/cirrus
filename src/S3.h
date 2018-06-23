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

/**
 * Initialize aws api
 */
void s3_initialize_aws();


/**
 * Create S3 client
 */
Aws::S3::S3Client s3_create_client(
    uint64_t max_connections = 2,
    uint64_t connect_timeout_ms = 30000,
    uint64_t request_timeout_ms = 60000);
Aws::S3::S3Client* s3_create_client_ptr();

/**
 * Write object into S3
 */
void s3_put_object(uint64_t id, Aws::S3::S3Client& s3_client,
    const std::string& bucket_name, const std::string& object);
void s3_put_object(const std::string& id, Aws::S3::S3Client& s3_client,
    const std::string& bucket_name, const std::string& object);

/**
 * Get object from S3
 */
std::string s3_get_object_value(uint64_t id, Aws::S3::S3Client& s3_client,
    const std::string& bucket_name);
std::string s3_get_object_value(const std::string& id, Aws::S3::S3Client& s3_client,
    const std::string& bucket_name);
std::ostringstream* s3_get_object_ptr(uint64_t id, Aws::S3::S3Client& s3_client,
    const std::string& bucket_name);
std::ostringstream* s3_get_object_ptr(const std::string& id, Aws::S3::S3Client& s3_client,
    const std::string& bucket_name);
std::ostringstream* s3_get_object_range_ptr(const std::string& id, Aws::S3::S3Client& s3_client,
    const std::string& bucket_name, std::pair<uint64_t, uint64_t> range);

/**
 * Shutdown aws APi
 */
void s3_shutdown_aws();

}  // namespace cirrus

#endif  // _CIRRUS_S3_H
