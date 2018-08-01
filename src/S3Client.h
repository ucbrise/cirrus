#ifndef _CIRRUS_S3CLIENT_H_
#define _CIRRUS_S3CLIENT_H_

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <memory>
#include <string>

using namespace Aws::S3;

namespace cirrus {
class S3Client {
 public:
  S3Client();
  void s3_put_object(uint64_t id,
                     const std::string& bucket_name,
                     const std::string& object);
  std::string s3_get_object_value(uint64_t id, const std::string& bucket_name);
  std::ostringstream* s3_get_object_ptr(uint64_t id,
                                        const std::string& bucket_name);
  void s3_put_object(const std::string& key_name,
                     const std::string& bucket_name,
                     const std::string& object);
  std::string s3_get_object_value(const std::string& key_name,
                                  const std::string& bucket_name);
  std::ostringstream* s3_get_object_ptr(const std::string& key_name,
                                        const std::string& bucket_name);
  std::shared_ptr<std::ostringstream> s3_get_object_range_ptr(
      const std::string& key_name,
      const std::string& bucket_name,
      std::pair<uint64_t, uint64_t> range);

 private:
  std::unique_ptr<Aws::S3::S3Client> s3_client;
};
}

#endif
