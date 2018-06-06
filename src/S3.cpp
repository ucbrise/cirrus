#include "S3.h"

//#define DEBUG

using namespace Aws::S3;

Aws::SDKOptions options;

void s3_initialize_aws() {
  Aws::InitAPI(options);
}

Aws::S3::S3Client s3_create_client() {
  Aws::Client::ClientConfiguration clientConfig;
  clientConfig.region = Aws::Region::US_WEST_2;
  clientConfig.maxConnections = 2;
  clientConfig.connectTimeoutMs = 30000;
  clientConfig.requestTimeoutMs = 60000;

  Aws::S3::S3Client s3_client(clientConfig);
  return s3_client;
}

Aws::S3::S3Client* s3_create_client_ptr() {
  Aws::Client::ClientConfiguration clientConfig;
  clientConfig.region = Aws::Region::US_WEST_2;
  
  // try big timeout
  clientConfig.connectTimeoutMs = 30000;
  clientConfig.requestTimeoutMs = 60000;

  return new Aws::S3::S3Client(clientConfig);
}

void s3_put_object(const std::string& key_name, Aws::S3::S3Client& s3_client,
                const std::string& bucket_name, const std::string& object) {
  Model::PutObjectRequest putObjectRequest;
#ifdef DEBUG
  std::cout << "putObjectRequest building" << std::endl;
#endif
  putObjectRequest.WithBucket(bucket_name.c_str())
      .WithKey(key_name.c_str());

  auto ss = Aws::MakeShared<Aws::StringStream>("TAG");
#ifdef DEBUG
  std::cout << "writing stringstream" << std::endl;
#endif
  *ss << object;
  ss->flush();

#ifdef DEBUG
  std::cout << "setting body" << std::endl;
#endif
  putObjectRequest.SetBody(ss);

#ifdef debug
  std::cout << "putting object" << std::endl;
#endif
  auto put_object_outcome = s3_client.PutObject(putObjectRequest);

#ifdef debug
  std::cout << "checking success" << std::endl;
#endif
  if (put_object_outcome.IsSuccess()) {
      std::cout << "Done!" << std::endl;
  } else {
      std::cout << "PutObject error: " <<
          put_object_outcome.GetError().GetExceptionName() << " " <<
          put_object_outcome.GetError().GetMessage() << std::endl;
  }
}

void s3_put_object(uint64_t id, Aws::S3::S3Client& s3_client,
                const std::string& bucket_name, const std::string& object) {
  std::string key_name = "CIRRUS" + std::to_string(id);
  s3_put_object(key_name, s3_client, bucket_name, object);
}

std::string s3_get_object_value(const std::string& key_name, Aws::S3::S3Client& s3_client,
    const std::string& bucket_name) {
  auto ret = s3_get_object_ptr(key_name, s3_client, bucket_name);
  std::string value = ret->str();
  delete ret;
  return std::move(value);
}

std::string s3_get_object_value(uint64_t id, Aws::S3::S3Client& s3_client,
    const std::string& bucket_name) {
  std::string key_name = "cirrus" + std::to_string(id);
  auto ret = s3_get_object_ptr(key_name, s3_client, bucket_name);
  std::string value = ret->str();
  delete ret;
  return std::move(value);
}

std::ostringstream* s3_get_object_ptr(uint64_t id, Aws::S3::S3Client& s3_client,
                const std::string& bucket_name) {
  std::string key_name = "CIRRUS" + std::to_string(id);
  return s3_get_object_ptr(key_name, s3_client, bucket_name);
}

std::ostringstream* s3_get_object_ptr(const std::string& key_name, Aws::S3::S3Client& s3_client,
                const std::string& bucket_name) {
  Aws::S3::Model::GetObjectRequest object_request;
  object_request.WithBucket(bucket_name.c_str()).WithKey(key_name.c_str());

  auto get_object_outcome = s3_client.GetObject(object_request);

  if (get_object_outcome.IsSuccess()) {
    std::ostringstream* ss = new std::ostringstream;
    *ss << get_object_outcome.GetResult().GetBody().rdbuf();
    return ss;
  } else {
    std::cout << "GetObject error: " <<
       get_object_outcome.GetError().GetExceptionName() << " " <<
       get_object_outcome.GetError().GetMessage() << std::endl;
    throw std::runtime_error("Error");
  }
}

void s3_shutdown_aws() {
  Aws::ShutdownAPI(options);
}

