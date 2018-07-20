
#include "S3.h"
#include "S3Client.h"
#include <iostream>

int main() {
  cirrus::s3_initialize_aws();
  auto client = new cirrus::S3Client();
  client->s3_put_object(0, S3_BUCKET, "JOAO");
  auto res = client->s3_get_object(0, S3_BUCKET);

  std::cout << "Received: " << res << std::endl;

  cirrus::s3_shutdown_aws();
  return 0;
}
