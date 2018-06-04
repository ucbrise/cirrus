
#include "S3.h"
#include <iostream>

int main() {
  s3_initialize_aws();
  auto client = s3_create_client();
  s3_put_object(0, client, S3_BUCKET, "JOAO");
  auto res = s3_get_object(0, client, S3_BUCKET);

  std::cout << "Received: " << res << std::endl;

  s3_shutdown_aws();
  return 0;
}
