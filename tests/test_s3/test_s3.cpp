#include <stdio.h>
#include <unistd.h>
#include <chrono>
#include <iostream>
#include <string>
#include "S3.h"
#include "S3Client.h"

static const int KB = 1024;

void test_s3() {
  std::cout << "Initializing aws" << std::endl;
  cirrus::s3_initialize_aws();

  std::cout << "Creating client" << std::endl;
  auto client = new cirrus::S3Client();
  std::cout << "Putting object" << std::endl;
  client->s3_get_object_ptr("test", "cirrus-test-buck");

  cirrus::s3_shutdown_aws();
}

int main() {
  std::cout << "Testing s3" << std::endl;
  test_s3();

  return 0;
}
