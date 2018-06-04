#include "Redis.h"
#include "S3.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include <unistd.h>
#include <chrono>

static const int KB = 1024;

void test_put(redisContext* r, const char* id, const char*value) {
   redis_put_binary(r, id, value, strlen(value));
#ifdef DEBUG
   std::cout << "Wrote: "
             << value
             << " size: " << strlen(value)
             << std::endl;
#endif
}

char* test_get(redisContext* r, const char* id) {
   int len;
   char* s = redis_get(r, id, &len);

   if (s) {
#ifdef DEBUG
       std::cout << "2. Received: "
           << s
           << " size: " << len
           << std::endl;
#endif
           return s;
   } else {
      std::cout << "Object does not exist" << std::endl;
      return nullptr;
   }
}

int test_redis() {
   std::cout << "Connecting to redis" << std::endl;
   const char* redis_path =
     "ec2-34-215-76-49.us-west-2.compute.amazonaws.com";
   auto r = redis_connect(redis_path, 6379);

   if (r == NULL || r->err) {
     std::cout << "Connection error" << std::endl;
     return -1;
   }
   
   std::string s = "a";
   std::string id = "testredis";

   // put object in the object store
   test_put(r, id.c_str(), s.c_str());
   test_get(r, id.c_str());
   redis_delete(r, id.c_str());
   return 0;
}

void test_s3() {
    std::cout << "Initializing aws" << std::endl;
    s3_initialize_aws();

    std::cout << "Creating client" << std::endl;
    auto client = s3_create_client();
    std::cout << "Putting object" << std::endl;
    s3_put_object(0, client, S3_BUCKET, "JOAO");
    std::cout << "Getting object" << std::endl;
    auto res = s3_get_object(0, client, S3_BUCKET);
    std::cout << "Received: " << res << std::endl;

    s3_shutdown_aws();
}

int main() {
    std::cout << "Testing redis" << std::endl;
    int ret = test_redis();
    std::cout << "Redis test returned: " << ret << std::endl;
    std::cout << "Testing s3" << std::endl;
    test_s3();

    return 0;
}

