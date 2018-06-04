#include <iostream>
#include <chrono>

#include "Redis.h"

int main() {
    auto r = redis_connect("172.31.4.190", 6379);

    std::chrono::steady_clock::time_point start =
                               std::chrono::steady_clock::now();
    int len;

    redisReply* reply = reinterpret_cast<redisReply*>(
                                redisCommand(r, "GET 0"));

    std::chrono::steady_clock::time_point start2 =
                              std::chrono::steady_clock::now();
    char* ret = new char[reply->len];
    memcpy(ret, reply->str, reply->len);
    len = reply->len;

    freeReplyObject(reply);

    std::chrono::steady_clock::time_point finish =
                                              std::chrono::steady_clock::now();
    uint64_t elapsed_ns =
             std::chrono::duration_cast<std::chrono::nanoseconds>
                                              (finish-start.count();
    uint64_t elapsed2_ns =
                  std::chrono::duration_cast<std::chrono::nanoseconds>
                                                        (finish-start2).count();
    double elapsed_sec = 1.0 * elapsed_ns / 1000 / 1000 / 1000;
    double elapsed2_sec = 1.0 * elapsed2_ns / 1000 / 1000 / 1000;

    std::cout << "Elapsed " << elapsed_ns << std::endl;
    std::cout << "Elapsed2 " << elapsed2_ns << std::endl;
    std::cout << "BW (MB/s) "
              << 1.0 * len / 1024 / 1024 / elapsed_sec
              << std::endl;
    std::cout << "BW2 (MB/s) "
              << 1.0 * len / 1024 / 1024 / elapsed2_sec
              << std::endl;
    return 0;
}
