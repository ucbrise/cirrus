#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <iostream>

#include "../Redis.h"
//#include "hiredis/hiredis.h"
//#include "hiredis/async.h"

int main (int argc, char **argv) {
    redisContext *c = redis_connect("172.31.0.28", 6379);
    if (c->err) {
        printf("error: %s\n", c->errstr);
        return 1;
    }

    std::cout << "Conn succ" << std::endl;

    int prev;
    redis_increment_counter(c, "test_counter", &prev);
    std::cout << "incr succ .prev: " << prev << std::endl;

    return 0;
}
