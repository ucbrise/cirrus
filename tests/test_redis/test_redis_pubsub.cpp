#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "hiredis/adapters/libevent.h"

void onMessage(redisAsyncContext *c, void *reply, void *privdata) {
    redisReply *r = (redisReply*)reply;
    if (reply == NULL) return;

    if (r->type == REDIS_REPLY_ARRAY) {
        for (unsigned int j = 0; j < r->elements; j++) {
            printf("%u) %s len: %lu\n", j, r->element[j]->str,
                r->element[j]->len);
        }
    }
}

void connectCallback(const redisAsyncContext *c, int i) {

}

void disconnectCallback(const redisAsyncContext *c, int i) {

}

void* thread_function(void*) {
  redisContext *c = redisConnect("172.31.0.28", 6379);
  while (1) {
    sleep(1);

    const char* random = "HEY\x00\xff\x31";
    redisCommand(c, "PUBLISH testtopic %b", random, 6);
  }

  return NULL;
}

int main (int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    struct event_base *base = event_base_new();

    redisAsyncContext *c = redisAsyncConnect("172.31.0.28", 6379);
    if (c->err) {
        printf("error: %s\n", c->errstr);
        return 1;
    }

    redisLibeventAttach(c, base);
    redisAsyncSetConnectCallback(c, connectCallback);
    redisAsyncSetDisconnectCallback(c, disconnectCallback);
    redisAsyncCommand(c, onMessage, NULL, "SUBSCRIBE testtopic");

    pthread_t tid;
    pthread_create(&tid, NULL, thread_function, NULL);

    event_base_dispatch(base);
    return 0;
}
