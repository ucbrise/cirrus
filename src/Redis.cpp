#include <stdio.h>
#include <iostream>

extern "C" {

#include <string.h>
#include <assert.h>
#include "Redis.h"

#undef REDIS_DEBUG

static char cmd[10000];

redisContext* redis_connect(const char* hostname, int port) {
    struct timeval timeout = { 1, 500000 }; // 1.5 seconds

#ifdef REDIS_DEBUG
    std::cout << "Connecting to hostname: " << hostname << std::endl;
#endif
    redisContext *c;
    c = redisConnectWithTimeout(hostname, port, timeout);

    return c;
}

void redis_put(redisContext* c, const char* id,
        const char* s) {
#ifdef REDIS_DEBUG
    std::cout << "Redis put"
        << " id: " << id
        << " id size: " << strlen(id)
        << std::endl;    
#endif
    redisReply* reply = (redisReply*) redisCommand(c,"SET %s %s", id, s);

#ifdef REDIS_DEBUG
    std::cout << "Redis put"
        << " reply type: " << reply->type
        << " status: " << reply->str
        << std::endl;    
#endif

    freeReplyObject(reply);
}

void redis_put_binary(redisContext* c, const char* id,
        const char* s, size_t size) {
#ifdef REDIS_DEBUG
    std::cout << "Redis put binary"
        << " id: " << id
        << " data size: " << size
        << std::endl;    
#endif
    redisReply* reply = (redisReply*)
        redisCommand(c,"SET %s %b", id, s, size);

#ifdef REDIS_DEBUG
    std::cout << "Redis put binary"
        << " reply type: " << reply->type
        << " status: " << reply->str
        << " size: " << reply->len
        << std::endl;    
#endif

    freeReplyObject(reply);
}

void redis_put_binary_numid(redisContext* c, uint64_t id,
        const char* s, size_t size) {
#ifdef REDIS_DEBUG
    std::cout << "[REDIS] "
        << "redis put binary numid"
        << "id : " << id
        << " size: " << size << std::endl;
#endif

    char id_str[100];
    int ret = snprintf(id_str, 100, "%lu", id);
    if (ret < 0) {
        std::cout << "ERROR in sprintf" << std::endl;
    }
#ifdef REDIS_DEBUG
    std::cout << "[REDIS] "
        << "redis put binary2 with sprintf" 
        << "id_str:-" << id_str << "-"
        << std::endl;
#endif
    redis_put_binary(c, id_str, s, size);
#ifdef REDIS_DEBUG
    std::cout << "[REDIS] "
        << "redis put binary3 " << std::endl;
#endif
}

char* redis_get(redisContext* c, const char* id, int* len) {
#ifdef REDIS_DEBUG
    std::cout << "[REDIS] "
        << "redis get id: " << id << std::endl;
#endif
    redisReply* reply = (redisReply*)redisCommand(c,"GET %s", id);

    if (reply->type == REDIS_REPLY_NIL) {
#ifdef REDIS_DEBUG
        std::cout << "[REDIS] "
            << "redis returned nil"
            << " with id: " << id
            << std::endl;
#endif
        freeReplyObject(reply);
        return NULL;
    }

#ifdef REDIS_DEBUG
    std::cout << "[REDIS] "
        << "redis returned success len: " << reply->len << std::endl;
#endif

    char* ret = (char*)malloc(reply->len);
    memcpy(ret, reply->str, reply->len);

    if (len != nullptr) {
        *len = reply->len;
    }

    freeReplyObject(reply);
    return ret;
}

char* redis_binary_get(redisContext* c, const char* id, int* len) {
#ifdef REDIS_DEBUG
    std::cout << "[REDIS] "
        << "redis binary get id: " << id << std::endl;
#endif
    redisReply* reply = (redisReply*)redisCommand(c,"GET %b", id, (size_t)strlen(id));

    if (reply->type == REDIS_REPLY_NIL) {
#ifdef REDIS_DEBUG
        std::cout << "[REDIS] "
            << "redis returned nil"
            << " with id: " << id
            << std::endl;
#endif
        freeReplyObject(reply);
        return NULL;
    }

#ifdef REDIS_DEBUG
    std::cout << "[REDIS] "
        << "redis returned success len: " << reply->len << std::endl;
#endif

    char* ret = (char*)malloc(reply->len);
    memcpy(ret, reply->str, reply->len);

    if (len != nullptr) {
        *len = reply->len;
    }

    freeReplyObject(reply);
    return ret;
}

char* redis_get_numid(redisContext* c, uint64_t id, int* len) {
    char* id_str;
    if (asprintf(&id_str , "%lu", id) == -1) {
        return nullptr;
    }

    char* ret = redis_get(c, id_str, len);
    if (ret) {
#ifdef REDIS_DEBUG
        std::cout << "[REDIS] "
            << "redis got"
            << "id : " << id_str
            << " len: " << *len
            << std::endl;
        if (*len == 2) {
          std::cout << "[REDIS] "
            << "redis len 2 got ret: " << ret << std::endl;
        }
#endif
    } else {
#ifdef REDIS_DEBUG
        std::cout << "[REDIS] "
            << "redis get failed"
            << " redis return nullptr"
            << std::endl;
#endif
        free(id_str);
        return nullptr;
    }

#ifdef REDIS_DEBUG
    std::cout << "[REDIS] "
        << "return ret"
        << std::endl;
#endif
    free(id_str);
    return ret;
}

void redis_ping(redisContext* c) {
    redisReply* reply = (redisReply*)redisCommand(c,"PING");
    freeReplyObject(reply);
}

void redis_delete(redisContext* c, const char* id) {
    redisReply* reply = (redisReply*)
        redisCommand(c,"DEL %s", id );
    freeReplyObject(reply);
}

char** redis_mget_numid(redisContext* c, uint64_t n, uint64_t* id) {
    cmd[0] = 0;
    strcat(cmd, "MGET");
   
    // can be made more efficient by asprintf'ing directly into buffer 
    for (uint64_t i = 0; i < n; ++i) {
        char* id_str;
        if (asprintf(&id_str , "%lu", id[i]) == -1) {
            return nullptr;
        }
        strcat(cmd, " ");
        // we assume id_str fits
        strcat(cmd, id_str);
        free(id_str);
    }
    
    redisReply* reply = (redisReply*) redisCommand(c, cmd);

    char** ret_vec = new char*[n];
    for (uint64_t i = 0; i < n; ++i) {
        ret_vec[i] = new char[strlen(reply->element[i]->str) + 1];
        strcpy(ret_vec[i], reply->element[i]->str);
    }

    return ret_vec;
}

void redis_push_list(redisContext* r, const char* list_name, const char* data) {
    cmd[0] = 0;
    strcat(cmd, "LPUSH ");
    // we assume list_name fits
    strcat(cmd, list_name);
    strcat(cmd, " ");
    strcat(cmd, data);

    // XXX check for status    
    redisReply* reply = (redisReply*) redisCommand(r, cmd);
    
    freeReplyObject(reply);
}

char* redis_pop_list(redisContext* r, const char* list_name) {
    cmd[0] = 0;
    strcat(cmd, "LPOP ");
    strcat(cmd, list_name);
    redisReply* reply = (redisReply*) redisCommand(r, cmd);

    if (reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(reply);
        return NULL;
    }

    char* ret = (char*)malloc(reply->len);
    memcpy(ret, reply->str, reply->len);

    freeReplyObject(reply);
    return ret;
}

uint64_t redis_list_size(redisContext* r, const char* list_name) {
    cmd[0] = 0;
    strcat(cmd, "LLEN ");
    strcat(cmd, list_name);
    redisReply* reply = (redisReply*) redisCommand(r, cmd);

    uint64_t res = reply->integer;

    freeReplyObject(reply);

    return res;
}
/**
  * ASYNC
  */
redisAsyncContext* redis_async_connect(const char* hostname, int port) {
    redisAsyncContext *c = redisAsyncConnect(hostname, port);
    if (c->err) {
        printf("Error in redisAsyncConnect: %s\n", c->errstr);
        return NULL;
    }
    return c;
}

void redis_subscribe_callback(redisAsyncContext* c, sub_handler h,
    const char* name) {
  redisAsyncCommand(c, h, NULL, "SUBSCRIBE %s", name);
}

void redis_connect_callback(redisAsyncContext* c, conn_handler h) {
    redisAsyncSetConnectCallback(c, h);
}

void redis_disconnect_callback(redisAsyncContext* c, conn_handler h) {
    redisAsyncSetDisconnectCallback(c, h);
}

void redis_increment_counter(redisContext* r, const char* id, int* prev) {
    redisReply* reply = (redisReply*) redisCommand(r,"INCR %s", id);
    if (reply->type == REDIS_REPLY_ERROR) {
      throw std::runtime_error("Error incrementing counter");
    } 
    
    assert(reply->type == REDIS_REPLY_INTEGER);

    if (prev != nullptr) {
      *prev = reply->integer;
    }
    freeReplyObject(reply);
}

}
