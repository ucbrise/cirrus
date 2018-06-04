#ifndef _CIRRUS_REDIS_H_
#define _CIRRUS_REDIS_H_

extern "C" {

#include <hiredis.h>
#include <async.h>
#include <string.h>
#include <stdlib.h>

redisContext* redis_connect(const char* hostname, int port);
redisAsyncContext* redis_async_connect(const char* hostname, int port);

void redis_put_binary(redisContext* c, const char* id,
                             const char* s, size_t size);
void redis_put_binary_numid(redisContext* c, uint64_t id,
                             const char* s, size_t size);

char* redis_get(redisContext* c, const char* id, int* len = nullptr);
char* redis_get_numid(redisContext* c, uint64_t id, int* len = nullptr);
char** redis_mget_numid(redisContext* c, uint64_t n, uint64_t* id);

void redis_ping(redisContext* c);
void redis_delete(redisContext* c, const char* id);
   
typedef void(*sub_handler)(redisAsyncContext*, void*, void*);
typedef void(*conn_handler)(const redisAsyncContext*, int);
void redis_subscribe_callback(redisAsyncContext*, sub_handler,
    const char*);
void redis_connect_callback(redisAsyncContext*, conn_handler);
void redis_disconnect_callback(redisAsyncContext*, conn_handler);

char* redis_binary_get(redisContext* c, const char* id, int* len);

void redis_increment_counter(redisContext*, const char*, int* = nullptr);

// Lists
#if 0
void redis_push_list(redisContext*, const char* list_name, const char* data);
char* redis_pop_list(redisContext*, const char* list_name);
uint64_t redis_list_size(redisContext*, const char* list_name);
#endif

}

#endif  // _CIRRUS_REDIS_H_
