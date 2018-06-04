CXXFLAGS=-ansi -std=c++11 -Wall
INCLUDES=-I../../../third_party/hiredis/ -I../benchmark_redis/ \
	 -I..
LDFLAGS=-L../../../third_party/hiredis/ \
	 -lhiredis \
	 -L/home/ec2-user/libevent/install_dir/lib/ -levent

LIBFLAGS  = -static-libgcc  -static  -Wl,--whole-archive -lpthread -Wl,--no-whole-archive

all:   test_redis_pubsub test_redis_incr

test_redis_pubsub:test_redis_pubsub.cpp
	g++ $(CXXFLAGS) test_redis_pubsub.cpp $(INCLUDES)  $(LDFLAGS) $(LIBFLAGS) -o test_redis_pubsub

test_redis_incr:test_redis_incr.cpp ../Redis.cpp
	g++ $(CXXFLAGS) test_redis_incr.cpp ../Redis.cpp $(INCLUDES)  $(LDFLAGS) $(LIBFLAGS) -o test_redis_incr


clean:
	rm -rf a.out test_redis_pubsub test_redis_incr

run:
	LD_LIBRARY_PATH=/usr/local/lib/ ./test
