all:  
	g++ -std=c++11 -Wall benchmark_redis.cpp ../Redis.cpp -I../../../third_party/hiredis/ -L../../../third_party/hiredis/ -lhiredis -static -o benchmark_redis -static-libgcc -static-libstdc++

clean:
	rm -fr a.out benchmark_redis
