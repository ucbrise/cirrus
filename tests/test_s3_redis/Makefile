CXXFLAGS=-std=c++11 -Wall
INCLUDES=-I../../../third_party/hiredis/ -I../benchmark_redis/ \
	 -I../test_aws/ -I.. \
	 -I/home/ec2-user/cirrus/third_party/aws-sdk-cpp/aws-cpp-sdk-s3/include/ \
        -I/home/ec2-user/cirrus/third_party/aws-sdk-cpp/aws-cpp-sdk-core/include/
LDFLAGS=-L/home/ec2-user/cirrus/third_party/aws-sdk-cpp/build/aws-cpp-sdk-core/ \
         -L/home/ec2-user/cirrus/third_party/aws-sdk-cpp/build/aws-cpp-sdk-s3 \
         -L/home/ec2-user/kerberos/krb5-1.15.2/src/lib \
         -L/home/ec2-user/keyutils/keyutils-1.5.10 \
	 -L../../../third_party/hiredis/ \
	 -laws-cpp-sdk-s3 -laws-cpp-sdk-core \
	 -lcurl -lssl -lcrypto -lz -ldl -lkrb5 -lk5crypto \
	 -lall -lkeyutils -lhiredis

LIBFLAGS  = -static-libgcc  -static  -Wl,--whole-archive -lpthread -Wl,--no-whole-archive

all:  
	g++ $(CXXFLAGS) test_s3_redis.cpp ../Redis.cpp ../S3.cpp $(INCLUDES) $(LDFLAGS) $(LIBFLAGS) -o test_s3_redis
	g++ $(CXXFLAGS) test_s3_contention.cpp ../S3.cpp $(INCLUDES) $(LDFLAGS) $(LIBFLAGS) -o test_s3_contention

clean:
	rm -rf a.out test_s3_redis
