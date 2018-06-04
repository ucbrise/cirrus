INCLUDE=-I/home/ec2-user/cirrus/third_party/aws-sdk-cpp/aws-cpp-sdk-s3/include/ \
        -I/home/ec2-user/cirrus/third_party/aws-sdk-cpp/aws-cpp-sdk-core/include/ \
        -I./../../ \
        -I./../..//examples/sparsehash/src/ \
        -I./../..//src -I./../..//third_party/flatbuffers/include \
        -I./../..//third_party/eigen_source \
        -isystem ./../..//third_party/libcuckoo/

LD_PATHS=-L/home/ec2-user/cirrus/third_party/aws-sdk-cpp/build/aws-cpp-sdk-core/ \
         -L/home/ec2-user/cirrus/third_party/aws-sdk-cpp/build/aws-cpp-sdk-s3 \
         -L/home/ec2-user/kerberos/krb5-1.15.2/src/lib \
         -L/home/ec2-user/keyutils/keyutils-1.5.10

LINKS=-laws-cpp-sdk-s3 -laws-cpp-sdk-core \
      -lcurl -lssl -lcrypto -lz -ldl -lkrb5 -lk5crypto \
      -lall -lkeyutils -lhiredis

CPPFLAGS=-O3 -fPIC -std=c++1z -Werror -static -static-libgcc -static-libstdc++ \
         -ggdb -static  -Wl,--whole-archive -lpthread -Wl,--no-whole-archive

test_aws: test_aws.cpp S3.h
	g++ test_aws.cpp $(CPPFLAGS) $(LD_PATHS) $(INCLUDE) $(LINKS) -o test_aws
