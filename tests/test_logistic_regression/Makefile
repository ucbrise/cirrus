CXX=g++
CXXFLAGS=-Wall -ansi -O3 -std=c++14 -ggdb

TOP_DIR=../../../..
THIRD_PARTY_DIR=../../third_party/
CIRRUS_SRC_DIR=../../src/
CIRRUS_SRC_FILES=$(CIRRUS_SRC_DIR)/Configuration.cpp \
		    $(CIRRUS_SRC_DIR)/Dataset.cpp \
		    $(CIRRUS_SRC_DIR)/Matrix.cpp \
		    $(CIRRUS_SRC_DIR)/LRModel.cpp \
		    $(CIRRUS_SRC_DIR)/SparseLRModel.cpp \
		    $(CIRRUS_SRC_DIR)/S3Iterator.cpp \
		    $(CIRRUS_SRC_DIR)/S3SparseIterator.cpp \
		    $(CIRRUS_SRC_DIR)/Utils.cpp \
		    $(CIRRUS_SRC_DIR)/MlUtils.cpp \
		    $(CIRRUS_SRC_DIR)/S3.cpp \
		    $(CIRRUS_SRC_DIR)/Checksum.cpp \
		    $(CIRRUS_SRC_DIR)/MurmurHash3.cpp \
		    $(CIRRUS_SRC_DIR)/ModelGradient.cpp \
		    $(CIRRUS_SRC_DIR)/InputReader.cpp \
		    $(CIRRUS_SRC_DIR)/PSSparseServerInterface.cpp \
		    $(CIRRUS_SRC_DIR)/SparseMFModel.cpp \
		    $(CIRRUS_SRC_DIR)/MFModel.cpp \
		    $(CIRRUS_SRC_DIR)/SparseDataset.cpp


LIBFLAGS =-static-libgcc  -static \
	  -Wl,--whole-archive -lpthread -Wl,--no-whole-archive \
	  -L$(THIRD_PARTY_DIR)/aws-sdk-cpp/build/aws-cpp-sdk-core/ \
	  -L$(THIRD_PARTY_DIR)/aws-sdk-cpp/build/aws-cpp-sdk-s3 \
	  -L/home/ec2-user/kerberos/krb5-1.15.2/src/lib \
	  -L/home/ec2-user/keyutils/keyutils-1.5.10 \
	  -laws-cpp-sdk-s3 -laws-cpp-sdk-core \
	  -lutils -lcurl -lssl -lcrypto -lz -ldl -lkrb5 -lk5crypto \
	  -lall -lkeyutils -lhiredis

INCLUDES=-I$(CIRRUS_SRC_DIR) \
	 -I$(THIRD_PARTY_DIR)/aws-sdk-cpp/aws-cpp-sdk-s3/include/ \
	 -I$(THIRD_PARTY_DIR)/aws-sdk-cpp/aws-cpp-sdk-core/include/

PROJ1=check_correctness_cirrus_gd
PROJ2=check_correctness_cirrus_sgd
PROJ3=check_correctness_cirrus_sgd_sparse
PROJ4=check_correctness_cirrus_sgd_rcv1
PROJ5=check_correctness_cirrus_sgd_s3

all: $(PROJ1) $(PROJ2) $(PROJ3) $(PROJ4) $(PROJ5)

check_correctness_cirrus_gd: $(PROJ1).cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ1).cpp $(CIRRUS_SRC_FILES)\
	  -o $@ $(LIBFLAGS) 

check_correctness_cirrus_sgd: $(PROJ2).cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ2).cpp $(CIRRUS_SRC_FILES) \
	  -o $@ $(LIBFLAGS) 

check_correctness_cirrus_sgd_sparse: $(PROJ3).cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ3).cpp $(CIRRUS_SRC_FILES) \
	  -o $@ $(LIBFLAGS) 

check_correctness_cirrus_sgd_rcv1: $(PROJ4).cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ4).cpp $(CIRRUS_SRC_FILES) \
	  -o $@ $(LIBFLAGS) 

check_correctness_cirrus_sgd_s3: $(PROJ5).cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ5).cpp $(CIRRUS_SRC_FILES) \
	  -o $@ $(LIBFLAGS) 

clean:
	rm -rf a.out $(PROJ1) $(PROJ2) $(PROJ3) $(PROJ4) $(PROJ5)
