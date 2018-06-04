CXX=g++
CXXFLAGS=-Wall -ansi -O3 -std=c++14 -ggdb

TOP_DIR=../../../..
CIRRUS_ML_DIR=../..
CIRRUS_ML_SRC_FILES=$(CIRRUS_ML_DIR)/Configuration.cpp \
		    $(CIRRUS_ML_DIR)/Dataset.cpp \
		    $(CIRRUS_ML_DIR)/Matrix.cpp \
		    $(CIRRUS_ML_DIR)/LRModel.cpp \
		    $(CIRRUS_ML_DIR)/SparseLRModel.cpp \
		    $(CIRRUS_ML_DIR)/S3Iterator.cpp \
		    $(CIRRUS_ML_DIR)/S3SparseIterator.cpp \
		    $(CIRRUS_ML_DIR)/Utils.cpp \
		    $(CIRRUS_ML_DIR)/MlUtils.cpp \
		    $(CIRRUS_ML_DIR)/S3.cpp \
		    $(CIRRUS_ML_DIR)/Checksum.cpp \
		    $(CIRRUS_ML_DIR)/MurmurHash3.cpp \
		    $(CIRRUS_ML_DIR)/ModelGradient.cpp \
		    $(CIRRUS_ML_DIR)/InputReader.cpp \
		    $(CIRRUS_ML_DIR)/PSSparseServerInterface.cpp \
		    $(CIRRUS_ML_DIR)/SparseMFModel.cpp \
		    $(CIRRUS_ML_DIR)/MFModel.cpp \
		    $(CIRRUS_ML_DIR)/SparseDataset.cpp


LIBFLAGS =-static-libgcc  -static \
	  -Wl,--whole-archive -lpthread -Wl,--no-whole-archive \
	  -L$(TOP_DIR)/src/utils/ \
	  -L/home/ec2-user/cirrus/third_party/aws-sdk-cpp/build/aws-cpp-sdk-core/ \
	  -L/home/ec2-user/cirrus/third_party/aws-sdk-cpp/build/aws-cpp-sdk-s3 \
	  -L/home/ec2-user/kerberos/krb5-1.15.2/src/lib \
	  -L/home/ec2-user/keyutils/keyutils-1.5.10 \
	  -laws-cpp-sdk-s3 -laws-cpp-sdk-core \
	  -lutils -lcurl -lssl -lcrypto -lz -ldl -lkrb5 -lk5crypto \
	  -lall -lkeyutils -lhiredis

INCLUDES=-I.. \
	 -I$(TOP_DIR) \
	 -I$(CIRRUS_ML_DIR) \
	 -I$(TOP_DIR)/src/ \
	 -I/home/ec2-user/cirrus/third_party/aws-sdk-cpp/aws-cpp-sdk-s3/include/ \
	 -I/home/ec2-user/cirrus/third_party/aws-sdk-cpp/aws-cpp-sdk-core/include/ \
	 -I$(TOP_DIR)/third_party/eigen_source \
	 -I$(TOP_DIR)/third_party/libcuckoo \
	 -I$(TOP_DIR)/third_party/flatbuffers/include \
	 -I$(TOP_DIR)/third_party/hiredis

PROJ1=check_correctness_cirrus_gd
PROJ2=check_correctness_cirrus_sgd
PROJ3=check_correctness_cirrus_sgd_sparse
PROJ4=check_correctness_cirrus_sgd_rcv1
PROJ5=check_correctness_cirrus_sgd_s3

#CIRRUS_ML_SRC_FILES := $(wildcard $(CIRRUS_ML_DIR)/*.cpp)

all: $(PROJ1) $(PROJ2) $(PROJ3) $(PROJ4) $(PROJ5)

check_correctness_cirrus_gd: $(PROJ1).cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ1).cpp $(CIRRUS_ML_SRC_FILES)\
	  -o $@ $(LIBFLAGS) 

check_correctness_cirrus_sgd: $(PROJ2).cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ2).cpp $(CIRRUS_ML_SRC_FILES) \
	  -o $@ $(LIBFLAGS) 

check_correctness_cirrus_sgd_sparse: $(PROJ3).cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ3).cpp $(CIRRUS_ML_SRC_FILES) \
	  -o $@ $(LIBFLAGS) 

check_correctness_cirrus_sgd_rcv1: $(PROJ4).cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ4).cpp $(CIRRUS_ML_SRC_FILES) \
	  -o $@ $(LIBFLAGS) 

check_correctness_cirrus_sgd_s3: $(PROJ5).cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ5).cpp $(CIRRUS_ML_SRC_FILES) \
	  -o $@ $(LIBFLAGS) 

clean:
	rm -rf a.out $(PROJ1) $(PROJ2) $(PROJ3) $(PROJ4) $(PROJ5)
