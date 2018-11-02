AUTOMAKE_OPTIONS = foreign

CXX=g++
CXXFLAGS=-Wall -ansi -O3 -std=c++17 -ggdb

bin_PROGRAMS = test_register_worker test_keyvalue

TOP_DIR=../../../..
THIRD_PARTY_DIR=../../third_party
CIRRUS_SRC_DIR=../../src
CIRRUS_SRC_FILES=$(CIRRUS_SRC_DIR)/Configuration.cpp \
		    $(CIRRUS_SRC_DIR)/Dataset.cpp \
		    $(CIRRUS_SRC_DIR)/Matrix.cpp \
		    $(CIRRUS_SRC_DIR)/LRModel.cpp \
		    $(CIRRUS_SRC_DIR)/SparseLRModel.cpp \
		    $(CIRRUS_SRC_DIR)/S3SparseIterator.cpp \
		    $(CIRRUS_SRC_DIR)/Utils.cpp \
		    $(CIRRUS_SRC_DIR)/MlUtils.cpp \
		    $(CIRRUS_SRC_DIR)/S3.cpp \
		    $(CIRRUS_SRC_DIR)/OptimizationMethod.cpp \
		    $(CIRRUS_SRC_DIR)/SGD.cpp \
		    $(CIRRUS_SRC_DIR)/Checksum.cpp \
		    $(CIRRUS_SRC_DIR)/MurmurHash3.cpp \
		    $(CIRRUS_SRC_DIR)/ModelGradient.cpp \
		    $(CIRRUS_SRC_DIR)/InputReader.cpp \
		    $(CIRRUS_SRC_DIR)/PSSparseServerInterface.cpp \
		    $(CIRRUS_SRC_DIR)/PSSparseServerTask.cpp \
		    $(CIRRUS_SRC_DIR)/SparseMFModel.cpp \
		    $(CIRRUS_SRC_DIR)/MFModel.cpp \
		    $(CIRRUS_SRC_DIR)/Nesterov.cpp \
		    $(CIRRUS_SRC_DIR)/AdaGrad.cpp \
		    $(CIRRUS_SRC_DIR)/Momentum.cpp \
		    $(CIRRUS_SRC_DIR)/S3Iterator.cpp \
		    $(CIRRUS_SRC_DIR)/S3IteratorLibsvm.cpp \
		    $(CIRRUS_SRC_DIR)/S3Client.cpp \
		    $(CIRRUS_SRC_DIR)/SparseDataset.cpp

LINCLUDES = -L$(THIRD_PARTY_DIR)/kerberos/src/lib \
	    -L$(THIRD_PARTY_DIR)/keyutils/ \
	    -L$(THIRD_PARTY_DIR)/gflags/lib/ \
	    -L$(THIRD_PARTY_DIR)/curl/curl/lib/.libs/ \
	    -L$(THIRD_PARTY_DIR)/aws-sdk-cpp/build/aws-cpp-sdk-core/ \
	    -L$(THIRD_PARTY_DIR)/aws-sdk-cpp/build/aws-cpp-sdk-s3 \
	    -L/home/ec2-user/kerberos/krb5-1.15.2/src/lib \
	    -L/home/ec2-user/keyutils/keyutils-1.5.10

LIBS= -laws-cpp-sdk-s3 -laws-cpp-sdk-core \
      -lcurl -lssl -lcrypto -lz -ldl -lkrb5 -lk5crypto \
      -lall -lkeyutils -lresolv -lgflags

LDFLAGS  =-static-libgcc  -static \
	  -Wl,--whole-archive -lpthread -Wl,--no-whole-archive

LDADD    = $(LINCLUDES) $(LIBS)

AM_CPPFLAGS=-I$(CIRRUS_SRC_DIR) \
	 -I$(THIRD_PARTY_DIR)/aws-sdk-cpp/aws-cpp-sdk-s3/include/ \
	 -I$(THIRD_PARTY_DIR)/aws-sdk-cpp/aws-cpp-sdk-core/include/ \
	 -I$(THIRD_PARTY_DIR)/eigen_source/

test_register_worker_SOURCES = test_register_worker.cpp $(CIRRUS_SRC_FILES)
test_keyvalue_SOURCES = test_keyvalue.cpp $(CIRRUS_SRC_FILES)

clean:
	rm -rf a.out
