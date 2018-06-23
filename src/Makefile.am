TOP_DIR = $(top_srcdir)/

AUTOMAKE_OPTIONS = foreign

CXX = g++

bin_PROGRAMS = parameter_server ps_test csv_to_libsvm

LIBS         =  -laws-cpp-sdk-s3 -laws-cpp-sdk-core \
		-lcurl -lssl -lcrypto -lz -ldl -lkrb5 -lk5crypto \
		-lall -lkeyutils -lresolv -lgflags

LINCLUDES    = -L$(TOP_DIR)/third_party/aws-sdk-cpp/build/aws-cpp-sdk-core/ \
	       -L$(TOP_DIR)/third_party/aws-sdk-cpp/build/aws-cpp-sdk-s3 \
	       -L$(TOP_DIR)/third_party/kerberos/src/lib \
	       -L$(TOP_DIR)/third_party/keyutils/ \
	       -L$(TOP_DIR)/third_party/gflags/lib/ \
	       -L$(TOP_DIR)/third_party/curl/curl/lib/.libs/

LDADD    = $(LINCLUDES) $(LIBS)
LDFLAGS  = -static-libgcc  -static  -Wl,--whole-archive \
	   -lpthread -Wl,--no-whole-archive

CPPFLAGS = -static -static-libgcc -static-libstdc++ \
           -I$(TOP_DIR) \
           -I$(TOP_DIR)/third_party/flatbuffers/include \
           -I$(TOP_DIR)/third_party/eigen_source \
	   -I$(TOP_DIR)/third_party/aws-sdk-cpp/aws-cpp-sdk-s3/include/ \
	   -I$(TOP_DIR)/third_party/aws-sdk-cpp/aws-cpp-sdk-core/include/  \
	   -I$(TOP_DIR)/third_party/gflags/include/ \
	   -ggdb -frecord-gcc-switches -O3

CPP_SOURCES = InputReader.cpp Utils.cpp \
              Dataset.cpp Matrix.cpp \
	      OptimizationMethod.cpp AdaGrad.cpp Momentum.cpp SGD.cpp \
	      Model.cpp LRModel.cpp SoftmaxModel.cpp SparseLRModel.cpp \
	      SparseMFModel.cpp MFModel.cpp\
              ModelGradient.cpp MlUtils.cpp Configuration.cpp \
              Checksum.cpp \
              ErrorSparseTask.cpp \
	      LogisticSparseTaskS3.cpp \
	      MFNetflixTask.cpp \
	      LoadingSparseTaskS3.cpp LoadingNetflixTask.cpp \
              TasksWait.cpp MurmurHash3.cpp \
	      S3Iterator.cpp S3SparseIterator.cpp \
	      PerformanceLambdaTask.cpp \
	      S3.cpp SparseDataset.cpp \
	      PSSparseServerTask.cpp PSSparseServerInterface.cpp

CPP_SOURCES_SOFTMAX = InputReader.cpp Utils.cpp \
              Dataset.cpp Matrix.cpp Model.cpp LRModel.cpp \
              ModelGradient.cpp MlUtils.cpp Configuration.cpp \
              Checksum.cpp SoftmaxModel.cpp \
              Serializers.cpp  \
              SparseDataset.cpp MurmurHash3.cpp

parameter_server_SOURCES  = parameter_server.cpp $(CPP_SOURCES)

ps_test_SOURCES = PSSparseServerTaskTest.cpp $(CPP_SOURCES)

csv_to_libsvm_SOURCES = CriteoCsvToLibSVM.cpp $(CPP_SOURCES)
