CXX=g++
CXXFLAGS=-Wall -ansi -O3 -std=c++14 -pthread -ggdb
TOP_DIR=../../../..
CIRRUS_ML_DIR=../..
LIBFLAGS =-static-libgcc  -static \
	  -Wl,--whole-archive -lpthread -Wl,--no-whole-archive \
	  -L$(TOP_DIR)/src/utils/ \
	  -lutils
INCLUDES= -I$(CIRRUS_ML_DIR)/ \
	 -I$(TOP_DIR)/ \
	 -I$(TOP_DIR)/src/ \
	 -I$(TOP_DIR)/third_party/eigen_source

SOURCES=$(CIRRUS_ML_DIR)/Dataset.cpp  $(CIRRUS_ML_DIR)/ModelGradient.cpp $(CIRRUS_ML_DIR)/Utils.cpp \
	$(CIRRUS_ML_DIR)/Matrix.cpp   $(CIRRUS_ML_DIR)/MlUtils.cpp       $(CIRRUS_ML_DIR)/Checksum.cpp $(CIRRUS_ML_DIR)/InputReader.cpp \
	$(CIRRUS_ML_DIR)/SparseDataset.cpp $(CIRRUS_ML_DIR)/MFModel.cpp  $(CIRRUS_ML_DIR)/Model.cpp \
	$(CIRRUS_ML_DIR)/MurmurHash3.cpp $(CIRRUS_ML_DIR)/Configuration.cpp

PROJ1=test_mf
PROJ2=test_mf_threads

all: $(PROJ1) $(PROJ2)

$(PROJ1): $(PROJ1).cpp $(SOURCES)
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ1).cpp $(SOURCES) \
	  -o $@ $(LIBFLAGS) 

$(PROJ2): $(PROJ2).cpp $(SOURCES)
	$(CXX) $(INCLUDES) $(CXXFLAGS) \
	  $(PROJ2).cpp $(SOURCES) \
	  -o $@ $(LIBFLAGS) 

clean:
	rm -rf a.out $(PROJ1) $(PROJ2)
