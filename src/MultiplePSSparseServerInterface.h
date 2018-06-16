#ifndef MULTIPLE_PS_SPARSE_SERVER_INTERFACE
#define MULTIPLE_PS_SPARSE_SERVER_INTERFACE

#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>
#include <iostream>
#include "ModelGradient.h"
#include "Utils.h"
#include "SparseLRModel.h"
#include "PSSparseServerInterface.h"

namespace cirrus {

class MultiplePSSparseServerInterface {
 
 public:
  MultiplePSSparseServerInterface(std::vector<std::string> param_ips);
  void send_gradient(const cirrus::LRSparseGradient*);
  std::unique_ptr<cirrus::CirrusModel> get_full_model();
  cirrus::SparseLRModel get_lr_sparse_model(const cirrus::SparseDataset& ds, const cirrus::Configuration& config);


 private:
  std::vector<cirrus::PSSparseServerInterface> psints;


};

}
#endif //  MULTIPLE_PS_SPARSE_SERVER_INTERFACE
