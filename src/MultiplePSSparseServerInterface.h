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
  MultiplePSSparseServerInterface(std::vector<std::string> ps_ips, std::vector<uint64_t> ps_ports);
  void send_gradient(const LRSparseGradient& gradient);
  std::shared_ptr<CirrusModel> get_full_model();
  SparseLRModel get_lr_sparse_model(const SparseDataset& ds, const Configuration& config);


 private:
  std::vector<PSSparseServerInterface*> psints;


};

}
#endif //  MULTIPLE_PS_SPARSE_SERVER_INTERFACE
