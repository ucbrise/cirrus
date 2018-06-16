#include <cassert>
#include "PSSparseServerInterface.h"
#include "MultiplePSSparseServerInterface.h"

#undef DEBUG

#define MAX_MSG_SIZE (1024*1024)

namespace cirrus {

MultiplePSSparseServerInterface::MultiplePSSparseServerInterface(std::vector<std::string> param_ips ) {

  for (int i = 0; i < param_ips.size(); i++) { // replace 2 with num_servers

    // FIXME: need to un-hardcode this port number
    psints.push_back(PSSparseServerInterface(param_ips[i], 1337));
  }
}

void MultiplePSSparseServerInterface::send_gradient(const LRSparseGradient* gradient) {
  int num_ps = psint.size();
  uint32_t operation = SEND_LR_GRADIENT;
#ifdef DEBUG
  std::cout << "Sending gradient" << std::endl;
#endif
  for (auto psint : psints) {
    int ret = psint.send_wrapper(&operation, sizeof(uint32_t));
    if (ret == -1)
      throw std::runtime_error("Error sending operation");
  }

  
  uint32_t size = gradient.getSerializedSize();
#ifdef DEBUG
  std::cout << "Sending gradient with size: " << size << std::endl;
#endif
  ret = send(sock, &size, sizeof(uint32_t), 0);
  if (ret == -1) {
    throw std::runtime_error("Error sending grad size");
  }
  
  char data[size];
  gradient.serialize(data);
  ret = send_all(sock, data, size);
  if (ret == 0) {
    throw std::runtime_error("Error sending grad");
  }
    
}


SparseLRModel MultiplePSSparseServerInterface::get_lr_sparse_model(const SparseDataset& ds, const Configuration& config) {
  // Initialize variables
  SparseLRModel model(0);
  //std::unique_ptr<CirrusModel> model = std::make_unique<SparseLRModel>(0);
  // we don't know the number of weights to start with
  char** msg_lst = new char*[this->num_servers];
  char** msg_begin_lst = new char*[this->num_servers];
  uint32_t* num_weights_lst = new uint32_t[this->num_servers];
  for (int i = 0; i < this->num_servers; i++) {
    msg_lst[i] = new char[MAX_MSG_SIZE];
    msg_begin_lst[i] = msg_lst[i];
    num_weights_lst[i] = 0;
    store_value<uint32_t>(msg_lst[i], num_weights_lst[i]); // just make space for the number of weights
  }


  // Split the dataset based on which server data belongs to.
  // XXX consider optimizing this
  for (const auto& sample : ds.data_) {
    for (const auto& w : sample) {
      int server_index = w.first % this->num_servers;
      int data_index = (w.first - server_index) / num_servers;
      //std::cout << "[converted] " << w.first << " to " << data_index << std::endl;
      store_value<uint32_t>(msg_lst[server_index], data_index);
      num_weights_lst[server_index]++;
    }
  }

  // we get the model subset with just the right amount of weights
  for (int i = 0; i < NUM_PS; i++) {
    psint[i]->get_lr_sparse_model_inplace_sharded(model, config, msg_begin_lst[i], num_weights_lst[i], i);
  }

  for (int i = 0; i < this->num_servers; i++) {
    delete[] msg_begin_lst[i];
  }


  delete[] msg_begin_lst;
  delete[] msg_lst;
  delete[] num_weights_lst;
  //return psint[0]->get_lr_sparse_model(ds, config);
  return model;
}


std::unique_ptr<CirrusModel> MultiplePSSparseServerInterface::get_full_model() {
  //SparseLRModel model(0);
  std::unique_ptr<CirrusModel> model = std::make_unique<SparseLRModel>(0);
  // placeholder for now NOT CORRECT
  for (int i = 0; i < NUM_PS; i++) {
    model = psint[i]->get_full_model(false, i, std::move(model));

  }
  return model;
}


}