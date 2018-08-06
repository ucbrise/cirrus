#include <cassert>
#include "PSSparseServerInterface.h"
#include "MultiplePSSparseServerInterface.h"
#include "Constants.h"

#undef DEBUG

#define MAX_MSG_SIZE (1024*1024)

namespace cirrus {

MultiplePSSparseServerInterface::MultiplePSSparseServerInterface(std::vector<std::string> param_ips, std::vector<uint64_t> ps_ports ) {
  
  std::cout << "Starting Multiple PS " << param_ips.size() << std::endl;
  for (int i = 0; i < param_ips.size(); i++) { // replace 2 with num_servers
    std::cout << "Attempting connection to " << param_ips[i] << ":" << ps_ports[i] << std::endl;
    auto ptr = new PSSparseServerInterface(param_ips[i], ps_ports[i]);
        ptr->connect();
    psints.push_back(ptr);
    std::cout << "Connected!!!" << std::endl;
  }

}

void MultiplePSSparseServerInterface::send_gradient(const LRSparseGradient& gradient) {
  int num_ps = psints.size();
  uint32_t operation = SEND_LR_GRADIENT;
#ifdef DEBUG
  std::cout << "Sending gradient" << std::endl;
#endif
  int ret;
  for (auto psint : psints) {
    ret = psint->send_wrapper(operation, sizeof(uint32_t));
    if (ret == -1)
      throw std::runtime_error("Error sending operation");

  }

  uint32_t size = gradient.getShardSerializedSize(num_ps);
  char data[size];
  auto starts_and_size = gradient.shard_serialize(data, num_ps);

  for (int i = 0; i < num_ps; i++) {
    auto psint = psints[i];
    auto sas = starts_and_size[i];

    ret = psint->send_wrapper(std::get<1>(sas), sizeof(uint32_t));
    if (ret == -1) {
      throw std::runtime_error("Error sending grad size");
    }
    ret = psint->send_all_wrapper(data + std::get<0>(sas), std::get<1>(sas));
    if (ret == 0) {
      throw std::runtime_error("Error sending grad");
    }

  }

    
}


SparseLRModel MultiplePSSparseServerInterface::get_lr_sparse_model(const SparseDataset& ds, const Configuration& config) {
  // Initialize variables
  SparseLRModel model(0);
  //std::unique_ptr<CirrusModel> model = std::make_unique<SparseLRModel>(0);
  // we don't know the number of weights to start with
  int num_servers = psints.size();
  char** msg_lst = new char*[num_servers];
  char** msg_begin_lst = new char*[num_servers];
  uint32_t* num_weights_lst = new uint32_t[num_servers];
  for (int i = 0; i < num_servers; i++) {
    msg_lst[i] = new char[MAX_MSG_SIZE];
    msg_begin_lst[i] = msg_lst[i];
    num_weights_lst[i] = 0;
    store_value<uint32_t>(msg_lst[i], num_weights_lst[i]); // just make space for the number of weights
  }


  // Split the dataset based on which server data belongs to.
  // XXX consider optimizing this
  for (const auto& sample : ds.data_) {
    for (const auto& w : sample) {
      int server_index = w.first % num_servers;
      int data_index = (w.first - server_index) / num_servers;
      //std::cout << "[converted] " << w.first << " to " << data_index << std::endl;
      store_value<uint32_t>(msg_lst[server_index], data_index);
      num_weights_lst[server_index]++;
    }
  }

  // we get the model subset with just the right amount of weights
  for (int i = 0; i < num_servers; i++) {
    psints[i]->get_lr_sparse_model_inplace_sharded(model, config, msg_begin_lst[i], num_weights_lst[i], i, num_servers);
  }

  /*
  for (int i = 0; i < num_servers; i++) {
    std::cout << "msg_begin_list[i] is " << i << " " << static_cast<void*>(msg_begin_lst[i]) << std::endl;
    delete[] msg_begin_lst[i];
  }
  */

  delete[] msg_begin_lst;
  delete[] msg_lst;
  delete[] num_weights_lst;
  //return psint[0]->get_lr_sparse_model(ds, config);
  return model;
}


std::unique_ptr<CirrusModel> MultiplePSSparseServerInterface::get_full_model() {
  std::unique_ptr<SparseLRModel> model = std::make_unique<SparseLRModel>(0);
  // placeholder for now NOT CORRECT
  for (int i = 0; i < psints.size(); i++) {
    psints[i]->get_full_model_inplace(model, i, psints.size());
  }
  return std::move(model);

}


}
