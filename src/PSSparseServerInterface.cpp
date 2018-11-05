#include <cassert>
#include <stdexcept>
#include "PSSparseServerInterface.h"
#include "Constants.h"
#include "MFModel.h"
#include "Checksum.h"
#include "Constants.h"

#undef DEBUG

#define MAX_MSG_SIZE (1024*1024)

namespace cirrus {

PSSparseServerInterface::PSSparseServerInterface(const std::string& ip, int port) :
  ip(ip), port(port) {

  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    throw std::runtime_error("Error when creating socket.");
  }
  int opt = 1;
  if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt))) {
    throw std::runtime_error("Error setting socket options.");
  }

  serv_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) != 1) {
    throw std::runtime_error("Address family invalid or invalid "
        "IP address passed in");
  }
  // Save the port in the info
  serv_addr.sin_port = htons(port);
  std::memset(serv_addr.sin_zero, 0, sizeof(serv_addr.sin_zero));
}

void PSSparseServerInterface::connect() {
  int ret = ::connect(sock, (struct sockaddr*) &serv_addr, sizeof(serv_addr));
  if (ret < 0) {
    throw std::runtime_error("Failed to make contact with server with ip: " +
                             ip + " port: " + std::to_string(port) + "\n");
  }
}

PSSparseServerInterface::~PSSparseServerInterface() {
  if (sock != -1) {
    close(sock);
  }
}

void PSSparseServerInterface::send_lr_gradient(const LRSparseGradient& gradient) {
  uint32_t operation = SEND_LR_GRADIENT;
#ifdef DEBUG
  std::cout << "Sending gradient" << std::endl;
#endif
  int ret = send(sock, &operation, sizeof(uint32_t), 0);
  if (ret == -1) {
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

void PSSparseServerInterface::get_lr_sparse_model_inplace(const SparseDataset& ds, SparseLRModel& lr_model,
    const Configuration& config) {
#ifdef DEBUG
  std::cout << "Getting LR sparse model inplace" << std::endl;
#endif
  // we don't know the number of weights to start with
  char* msg = new char[MAX_MSG_SIZE];
  char* msg_begin = msg; // need to keep this pointer to delete later

  uint32_t num_weights = 0;
  store_value<uint32_t>(msg, num_weights); // just make space for the number of weights
  for (const auto& sample : ds.data_) {
    for (const auto& w : sample) {
      store_value<uint32_t>(msg, w.first); // encode the index
      num_weights++;
    }
  }
  msg = msg_begin;
  store_value<uint32_t>(msg, num_weights); // store correct value here
#ifdef DEBUG
  assert(std::distance(msg_begin, msg) < MAX_MSG_SIZE);
  std::cout << std::endl;
#endif

#ifdef DEBUG
  std::cout << "Sending operation and size" << std::endl;
#endif
  uint32_t operation = GET_LR_SPARSE_MODEL;
  if (send_all(sock, &operation, sizeof(uint32_t)) == -1) {
    throw std::runtime_error("Error getting sparse lr model");
  }
  uint32_t msg_size = sizeof(uint32_t) + sizeof(uint32_t) * num_weights;
#ifdef DEBUG
  std::cout << "msg_size: " << msg_size
    << " num_weights: " << num_weights
    << std::endl;
#endif
  send_all(sock, &msg_size, sizeof(uint32_t));
  if (send_all(sock, msg_begin, msg_size) == -1) {
    throw std::runtime_error("Error getting sparse lr model");
  }
  uint32_t to_receive_size = sizeof(FEATURE_TYPE) * num_weights;

#ifdef DEBUG
  std::cout << "Receiving " << to_receive_size << " bytes" << std::endl;
#endif
  char* buffer = new char[to_receive_size];
  read_all(sock, buffer, to_receive_size); //XXX this takes 2ms once every 5 runs

#ifdef DEBUG
  std::cout << "Loading model from memory" << std::endl;
#endif
  // build a truly sparse model and return
  // XXX this copy could be avoided
  lr_model.loadSerializedSparse((FEATURE_TYPE*)buffer, (uint32_t*)msg, num_weights, config);
  
  delete[] msg_begin;
  delete[] buffer;
}

SparseLRModel PSSparseServerInterface::get_lr_sparse_model(const SparseDataset& ds, const Configuration& config) {
  SparseLRModel model(0);
  get_lr_sparse_model_inplace(ds, model, config);
  return std::move(model);
}

std::unique_ptr<CirrusModel> PSSparseServerInterface::get_full_model(
    bool isCollaborative //XXX use a better argument here
    ) {
#ifdef DEBUG
  std::cout << "Getting full model isCollaborative: " << isCollaborative << std::endl;
#endif
  if (isCollaborative) {
    uint32_t operation = GET_MF_FULL_MODEL;
    send_all(sock, &operation, sizeof(uint32_t));
    uint32_t to_receive_size;
    read_all(sock, &to_receive_size, sizeof(uint32_t));

    char* buffer = new char[to_receive_size];
    read_all(sock, buffer, to_receive_size);
    
    std::cout
      << " buffer checksum: " << crc32(buffer, to_receive_size)
      << std::endl;

    // build a sparse model and return
    std::unique_ptr<CirrusModel> model = std::make_unique<MFModel>(
        (FEATURE_TYPE*)buffer, 0, 0, 0); //XXX fix this
    delete[] buffer;
    return model;
  } else {
    uint32_t operation = GET_LR_FULL_MODEL;
    send_all(sock, &operation, sizeof(uint32_t));
    int model_size;
    if (read_all(sock, &model_size, sizeof(int)) == 0) {
      throw std::runtime_error("Error talking to PS");
    }
    char* model_data = new char[sizeof(int) + model_size * sizeof(FEATURE_TYPE)];
    char*model_data_ptr = model_data;
    store_value<int>(model_data_ptr, model_size);

    if (read_all(sock, model_data_ptr, model_size * sizeof(FEATURE_TYPE)) == 0) {
      throw std::runtime_error("Error talking to PS");
    }
    std::unique_ptr<CirrusModel> model = std::make_unique<SparseLRModel>(0);
    model->loadSerialized(model_data);

    delete[] model_data;
    return model;
  }
}

// Collaborative filtering

/**
  * This function needs to send to the PS a list of users and items
  * FORMAT of message to send is:
  * K item ids to send (uint32_t)
  * base user id (uint32_t)
  * minibatch size (uint32_t)
  * magic number (MAGIC_NUMBER) (uint32_t)
  * list of K item ids (K * uint32_t)
  */
SparseMFModel PSSparseServerInterface::get_sparse_mf_model(
    const SparseDataset& ds, uint32_t user_base, uint32_t minibatch_size) {
  char* msg = new char[MAX_MSG_SIZE];
  char* msg_begin = msg; // need to keep this pointer to delete later
  uint32_t item_ids_count = 0;
  store_value<uint32_t>(msg, 0); // we will write this value later
  store_value<uint32_t>(msg, user_base);
  store_value<uint32_t>(msg, minibatch_size);
  store_value<uint32_t>(msg, MAGIC_NUMBER); // magic value
  bool seen[17770] = {false};
  for (const auto& sample : ds.data_) {
    for (const auto& w : sample) {
      uint32_t movieId = w.first;
      if (seen[movieId])
          continue;
      store_value<uint32_t>(msg, movieId);
      seen[movieId] = true;
      item_ids_count++;
    }
  }
  msg = msg_begin;
  store_value<uint32_t>(msg, item_ids_count); // store correct value here
  uint32_t operation = GET_MF_SPARSE_MODEL;
  send_all(sock, &operation, sizeof(uint32_t));
  uint32_t msg_size = sizeof(uint32_t) * 4 + sizeof(uint32_t) * item_ids_count;
  send_all(sock, &msg_size, sizeof(uint32_t));
  if (send_all(sock, msg_begin, msg_size) == -1) {
    throw std::runtime_error("Error getting sparse mf model");
  }
  uint32_t to_receive_size;
  read_all(sock, &to_receive_size, sizeof(uint32_t));

  char* buffer = new char[to_receive_size];
  if (read_all(sock, buffer, to_receive_size) == 0) {
    throw std::runtime_error("");
  }

  // build a sparse model and return
  SparseMFModel model((FEATURE_TYPE*)buffer, minibatch_size, item_ids_count);
  
  delete[] msg_begin;
  delete[] buffer;

  return std::move(model);
}

void PSSparseServerInterface::send_mf_gradient(const MFSparseGradient& gradient) {
  uint32_t operation = SEND_MF_GRADIENT;
  if (send(sock, &operation, sizeof(uint32_t), 0) == -1) {
    throw std::runtime_error("Error sending operation");
  }

  uint32_t size = gradient.getSerializedSize();
  if (send(sock, &size, sizeof(uint32_t), 0) == -1) {
    throw std::runtime_error("Error sending grad size");
  }
  
  char* data = new char[size];
  gradient.serialize(data);
  if (send_all(sock, data, size) == 0) {
    throw std::runtime_error("Error sending grad");
  }
  delete[] data;
}

uint32_t PSSparseServerInterface::register_task(uint32_t id,
                                                uint32_t remaining_time_sec) {
#ifdef DEBUG
  std::cout << "Registering task id: " << id
            << " remaining_time_sec: " << remaining_time_sec << std::endl;
#endif

  uint32_t data[3] = {REGISTER_TASK, id, remaining_time_sec};
  if (send_all(sock, data, sizeof(uint32_t) * 3) == -1) {
    throw std::runtime_error("Error registering task");
  }

  uint32_t status;
  if (read_all(sock, &status, sizeof(uint32_t)) == 0) {
    throw std::runtime_error("Error getting task register return");
  }
  return status;
}

uint32_t PSSparseServerInterface::deregister_task(uint32_t id) {
#ifdef DEBUG
  std::cout << "Deregistering task id: " << id << std::endl;
#endif

  uint32_t data[2] = {DEREGISTER_TASK, id};
  if (send_all(sock, data, sizeof(uint32_t) * 2) == -1) {
    throw std::runtime_error("Error registering task");
  }

#ifdef DEBUG
  std::cout << "Deregistering reading reply: " << std::endl;
#endif
  uint32_t status;
  if (read_all(sock, &status, sizeof(uint32_t)) == 0) {
    throw std::runtime_error("Error getting task register return");
  }
  return status;
}

void PSSparseServerInterface::set_status(uint32_t id, uint32_t status) {
  std::cout << "Setting status id: " << id << " status: " << status << std::endl;
  uint32_t data[3] = {SET_TASK_STATUS, id, status};
  if (send_all(sock, data, sizeof(uint32_t) * 3) == -1) {
    throw std::runtime_error("Error setting task status");
  }
}

uint32_t PSSparseServerInterface::get_status(uint32_t id) {
  uint32_t data[2] = {GET_TASK_STATUS, id};
  if (send_all(sock, data, sizeof(uint32_t) * 2) == -1) {
    throw std::runtime_error("Error getting task status");
  }
  uint32_t status;
  if (read_all(sock, &status, sizeof(uint32_t)) == 0) {
    throw std::runtime_error("Error getting task status");
  }
  return status;
}

void PSSparseServerInterface::set_value(const std::string& key,
                                        char* data,
                                        uint32_t size) {
  assert(key.size() <= KEY_SIZE);

  char key_char[KEY_SIZE] = {0};
  std::copy(key.data(), key.data() + key.size(), key_char);

  uint32_t operation = SET_VALUE;
  if (send_all(sock, &operation, sizeof(operation)) != sizeof(operation)) {
    throw std::runtime_error("Error sending operation");
  }
  if (send_all(sock, key_char, KEY_SIZE) != KEY_SIZE) {
    throw std::runtime_error("Error sending key name");
  }
  if (send_all(sock, &size, sizeof(uint32_t)) != sizeof(uint32_t)) {
    throw std::runtime_error("Error sending value size");
  }
  if (send_all(sock, data, size) != size) {
    throw std::runtime_error("Error sending value data");
  }
}

std::pair<std::shared_ptr<char>, uint32_t> PSSparseServerInterface::get_value(
    const std::string& key) {
  char key_char[KEY_SIZE] = {0};
  std::copy(key.data(), key.data() + key.size(), key_char);

  uint32_t operation = GET_VALUE;
  if (send_all(sock, &operation, sizeof(operation)) != sizeof(operation)) {
    throw std::runtime_error("Error sending operation");
  }

  if (send_all(sock, key_char, KEY_SIZE) != KEY_SIZE) {
    throw std::runtime_error("Error sending key name");
  }

  uint32_t size = 0;
  if (read_all(sock, &size, sizeof(uint32_t)) != sizeof(uint32_t)) {
    throw std::runtime_error("Error reading key value");
  }

  if (size == 0) {
    // object not found
    return std::make_pair(std::shared_ptr<char>(nullptr), 0);
  }

  std::shared_ptr<char> value_data =
      std::shared_ptr<char>(new char[size], std::default_delete<char[]>());

  if (read_all(sock, value_data.get(), size) != size) {
    throw std::runtime_error("Error receiving value data");
  }

  return std::make_pair(value_data, size);
}

} // namespace cirrus

