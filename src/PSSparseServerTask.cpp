#include <Tasks.h>

#include "Serializers.h"
#include "Utils.h"
#include "Constants.h"
#include "Checksum.h"
#include <signal.h>
#include "OptimizationMethod.h"
#include "AdaGrad.h"
#include "Momentum.h"
#include "SGD.h"

#undef DEBUG

#define MAX_CONNECTIONS (nworkers * 2 + 1) // (2 x # workers + 1)
#define THREAD_MSG_BUFFER_SIZE 5000000

namespace cirrus {

PSSparseServerTask::PSSparseServerTask(
    uint64_t model_size,
    uint64_t batch_size, uint64_t samples_per_batch,
    uint64_t features_per_sample, uint64_t nworkers,
    uint64_t worker_id, const std::string& ps_ip,
    uint64_t ps_port) :
  MLTask(model_size,
      batch_size, samples_per_batch, features_per_sample,
      nworkers, worker_id, ps_ip, ps_port) {
    std::cout << "PSSparseServerTask is built" << std::endl;

    std::atomic_init(&thread_count, 0);

    operation_to_name[0] = "SEND_LR_GRADIENT";
    operation_to_name[1] = "SEND_MF_GRADIENT";
    operation_to_name[2] = "GET_LR_FULL_MODEL";
    operation_to_name[3] = "GET_MF_FULL_MODEL";
    operation_to_name[4] = "GET_LR_SPARSE_MODEL";
    operation_to_name[5] = "GET_MF_SPARSE_MODEL";
    operation_to_name[6] = "SET_TASK_STATUS";
    operation_to_name[7] = "GET_TASK_STATUS";

    for (int i = 0; i < NUM_PS_WORK_THREADS; i++)
      thread_msg_buffer[i] =
          new char[THREAD_MSG_BUFFER_SIZE]; // per-thread buffer
  }

std::shared_ptr<char> PSSparseServerTask::serialize_lr_model(
    const SparseLRModel& lr_model, uint64_t* model_size) const {
  *model_size = lr_model.getSerializedSize();
  auto d = std::shared_ptr<char>(
      new char[*model_size], std::default_delete<char[]>());
  lr_model.serializeTo(d.get());
  return d;
}

bool PSSparseServerTask::testRemove(struct pollfd x, int poll_id) {
  // If this pollfd will be removed, the index of the next location to insert
  // should be reduced by one correspondingly.
  if (x.fd == -1) {
    curr_indexes[poll_id] -= 1;
  }
  return x.fd == -1;
}

bool PSSparseServerTask::process_send_mf_gradient(const Request& req, std::vector<char>& thread_buffer) {
  uint32_t incoming_size = req.incoming_size;
#ifdef DEBUG
  std::cout << "APPLY_GRADIENT_REQ incoming size: " << incoming_size << std::endl;
#endif
  if (incoming_size > thread_buffer.size()) {
    throw std::runtime_error("Not enough buffer");
  }
  if (read_all(req.sock, thread_buffer.data(), incoming_size) == 0) {
    return false;
  }

  MFSparseGradient gradient;
  gradient.loadSerialized(thread_buffer.data());

  model_lock.lock();
#ifdef DEBUG
  std::cout << "Doing sgd update" << std::endl;
#endif
  mf_model->sgd_update(
      task_config.get_learning_rate(), &gradient);
#ifdef DEBUG
  std::cout
    << "sgd update done"
    << " checksum: " << mf_model->checksum()
    << std::endl;
#endif
  model_lock.unlock();
  gradientUpdatesCount++;
  return true;
}

bool PSSparseServerTask::process_send_lr_gradient(const Request& req, std::vector<char>& thread_buffer) {
  uint32_t incoming_size = req.incoming_size;
#ifdef DEBUG
  std::cout << "APPLY_GRADIENT_REQ incoming size: " << incoming_size << std::endl;
#endif
  if (incoming_size > thread_buffer.size()) {
    throw std::runtime_error("Not enough buffer");
  }
  //buffer.resize(incoming_size);
  try {
    if (read_all(req.sock, thread_buffer.data(), incoming_size) == 0) {
      return false;
    }
  } catch (...) {
    throw std::runtime_error("Uhandled error");
  }

  LRSparseGradient gradient(0);
  gradient.loadSerialized(thread_buffer.data());

  model_lock.lock();
  OptimizationMethod* opt_method = task_config.get_opt_method();
  std::vector<FEATURE_TYPE> weights = lr_model->get_weights();
  std::vector<FEATURE_TYPE> weight_hist = lr_model->get_weight_history();
  opt_method->sgd_update(
      weights, &gradient, weight_hist);
  lr_model->update_weights(weights);
  lr_model->update_weight_history(weight_hist);
  model_lock.unlock();
  gradientUpdatesCount++;
  return true;
}

// XXX we have to refactor this ASAP
// move this to SparseMFModel

bool PSSparseServerTask::process_get_mf_sparse_model(
    const Request& req, std::vector<char>& thread_buffer, int thread_number) {
  uint32_t k_items = 0;
  uint32_t base_user_id = 0;
  uint32_t minibatch_size = 0;
  uint32_t magic_value = 0;

  read_all(req.sock, &k_items, sizeof(uint32_t));
  read_all(req.sock, &base_user_id, sizeof(uint32_t));
  read_all(req.sock, &minibatch_size, sizeof(uint32_t));
  read_all(req.sock, &magic_value, sizeof(uint32_t));

  assert(k_items > 0);
  assert(minibatch_size > 0);
  if (magic_value != MAGIC_NUMBER) {
    throw std::runtime_error("Wrong message");
  }
  read_all(req.sock, thread_buffer.data(), k_items * sizeof(uint32_t));
  uint32_t to_send_size =
    minibatch_size * (sizeof(uint32_t) + (NUM_FACTORS + 1) * sizeof(FEATURE_TYPE)) +
    k_items * (sizeof(uint32_t) + (NUM_FACTORS + 1) * sizeof(FEATURE_TYPE));
#ifdef DEBUG
  std::cout << "k_items: " << k_items << std::endl;
  std::cout << "base_user_id: " << base_user_id << std::endl;
  std::cout << "minibatch_size: " << minibatch_size << std::endl;
#endif

  SparseMFModel sparse_mf_model((uint64_t) 0, 0, 0);
  sparse_mf_model.serializeFromDense(
      *mf_model, base_user_id, minibatch_size,
      k_items, thread_buffer.data(), thread_msg_buffer[thread_number]);

  //uint32_t to_send_size = data_to_send.size();
  if (send_all(req.sock, &to_send_size, sizeof(uint32_t)) == -1) {
    return false;
  }
  if (send_all(req.sock, thread_msg_buffer[thread_number], to_send_size) == -1) {
    return false;
  }
  return true;
}

bool PSSparseServerTask::process_get_lr_sparse_model(
    const Request& req, std::vector<char>& thread_buffer) {
  // need to parse the buffer to get the indices of the model we want
  // to send back to the client
  uint32_t incoming_size = req.incoming_size;
  if (incoming_size > thread_buffer.size()) {
    throw std::runtime_error("Not enough buffer");
  }
#ifdef DEBUG
  std::cout << "GET_MODEL_REQ incoming size: " << incoming_size << std::endl;
#endif
  try {
    if (read_all(req.sock, thread_buffer.data(), incoming_size) == 0) {
      return false;
    }
  } catch (...) {
    throw std::runtime_error("Uhandled error");
  }

  const char* data = thread_buffer.data();
  uint64_t num_entries = load_value<uint32_t>(data);

  uint32_t to_send_size = num_entries * sizeof(FEATURE_TYPE);
  assert(to_send_size < 1024 * 1024);
  char data_to_send[1024 * 1024]; // 1MB
  char* data_to_send_ptr = data_to_send;
#ifdef DEBUG
  std::cout << "Sending back: " << num_entries
    << " weights from model. Size: " << to_send_size
    << std::endl;
#endif
  for (uint32_t i = 0; i < num_entries; ++i) {
    uint32_t entry_index = load_value<uint32_t>(data);
    std::string opt_method = task_config.get_opt_method_string();
    if (opt_method == "nesterov") {
        store_value<FEATURE_TYPE>(
            data_to_send_ptr,
            lr_model->get_nth_weight_nesterov(entry_index, task_config.get_momentum_beta()));
    } else {
        store_value<FEATURE_TYPE>(
            data_to_send_ptr,
            lr_model->get_nth_weight(entry_index));
    }
  }
  if (send_all(req.sock, data_to_send, to_send_size) == -1) {
    return false;
  }
  return true;
}

bool PSSparseServerTask::process_get_mf_full_model(
    const Request& req, std::vector<char>& thread_buffer) {
  model_lock.lock();
  auto mf_model_copy = *mf_model;
  model_lock.unlock();
  uint32_t model_size = mf_model_copy.getSerializedSize();

  if (thread_buffer.size() < model_size) {
    std::cout << "thread_buffer.size(): " << thread_buffer.size()
      << " model_size: " << model_size << std::endl;
    throw std::runtime_error("Thread buffer too small");
  }

  mf_model_copy.serializeTo(thread_buffer.data());
  std::cout
    << "Serializing mf model"
    << " mode checksum: " << mf_model_copy.checksum()
    << " buffer checksum: " << crc32(thread_buffer.data(), model_size)
    << std::endl;
  if (send_all(req.sock, &model_size, sizeof(uint32_t)) == -1) {
    return false;
  }
  if (send_all(req.sock, thread_buffer.data(), model_size) == -1) {
    return false;
  }
  return true;
}

bool PSSparseServerTask::process_get_lr_full_model(
    const Request& req, std::vector<char>& thread_buffer) {
  model_lock.lock();
  auto lr_model_copy = *lr_model;
  model_lock.unlock();
  uint32_t model_size = lr_model_copy.getSerializedSize();

  if (thread_buffer.size() < model_size) {
    std::string error_str = "buffer with size " + std::to_string(thread_buffer.size()) +
      "too small: " + std::to_string(model_size);
    throw std::runtime_error(error_str);
  }

  lr_model_copy.serializeTo(thread_buffer.data());
  if (send_all(req.sock, thread_buffer.data(), model_size) == -1)
    return false;
  return true;
}

void PSSparseServerTask::gradient_f() {
  std::vector<char> thread_buffer;
  thread_buffer.resize(120 * 1024 * 1024); // 120 MB

  int thread_number = thread_count++;
  while (1) {
    sem_wait(&sem_new_req);
    to_process_lock.lock();
    Request req = std::move(to_process.front());

    to_process.pop();
    to_process_lock.unlock();

    int sock = req.poll_fd.fd;

    uint32_t operation = 0;
    if (read_all(sock, &operation, sizeof(uint32_t)) == 0) {
      if (close(req.poll_fd.fd) != 0) {
        std::cout << "Error closing socket. errno: " << errno << std::endl;
      }
      num_connections--;
      std::cout << "PS closing connection after process(): " << num_connections << std::endl;
      req.poll_fd.fd = -1;
      req.poll_fd.revents = 0;
      continue;
    }

    req.req_id = operation;
    if (operation == SEND_LR_GRADIENT || operation == SEND_MF_GRADIENT ||
        operation == GET_LR_SPARSE_MODEL || operation == GET_MF_SPARSE_MODEL) {
      uint32_t incoming_size = 0;
      if (read_all(sock, &incoming_size, sizeof(uint32_t)) == 0) {
        if (close(req.poll_fd.fd) != 0) {
          std::cout << "Error closing socket. errno: " << errno << std::endl;
        }
        num_connections--;
        std::cout << "PS closing connection after process(): " << num_connections << std::endl;
        req.poll_fd.fd = -1;
        req.poll_fd.revents = 0;
        continue;
      }
      req.incoming_size = incoming_size;

    }

#ifdef DEBUG
    std::cout << "Processing request: " << req.req_id << std::endl;
#endif

    if (req.req_id == SEND_LR_GRADIENT) {
      if (!process_send_lr_gradient(req, thread_buffer)) {
        break;
      }
    } else if (req.req_id == SEND_MF_GRADIENT) {
      if (!process_send_mf_gradient(req, thread_buffer)) {
        break;
      }
    } else if (req.req_id == GET_LR_SPARSE_MODEL) {
#ifdef DEBUG
      std::cout << "process_get_lr_sparse_model" << std::endl;
      auto before = get_time_us();
#endif
      if (!process_get_lr_sparse_model(req, thread_buffer)) {
        break;
      }
#ifdef DEBUG
      auto elapsed = get_time_us() - before;
      std::cout << "GET_LR_SPARSE_MODEL Elapsed(us): " << elapsed << std::endl;
#endif
    } else if (req.req_id == GET_MF_SPARSE_MODEL) {
      if (!process_get_mf_sparse_model(req, thread_buffer, thread_number)) {
        break;
      }
    } else if (req.req_id == GET_LR_FULL_MODEL) {
      if (!process_get_lr_full_model(req, thread_buffer))
        break;
    } else if (req.req_id == GET_MF_FULL_MODEL) {
      if (!process_get_mf_full_model(req, thread_buffer))
        break;
    } else if (req.req_id == GET_TASK_STATUS) {
      uint32_t task_id;
      if (read_all(sock, &task_id, sizeof (uint32_t)) == 0) {
        break;
      }
#ifdef DEBUG
      std::cout << "Get status task id: " << task_id << std::endl;
#endif
      assert(task_id < 10000);
      if (task_to_status.find(task_id) == task_to_status.end() ||
          task_to_status[task_id] == false) {
        uint32_t status = 0;
        send_all(sock, &status, sizeof (uint32_t));
      } else {
        uint32_t status = 1;
        send_all(sock, &status, sizeof (uint32_t));
      }
    
    } else if (operation == SET_TASK_STATUS) {
    
      uint32_t data[2] = {0}; // id + status
      if (read_all(sock, data, sizeof (uint32_t) * 2) == 0) {
        break;
      }
#ifdef DEBUG
      std::cout << "Set status task id: " << data[0] << " status: " << data[1] << std::endl;
#endif
      task_to_status[data[0]] = data[1];
    
    } else {
      throw std::runtime_error("gradient_f: Unknown operation");
    }

    // We reactivate events from the client socket here
    req.poll_fd.events = POLLIN;
    //pthread_kill(main_thread, SIGUSR1);

    assert(write(pipefds[req.id][1], "a", 1) == 1); // wake up poll()

#ifdef DEBUG
    std::cout << "gradient_f done" << std::endl;
#endif
  }
}

/**
 * FORMAT
 * operation (uint32_t)
 * incoming size (uint32_t)
 * buffer with previous size
 */
bool PSSparseServerTask::process(struct pollfd& poll_fd, int thread_id) {
  int sock = poll_fd.fd;
#ifdef DEBUG
  std::cout << "Processing socket: " << sock << std::endl;
#endif

  uint32_t operation = 0;
  //if (read_all(sock, &operation, sizeof(uint32_t)) == 0) { // read operation
  //  return false;
  //}
#ifdef DEBUG 
  std::cout << "Operation: " << operation << " - "
      << operation_to_name[operation] << std::endl;
#endif

  uint32_t incoming_size = 0;
#ifdef DEBUG 
  std::cout << "incoming size: " << incoming_size << std::endl;
#endif
  to_process_lock.lock();
  poll_fd.events = 0; // explain this
  to_process.push(Request(operation, sock, thread_id, incoming_size, poll_fd));
  to_process_lock.unlock();
  sem_post(&sem_new_req);
  return true;
}

void PSSparseServerTask::start_server() {
  lr_model.reset(new SparseLRModel(model_size));
  lr_model->randomize();
  mf_model.reset(new MFModel(task_config.get_users(), task_config.get_items(), NUM_FACTORS));
  mf_model->randomize();

  sem_init(&sem_new_req, 0, 0);

  for (int i = 0; i < NUM_POLL_THREADS; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    server_threads.push_back(std::make_unique<std::thread>(
          std::bind(&PSSparseServerTask::main_poll_thread_fn, this, i)));
  }

  for (uint32_t i = 0; i < NUM_PS_WORK_THREADS; ++i) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    gradient_thread.push_back(std::make_unique<std::thread>(
          std::bind(&PSSparseServerTask::gradient_f, this)));
  }

  // start checkpoing thread
  if (task_config.get_checkpoint_frequency() > 0) {
      checkpoint_thread.push_back(std::make_unique<std::thread>(
                  std::bind(&PSSparseServerTask::checkpoint_model_loop, this)));
  }
}

void PSSparseServerTask::main_poll_thread_fn(int poll_id) {
  // id=0 -> poll thread responsible for handling new connections
  if (poll_id == 0) {
    std::cout << "Starting server, poll id " << poll_id << std::endl;

    poll_thread = pthread_self();

    server_sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock_ < 0) {
      throw std::string("Server error creating socket");
    }

    int opt = 1;
    if (setsockopt(server_sock_, IPPROTO_TCP,
                TCP_NODELAY, &opt, sizeof(opt))) {
      throw std::runtime_error("Error setting socket options.");
    }
    if (setsockopt(server_sock_, SOL_SOCKET,
                SO_REUSEADDR, &opt, sizeof(opt))) {
      throw std::runtime_error("Error forcing port binding");
    }

    if (setsockopt(server_sock_, SOL_SOCKET,
                SO_REUSEPORT, &opt, sizeof(opt))) {
      throw std::runtime_error("Error forcing port binding");
    }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port_);
    std::memset(serv_addr.sin_zero, 0, sizeof(serv_addr.sin_zero));

    int ret = bind(server_sock_,
            reinterpret_cast<sockaddr*> (&serv_addr), sizeof(serv_addr));
    if (ret < 0) {
      throw std::runtime_error("Error binding in port " + to_string(port_));
    }

    if (listen(server_sock_, SOMAXCONN) == -1) {
      throw std::runtime_error("Error listening on port " + to_string(port_));
    }
    fdses[0].at(0).fd = server_sock_;
    fdses[0].at(0).events = POLLIN;
    fdses[0].at(1).fd = pipefds[poll_id][0];
    fdses[0].at(1).events = POLLIN;
    curr_indexes[poll_id] = 2;
  } else {
    std::cout << "Starting secondary poll thread: " << poll_id << std::endl;
    fdses[poll_id].at(0).fd = pipefds[poll_id][0];
    fdses[poll_id].at(0).events = POLLIN;
    curr_indexes[poll_id] = 1;

  }
  loop(poll_id);
}

void PSSparseServerTask::loop(int poll_id) {
  struct sockaddr_in cli_addr;
  socklen_t clilen = sizeof(cli_addr);

  buffer.resize(10 * 1024 * 1024); // reserve 10MB upfront

  std::cout << "Starting loop for id: " << poll_id << std::endl;
  while (1) {
    int poll_status = poll(fdses[poll_id].data(), curr_indexes[poll_id], timeout);
    if (poll_status == -1) {
      if (errno != EINTR) {
        throw std::runtime_error("Server error calling poll.");
      } else {
        std::cout << "EINTR" << std::endl;
      }
    } else if (
            (poll_id == 0 && fdses[poll_id][1].revents == POLLIN)
         || (poll_id != 0 && fdses[poll_id][0].revents == POLLIN)) {
      //std::cout << "Ignoring" << std::endl;
      int posit = 0;
      if (poll_id == 0)
        posit = 1;
      fdses[poll_id][posit].revents = 0; // Reset the event flags
      char a[1];
      assert(read(pipefds[poll_id][0], a, 1) >= 0);
      // ignore
    } else if (poll_status == 0) {
    } else {
      // there is at least one pending event, find it.
      for (uint64_t i = 0; i < curr_indexes[poll_id]; i++) {
        struct pollfd& curr_fd = fdses[poll_id][i];
        // Ignore the fd if we've said we don't care about it
        if (curr_fd.fd == -1) {
          continue;
        }
        if (curr_fd.revents != POLLIN) {
          //LOG<ERROR>("Non read event on socket: ", curr_fd.fd);
          if (curr_fd.revents & POLLHUP) {
            std::cout << "PS closing connection " << num_connections << std::endl;
            num_connections--;
            close(curr_fd.fd);
            curr_fd.fd = -1;
          }
        } else if (poll_id == 0 && curr_fd.fd == server_sock_) {
          std::cout << "PS new connection!" << std::endl;
          int newsock = accept(server_sock_,
              reinterpret_cast<struct sockaddr*> (&cli_addr),
              &clilen);
          if (poll_id == 0 && newsock < 0) {
            throw std::runtime_error("Error accepting socket");
          }
          // If at capacity, reject connection
          if (poll_id == 0 && num_connections > (MAX_CONNECTIONS - 1)) {
            std::cout << "Rejecting connection "
              << num_connections
              << std::endl;
            close(newsock);
          } else if (poll_id == 0 && curr_indexes[poll_id] == max_fds) {
            throw std::runtime_error("We reached capacity");
            close(newsock);
          } else if (poll_id == 0) {
            int r = rand() % NUM_POLL_THREADS;
            std::cout << "Random: " << r << std::endl;
            fdses[r][curr_indexes[r]].fd = newsock;
            fdses[r][curr_indexes[r]].events = POLLIN;
            curr_indexes[r]++;
            num_connections++;

          }
        } else {
#ifdef DEBUG
          std::cout << "Calling process" << std::endl;
#endif
          if (!process(curr_fd, poll_id)) {
            if (close(curr_fd.fd) != 0) {
              std::cout << "Error closing socket. errno: " << errno << std::endl;
            }
            num_connections--;
            std::cout << "PS closing connection after process(): " << num_connections << std::endl;
            curr_fd.fd = -1;
          }
        }
        curr_fd.revents = 0; // Reset the event flags
      }
    }
    // If at max capacity, try to make room
    if (curr_indexes[poll_id] == max_fds) {
      // Try to purge unused fds, those with fd == -1
      std::cout << "Purging" << std::endl;
      std::remove_if(fdses[poll_id].begin(), fdses[poll_id].end(),
          std::bind(&PSSparseServerTask::testRemove,
              this, std::placeholders::_1, poll_id));
    }
  }
}

void sig_handler(int) {
  //std::cout << "Sig handler" << std::endl;
}

/**
 * This is the task that runs the parameter server
 * This task is responsible for
 * 1) sending the model to the workers
 * 2) receiving the gradient updates from the workers
 *
 */
void PSSparseServerTask::run(const Configuration& config) {
  std::cout
    << "PS task initializing model"
    << std::endl;

  for (int i = 0; i < NUM_POLL_THREADS; i++) {
    assert(pipe(pipefds[i]) != -1);
    curr_indexes[i] == 0;
    fdses[i].resize(max_fds);
  }

  main_thread = pthread_self();
  if (signal(SIGUSR1, sig_handler) == SIG_ERR) {
    throw std::runtime_error("Unable to set signal handler");
  }

  task_config = config;
  start_server();

  //wait_for_start(PS_SPARSE_SERVER_TASK_RANK, redis_con, nworkers);

  uint64_t start = get_time_us();
  uint64_t last_tick = get_time_us();
  while (1) {
    auto now = get_time_us();
    auto elapsed_us = now - last_tick;
    auto since_start_sec = 1.0 * (now - start) / 1000000;
    if (elapsed_us > 1000000) {
      last_tick = now;
      std::cout << "Events in the last sec: "
        << 1.0 * gradientUpdatesCount / elapsed_us * 1000 * 1000
        << " since (sec): " << since_start_sec
        << " #conns: " << num_connections
        << std::endl;
      gradientUpdatesCount = 0;
    }
    sleep(1);
  }
}

void PSSparseServerTask::checkpoint_model_loop() {
    if (task_config.get_checkpoint_frequency() == 0) {
        // checkpoint disabled
        return;
    }

    while (true) {
        sleep(task_config.get_checkpoint_frequency());
        // checkpoint to s3
    }
}

void PSSparseServerTask::checkpoint_model_file(const std::string& filename) const {
  uint64_t model_size;
  std::shared_ptr<char> data = serialize_lr_model(*lr_model, &model_size);

  std::ofstream fout(filename.c_str(), std::ofstream::binary);
  fout.write(data.get(), model_size);
  fout.close();
}

} // namespace cirrus
