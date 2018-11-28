#ifndef _TASKS_H_
#define _TASKS_H_

#include <Configuration.h>

#include "config.h"
#include "LRModel.h"
#include "MFModel.h"
#include "SparseLRModel.h"
#include "PSSparseServerInterface.h"
#include "S3SparseIterator.h"
#include "OptimizationMethod.h"

#include <chrono>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace cirrus {
class MLTask {
  public:
    MLTask(
        uint64_t model_size,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id,
        const std::string& ps_ip,
        uint64_t ps_port) :
      model_size(model_size),
      batch_size(batch_size), samples_per_batch(samples_per_batch),
      features_per_sample(features_per_sample),
      nworkers(nworkers), worker_id(worker_id),
      ps_ip(ps_ip), ps_port(ps_port)
  {}

    /**
     * Worker here is a value 0..nworkers - 1
     */
    void run(const Configuration& config, int worker);
    void wait_for_start(int index, int nworkers);

  protected:
    uint64_t model_size;
    uint64_t batch_size;
    uint64_t samples_per_batch;
    uint64_t features_per_sample;
    uint64_t nworkers;
    uint64_t worker_id;
    std::string ps_ip;
    uint64_t ps_port;
    Configuration config;
};

class LogisticSparseTaskS3 : public MLTask {
  public:
    LogisticSparseTaskS3(
        uint64_t model_size,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id,
        const std::string& ps_ip,
        uint64_t ps_port) :
      MLTask(model_size,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, worker_id, ps_ip, ps_port), psint(nullptr)
  {}

    /**
     * Worker here is a value 0..nworkers - 1
     */
    void run(const Configuration& config, int worker);

  private:
    class SparseModelGet {
      public:
        SparseModelGet(const std::string& ps_ip, int ps_port) :
          ps_ip(ps_ip), ps_port(ps_port) {
            psi = std::make_unique<PSSparseServerInterface>(ps_ip, ps_port);
            psi->connect();
        }

        SparseLRModel get_new_model(const SparseDataset& ds,
                                    const Configuration& config) {
          return std::move(psi->get_lr_sparse_model(ds, config));
        }
        void get_new_model_inplace(const SparseDataset& ds,
                                   SparseLRModel& model,
                                   const Configuration& config) {
          psi->get_lr_sparse_model_inplace(ds, model, config);
        }

      private:
        std::unique_ptr<PSSparseServerInterface> psi;
        std::string ps_ip;
        int ps_port;
    };

    bool get_dataset_minibatch(std::shared_ptr<SparseDataset>& dataset,
                               S3SparseIterator& s3_iter);
    void push_gradient(LRSparseGradient*);

    std::mutex redis_lock;
  
    std::unique_ptr<SparseModelGet> sparse_model_get;
    PSSparseServerInterface* psint;
};

class PSSparseTask : public MLTask {
  public:
    PSSparseTask(
        uint64_t model_size,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id, const std::string& ps_ip,
        uint64_t ps_port);

    void run(const Configuration& config);

  private:
    void put_model(const SparseLRModel& model);
    void publish_model(const SparseLRModel& model);

    void thread_fn();

    void publish_model_pubsub();
    void publish_model_redis();

    /**
      * Attributes
      */
#if defined(USE_REDIS)
    std::vector<unsigned int> gradientVersions;
#endif

    uint64_t server_clock = 0;  // minimum of all worker clocks
    std::unique_ptr<std::thread> thread;
};


class ErrorSparseTask : public MLTask {
  public:
   ErrorSparseTask(uint64_t model_size,
                   uint64_t batch_size,
                   uint64_t samples_per_batch,
                   uint64_t features_per_sample,
                   uint64_t nworkers,
                   uint64_t worker_id,
                   const std::string& ps_ip,
                   uint64_t ps_port);

   void run(const Configuration& config);
   void error_response();

  private:
   // Stores last recorded time/loss values
   double last_time = 0.0;
   double last_error = 0.0;
   std::atomic<double> curr_error;
   std::atomic<double> total_loss;
};

class PerformanceLambdaTask : public MLTask {
  public:
    PerformanceLambdaTask(
        uint64_t model_size,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id, const std::string& ps_ip,
        uint64_t ps_port) :
      MLTask(model_size,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, worker_id, ps_ip, ps_port)
  {}

    /**
     * Worker here is a value 0..nworkers - 1
     */
    void run(const Configuration& config);

  private:
};

class LoadingSparseTaskS3 : public MLTask {
  public:
    LoadingSparseTaskS3(
        uint64_t model_size,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id, const std::string& ps_ip,
        uint64_t ps_port) :
      MLTask(model_size,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, worker_id, ps_ip, ps_port)
  {}
    void run(const Configuration& config);
    SparseDataset read_dataset(const Configuration& config);
    void check_loading(const Configuration&,
                       std::unique_ptr<S3Client>& s3_client);
    void check_label(FEATURE_TYPE label);

  private:
};

class LoadingNetflixTask : public MLTask {
 public:
  LoadingNetflixTask(uint64_t model_size,
                     uint64_t batch_size,
                     uint64_t samples_per_batch,
                     uint64_t features_per_sample,
                     uint64_t nworkers,
                     uint64_t worker_id,
                     const std::string& ps_ip,
                     uint64_t ps_port)
      : MLTask(model_size,
               batch_size,
               samples_per_batch,
               features_per_sample,
               nworkers,
               worker_id,
               ps_ip,
               ps_port) {}
  void run(const Configuration& config);
  SparseDataset read_dataset(const Configuration& config, int&, int&);
  void check_loading(const Configuration&,
                     std::unique_ptr<S3Client>& s3_client);

 private:
};

class PSSparseServerTask : public MLTask {
 public:
  PSSparseServerTask(uint64_t model_size,
                     uint64_t batch_size,
                     uint64_t samples_per_batch,
                     uint64_t features_per_sample,
                     uint64_t nworkers,
                     uint64_t worker_id,
                     const std::string& ps_ip,
                     uint64_t ps_port);

  void run(const Configuration& config);

  struct Request {
   public:
    Request(int sock, int id, uint32_t incoming_size, struct pollfd& poll_fd)
        : sock(sock), id(id), incoming_size(incoming_size), poll_fd(poll_fd) {}

    int sock;
    int id;
    uint32_t incoming_size;
    struct pollfd& poll_fd;
  };

 private:
  /**
    * Handle the situation when a socket read fails within worker threads
    */
  void handle_failed_read(struct pollfd* pfd);
  void checkpoint_model_loop();      //< periodically checkpoint model
  void start_server();               //< start server thread
  void main_poll_thread_fn(int id);  //< setup polling thread and call poll()

  bool testRemove(struct pollfd x, int id);  //< clean dead connections
  void loop(int id);                         //< listen for requests
  bool process(struct pollfd&, int id);      //< process a request

  void set_operation_maps();  //< set maps related to requests

  /**
    * Model/ML related methods
    */

  // checkpoint model to file
  void checkpoint_model_file(const std::string&) const;

  // serialize lr model
  std::shared_ptr<char> serialize_lr_model(const SparseLRModel&,
                                           uint64_t* model_size) const;

  // worker thread function
  void gradient_f();

  // message handling
  bool process_get_lr_sparse_model(int,
                                   const Request&,
                                   std::vector<char>&,
                                   int);
  bool process_get_mf_sparse_model(int,
                                   const Request&,
                                   std::vector<char>&,
                                   int);
  bool process_send_lr_gradient(int, const Request&, std::vector<char>&, int);
  bool process_send_mf_gradient(int, const Request&, std::vector<char>&, int);
  bool process_get_lr_full_model(int, const Request&, std::vector<char>&, int);
  bool process_get_mf_full_model(int, const Request&, std::vector<char>&, int);
  bool process_get_task_status(int, const Request&, std::vector<char>&, int);
  bool process_set_task_status(int, const Request&, std::vector<char>&, int);
  bool process_get_num_conns(int, const Request&, std::vector<char>&, int);
  bool process_get_num_updates(int, const Request&, std::vector<char>&, int);
  bool process_get_last_time_error(int,
                                   const Request&,
                                   std::vector<char>&,
                                   int);
  bool process_get_value(int, const Request&, std::vector<char>&, int);
  bool process_set_value(int, const Request&, std::vector<char>&, int);
  bool process_register_task(int, const Request&, std::vector<char>&, int);
  bool process_deregister_task(int, const Request&, std::vector<char>&, int);

  void kill_server();

  static void destroy_pthread_barrier(pthread_barrier_t*);

  void check_tasks_lifetime();
  uint32_t declare_task_dead(uint32_t);

  /**
    * Attributes
    */
  std::unique_ptr<OptimizationMethod> opt_method;  //< SGD optimization method

  // keep track of per-worker connections
  std::vector<uint64_t> curr_indexes = std::vector<uint64_t>(NUM_POLL_THREADS);

  // threads to handle connections and messages
  std::vector<std::unique_ptr<std::thread>> server_threads;

  // threads to handle requests
  std::vector<std::unique_ptr<std::thread>> gradient_thread;

  std::set<uint64_t> registered_tasks;  //< ids of registered tasks
  // reamining time (sec) of each registered task
  std::map<uint64_t, int64_t> task_to_remaining_time;
  std::map<uint64_t, std::chrono::time_point<std::chrono::steady_clock>>
      task_to_starttime;
  std::mutex register_lock;  //< to coordinate access to reg. datastructures

  // thread to checkpoint model
  std::vector<std::unique_ptr<std::thread>> checkpoint_thread;
  pthread_t main_thread;
  std::mutex to_process_lock;      //< lock for queue of requests
  sem_t sem_new_req;               //< semaphore for queue of requests
  std::queue<Request> to_process;  //< list of requests
  std::mutex model_lock;  //< to coordinate access to the last computed model

  // file descriptors for pipes
  int pipefds[NUM_POLL_THREADS][2] = {{0}};

  int server_sock_ = 0;           //< server used to receive connections
  const uint64_t max_fds = 2000;  //< max number of connections supported
  int timeout = 1;                //< 1 ms

  // file descriptors for connections
  std::vector<std::vector<struct pollfd>> fdses =
      std::vector<std::vector<struct pollfd>>(NUM_POLL_THREADS);

  std::vector<char> buffer;  //< we use this buffer to hold data from workers

  std::atomic<uint64_t> gradientUpdatesCount;  //< # of gradients processed

  std::unique_ptr<SparseLRModel> lr_model;  //< last computed model
  std::unique_ptr<MFModel> mf_model;        //< last computed model
  Configuration task_config;                //< config for parameter server
  uint32_t num_connections = 0;             //< num of current connections
  uint32_t num_tasks = 0;                   //< num of currently reg. tasks

  std::map<int, bool> task_to_status;            //< keep track of task status
  std::map<int, std::string> operation_to_name;  //< request id to name

  // per-thread buffer
  std::shared_ptr<char[]> thread_msg_buffer[NUM_PS_WORK_THREADS];
  std::atomic<int> thread_count;  //< keep track of each thread's id

  uint32_t num_updates = 0;       //< Last measured num updates
  std::atomic<bool> kill_signal;  //< Used to coordinate thread kills

  // barrier to synchronize threads init
  std::unique_ptr<pthread_barrier_t, void (*)(pthread_barrier_t*)>
      threads_barrier;

  std::unordered_map<
      uint32_t,
      std::function<bool(int, const Request&, std::vector<char>&, int)>>
      operation_to_f;

  std::unordered_map<std::string, std::pair<uint32_t, std::shared_ptr<char>>>
      key_value_map;
};

class MFNetflixTask : public MLTask {
  public:
    MFNetflixTask(
        uint64_t model_size,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id, const std::string& ps_ip,
        uint64_t ps_port) :
      MLTask(model_size,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, worker_id, ps_ip, ps_port)
  {}

    /**
     * Worker here is a value 0..nworkers - 1
     */
    void run(const Configuration& config, int worker);

  private:
    class MFModelGet {
      public:
        MFModelGet(const std::string& ps_ip, int ps_port) :
          ps_ip(ps_ip), ps_port(ps_port) {
            psi = std::make_unique<PSSparseServerInterface>(ps_ip, ps_port);
            psi->connect();
        }

        SparseMFModel get_new_model(const SparseDataset& ds,
                                    uint64_t user_base_index,
                                    uint64_t mb_size) {
          return psi->get_sparse_mf_model(ds, user_base_index, mb_size);
        }

      private:
        std::unique_ptr<PSSparseServerInterface> psi;
        std::string ps_ip;
        int ps_port;
    };

  private:
   bool get_dataset_minibatch(std::shared_ptr<SparseDataset>& dataset,
                              S3SparseIterator& s3_iter);
   void push_gradient(MFSparseGradient&);

   std::unique_ptr<MFModelGet> mf_model_get;
   std::unique_ptr<PSSparseServerInterface> psint;
};

}

#endif  // _TASKS_H_
