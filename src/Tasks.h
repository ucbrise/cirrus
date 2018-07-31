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

#include <string>
#include <vector>
#include <map>

#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
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

    bool get_dataset_minibatch(
        std::unique_ptr<SparseDataset>& dataset,
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
    Request(int req_id,
            int sock,
            int id,
            uint32_t incoming_size,
            struct pollfd& poll_fd)
        : req_id(req_id),
          sock(sock),
          id(id),
          incoming_size(incoming_size),
          poll_fd(poll_fd) {}

    int req_id;
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
  bool process_get_lr_sparse_model(const Request& req, std::vector<char>&);
  bool process_send_lr_gradient(const Request& req, std::vector<char>&);
  bool process_get_mf_sparse_model(const Request& req,
                                   std::vector<char>&,
                                   int tn);
  bool process_get_lr_full_model(const Request& req,
                                 std::vector<char>& thread_buffer);
  bool process_send_mf_gradient(const Request& req,
                                std::vector<char>& thread_buffer);
  bool process_get_mf_full_model(const Request& req,
                                 std::vector<char>& thread_buffer);

  void kill_server();

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

  std::set<uint64_t> registered_tasks;  //< which tasks have registered

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
  const uint64_t max_fds = 1000;  //< max number of connections supported
  int timeout = 1;                //< 1 ms

  // file descriptors for connections
  std::vector<std::vector<struct pollfd>> fdses =
      std::vector<std::vector<struct pollfd>>(NUM_POLL_THREADS);

  std::vector<char> buffer;  //< we use this buffer to hold data from workers

  std::atomic<uint64_t> gradientUpdatesCount;  //< # of gradients processed

  std::unique_ptr<SparseLRModel> lr_model;  //< last computed model
  std::unique_ptr<MFModel> mf_model;        //< last computed model
  Configuration task_config;                //< config for parameter server
  uint32_t num_connections = 0;             //< number of current connections

  std::map<int, bool> task_to_status;            //< keep track of task status
  std::map<int, std::string> operation_to_name;  //< request id to name

  char* thread_msg_buffer[NUM_PS_WORK_THREADS];  // per-thread buffer
  std::atomic<int> thread_count;  //< keep track of each thread's id

  uint32_t num_updates = 0;       // Last measured num updates
  std::atomic<bool> kill_signal;  // Used to coordinate thread kills
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
    bool get_dataset_minibatch(
        std::unique_ptr<SparseDataset>& dataset,
        S3SparseIterator& s3_iter);
    void push_gradient(MFSparseGradient&);

    std::unique_ptr<MFModelGet> mf_model_get;
    std::unique_ptr<PSSparseServerInterface> psint;
};

}

#endif  // _TASKS_H_
