#ifndef _TASKS_H_
#define _TASKS_H_

#include <Configuration.h>

#include "config.h"
#include "LRModel.h"
#include "MFModel.h"
#include "SparseLRModel.h"
#include "PSSparseServerInterface.h"
#include "S3SparseIterator.h"

#include <string>
#include <vector>
#include <map>

#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <unistd.h>


class MLTask {
  public:
    MLTask(
        uint64_t MODEL_GRAD_SIZE, //XXX this doesn't generalize to all types of gradients
        uint64_t MODEL_BASE,
        uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
        uint64_t SAMPLE_BASE, uint64_t START_BASE,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id) :
      MODEL_GRAD_SIZE(MODEL_GRAD_SIZE),
      MODEL_BASE(MODEL_BASE), LABEL_BASE(LABEL_BASE),
      GRADIENT_BASE(GRADIENT_BASE), SAMPLE_BASE(SAMPLE_BASE),
      START_BASE(START_BASE),
      batch_size(batch_size), samples_per_batch(samples_per_batch),
      features_per_sample(features_per_sample),
      nworkers(nworkers), worker_id(worker_id)
  {}

    /**
     * Worker here is a value 0..nworkers - 1
     */
    void run(const Configuration& config, int worker);

    void wait_for_start(int index, int nworkers);
    bool get_worker_status(auto r, int worker_id);

  protected:
    uint64_t MODEL_GRAD_SIZE;
    uint64_t MODEL_BASE;
    uint64_t LABEL_BASE;
    uint64_t GRADIENT_BASE;
    uint64_t SAMPLE_BASE;
    uint64_t START_BASE;
    uint64_t batch_size;
    uint64_t samples_per_batch;
    uint64_t features_per_sample;
    uint64_t nworkers;
    uint64_t worker_id;
    Configuration config;
};

class LogisticSparseTaskS3 : public MLTask {
  public:
    LogisticSparseTaskS3(
        uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
        uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
        uint64_t SAMPLE_BASE, uint64_t START_BASE,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id) :
      MLTask(MODEL_GRAD_SIZE, MODEL_BASE,
          LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, worker_id)
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

        SparseLRModel get_new_model(const SparseDataset& ds, const Configuration& config) {
          return std::move(psi->get_lr_sparse_model(ds, config));
        }
        void get_new_model_inplace(const SparseDataset& ds, SparseLRModel& model, const Configuration& config) {
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
    auto get_model(auto r, auto lmd);
    void push_gradient(LRSparseGradient*);
    void unpack_minibatch(std::shared_ptr<FEATURE_TYPE> /*minibatch*/,
        auto& samples, auto& labels);

    std::mutex redis_lock;
  
    std::unique_ptr<SparseModelGet> sparse_model_get;
    PSSparseServerInterface* psint;
};

class PSSparseTask : public MLTask {
  public:
    PSSparseTask(
        uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
        uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
        uint64_t SAMPLE_BASE, uint64_t START_BASE,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id);

    void run(const Configuration& config);

  private:
    auto connect_redis();

    void put_model(const SparseLRModel& model);
    void publish_model(const SparseLRModel& model);

    void update_gradient_version(
        auto& gradient, int worker, SparseLRModel& model, Configuration config);

    void get_gradient(auto r, auto& gradient, auto gradient_id);

    void thread_fn();

    void update_publish(auto&);
    void publish_model_pubsub();
    void publish_model_redis();
    void update_publish_gradient(auto&);

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
    ErrorSparseTask(
        uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
        uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
        uint64_t SAMPLE_BASE, uint64_t START_BASE,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id) :
      MLTask(MODEL_GRAD_SIZE, MODEL_BASE,
          LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, worker_id)
      //mp(redis_ip, redis_port)
  {}
    void run(const Configuration& config);

  private:
    void get_samples_labels_redis(
        auto r, auto i, auto& samples, auto& labels,
        auto cad_samples, auto cad_labels);
    void parse_raw_minibatch(const FEATURE_TYPE* minibatch,
        auto& samples, auto& labels);

    //ProgressMonitor mp;
};

class PerformanceLambdaTask : public MLTask {
  public:
    PerformanceLambdaTask(
        uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
        uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
        uint64_t SAMPLE_BASE, uint64_t START_BASE,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id) :
      MLTask(MODEL_GRAD_SIZE, MODEL_BASE,
          LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, worker_id)
  {}

    /**
     * Worker here is a value 0..nworkers - 1
     */
    void run(const Configuration& config);

  private:
    void unpack_minibatch(const FEATURE_TYPE* /*minibatch*/,
        auto& samples, auto& labels);

    redisContext* connect_redis();
};

class LoadingSparseTaskS3 : public MLTask {
  public:
    LoadingSparseTaskS3(
        uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
        uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
        uint64_t SAMPLE_BASE, uint64_t START_BASE,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id) :
      MLTask(MODEL_GRAD_SIZE, MODEL_BASE,
          LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, worker_id)
  {}
    void run(const Configuration& config);
    SparseDataset read_dataset(const Configuration& config);
    void check_loading(const Configuration&, Aws::S3::S3Client& s3_client);
    void check_label(FEATURE_TYPE label);

  private:
};

class LoadingNetflixTask : public MLTask {
  public:
    LoadingNetflixTask(
        uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
        uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
        uint64_t SAMPLE_BASE, uint64_t START_BASE,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id) :
      MLTask(MODEL_GRAD_SIZE, MODEL_BASE,
          LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, worker_id)
  {}
    void run(const Configuration& config);
    SparseDataset read_dataset(const Configuration& config, int&, int&);
    void check_loading(const Configuration&, Aws::S3::S3Client& s3_client);

  private:
};

class PSSparseServerTask : public MLTask {
  public:
    PSSparseServerTask(
        uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
        uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
        uint64_t SAMPLE_BASE, uint64_t START_BASE,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id);

    void run(const Configuration& config);

    struct Request {
      public:
        Request(int req_id, int sock, int id, uint32_t incoming_size, struct pollfd& poll_fd) :
          req_id(req_id), sock(sock), id(id), incoming_size(incoming_size), poll_fd(poll_fd){}

        int req_id;
        int sock;
        int id;
        uint32_t incoming_size;
        struct pollfd& poll_fd;
    };

  private:
    void thread_fn();

    // network related methods
    void start_server();
    void main_poll_thread_fn(int id);

    bool testRemove(struct pollfd x);
    void loop(int id);
    bool process(struct pollfd&, int id);

    // Model/ML related methods
    void checkpoint_model() const;
    std::shared_ptr<char> serialize_lr_model(const SparseLRModel&, uint64_t* model_size) const;
    void gradient_f();

    // message handling
    bool process_get_lr_sparse_model(const Request& req, std::vector<char>&);
    bool process_send_lr_gradient(const Request& req, std::vector<char>&);
    bool process_get_mf_sparse_model(const Request& req, std::vector<char>&, int tn);
    bool process_get_lr_full_model(const Request& req, std::vector<char>& thread_buffer);
    bool process_send_mf_gradient(const Request& req, std::vector<char>& thread_buffer);
    bool process_get_mf_full_model(const Request& req, std::vector<char>& thread_buffer);

    /**
      * Attributes
      */

    std::vector<uint64_t> curr_indexes = std::vector<uint64_t>(NUM_POLL_THREADS);
#if 0
    uint64_t server_clock = 0;  // minimum of all worker clocks
#endif
    std::unique_ptr<std::thread> thread; // worker threads
    std::vector<std::unique_ptr<std::thread>> server_threads;
    std::unique_ptr<std::thread> server_thread2;
    std::vector<std::unique_ptr<std::thread>> gradient_thread;
    pthread_t poll_thread;
    pthread_t main_thread;
    std::mutex to_process_lock;
    sem_t sem_new_req;
    std::queue<Request> to_process;
    const uint64_t n_threads = 4;
    std::mutex model_lock; // used to coordinate access to the last computed model

    int pipefds[NUM_POLL_THREADS][2] = {0};

    int port_ = 1337;
    int server_sock_ = 0;
    const uint64_t max_fds = 1000;
    int timeout = 1; // 1 ms
    std::vector<std::vector<struct pollfd>> fdses = std::vector<std::vector<struct pollfd>>(NUM_POLL_THREADS);

    std::vector<char> buffer; // we use this buffer to hold data from workers

    volatile uint64_t gradientUpdatesCount = 0;
    redisContext* redis_con;
    
    std::unique_ptr<SparseLRModel> lr_model; // last computed model
    std::unique_ptr<MFModel> mf_model; // last computed model
    Configuration task_config;
    uint32_t num_connections = 0;

    std::map<int, bool> task_to_status;
    std::map<int, std::string> operation_to_name;

    char* holder[4];
    int tc = 0;

};

class MFNetflixTask : public MLTask {
  public:
    MFNetflixTask(
        uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
        uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
        uint64_t SAMPLE_BASE, uint64_t START_BASE,
        uint64_t batch_size, uint64_t samples_per_batch,
        uint64_t features_per_sample, uint64_t nworkers,
        uint64_t worker_id) :
      MLTask(MODEL_GRAD_SIZE, MODEL_BASE,
          LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
          batch_size, samples_per_batch, features_per_sample,
          nworkers, worker_id)
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

        SparseMFModel get_new_model(
            const SparseDataset& ds, uint64_t user_base_index, uint64_t mb_size) {
          return psi->get_sparse_mf_model(ds, user_base_index, mb_size);
        }

      private:
        std::unique_ptr<PSSparseServerInterface> psi;
        std::string ps_ip;
        int ps_port;
    };

  private:
    bool get_dataset_minibatch(
        auto& dataset,
        auto& s3_iter);
    auto get_model(auto r, auto lmd);
    void push_gradient(MFSparseGradient&);
    void unpack_minibatch(std::shared_ptr<FEATURE_TYPE> /*minibatch*/,
        auto& samples, auto& labels);

    std::unique_ptr<MFModelGet> mf_model_get;
    std::unique_ptr<PSSparseServerInterface> psint;
};

}
#endif  // _TASKS_H_
