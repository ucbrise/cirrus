#ifndef _TASKS_SOFTMAX_H_
#define _TASKS_SOFTMAX_H_

#include <Configuration.h>
#include <string>

#include <client/TCPClient.h>
#include "config.h"

class MLTask {
 public:
     MLTask(const std::string& IP, const std::string& PORT,
             uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
             uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
             uint64_t SAMPLE_BASE, uint64_t START_BASE,
             uint64_t batch_size, uint64_t samples_per_batch,
             uint64_t features_per_sample, uint64_t nworkers,
             uint64_t worker_id) :
         IP(IP), PORT(PORT), MODEL_GRAD_SIZE(MODEL_GRAD_SIZE),
         MODEL_BASE(MODEL_BASE), LABEL_BASE(LABEL_BASE),
         GRADIENT_BASE(GRADIENT_BASE), SAMPLE_BASE(SAMPLE_BASE),
         START_BASE(START_BASE),
         batch_size(batch_size), samples_per_batch(samples_per_batch),
         features_per_sample(features_per_sample), nworkers(nworkers),
         worker_id(worker_id)
    {}

     /**
       * Worker here is a value 0..nworkers - 1
       */
     void run(const Configuration& config, int worker);

#ifdef USE_CIRRUS
    void wait_for_start(int index, cirrus::TCPClient& client);
#elif defined(USE_REDIS)
    void wait_for_start(int index, auto r);
#endif

 protected:
     std::string IP;
     std::string PORT;
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
};

class LogisticTask : public MLTask {
 public:
     LogisticTask(const std::string& IP, const std::string& PORT,
             uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
             uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
             uint64_t SAMPLE_BASE, uint64_t START_BASE,
             uint64_t batch_size, uint64_t samples_per_batch,
             uint64_t features_per_sample, uint64_t nworkers,
             uint64_t worker_id) :
         MLTask(IP, PORT, MODEL_GRAD_SIZE, MODEL_BASE,
                LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
                batch_size, samples_per_batch, features_per_sample,
                nworkers, worker_id)
    {}

     /**
       * Worker here is a value 0..nworkers - 1
       */
     void run(const Configuration& config, int worker);

 private:
};

class LogisticTaskPreloaded : public MLTask {
 public:
     LogisticTaskPreloaded(const std::string& IP, const std::string& PORT,
             uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
             uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
             uint64_t SAMPLE_BASE, uint64_t START_BASE,
             uint64_t batch_size, uint64_t samples_per_batch,
             uint64_t features_per_sample, uint64_t nworkers,
             uint64_t worker_id) :
     MLTask(IP, PORT, MODEL_GRAD_SIZE, MODEL_BASE,
             LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
             batch_size, samples_per_batch, features_per_sample,
             nworkers, worker_id)
    {}
     void get_data_samples(auto r,
                           uint64_t left_id, uint64_t right_id,
                           auto& samples, auto& labels);

     /**
       * Worker here is a value 0..nworkers - 1
       */
     void run(const Configuration& config, int worker);

 private:
};

class PSTask : public MLTask {
 public:
     PSTask(const std::string& IP, const std::string& PORT,
             uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
             uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
             uint64_t SAMPLE_BASE, uint64_t START_BASE,
             uint64_t batch_size, uint64_t samples_per_batch,
             uint64_t features_per_sample, uint64_t nworkers,
             uint64_t worker_id) :
         MLTask(IP, PORT, MODEL_GRAD_SIZE, MODEL_BASE,
                LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
                batch_size, samples_per_batch, features_per_sample,
                nworkers, worker_id)
    {}
     void run(const Configuration& config);

 private:
};

class ErrorTask : public MLTask {
 public:
     ErrorTask(const std::string& IP, const std::string& PORT,
             uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
             uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
             uint64_t SAMPLE_BASE, uint64_t START_BASE,
             uint64_t batch_size, uint64_t samples_per_batch,
             uint64_t features_per_sample, uint64_t nworkers,
             uint64_t worker_id) :
         MLTask(IP, PORT, MODEL_GRAD_SIZE, MODEL_BASE,
                LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
                batch_size, samples_per_batch, features_per_sample,
                nworkers, worker_id)
    {}
     void run(const Configuration& config);

 private:
};

class LoadingTask : public MLTask {
 public:
     LoadingTask(const std::string& IP, const std::string& PORT,
             uint64_t MODEL_GRAD_SIZE, uint64_t MODEL_BASE,
             uint64_t LABEL_BASE, uint64_t GRADIENT_BASE,
             uint64_t SAMPLE_BASE, uint64_t START_BASE,
             uint64_t batch_size, uint64_t samples_per_batch,
             uint64_t features_per_sample, uint64_t nworkers,
             uint64_t worker_id) :
         MLTask(IP, PORT, MODEL_GRAD_SIZE, MODEL_BASE,
                LABEL_BASE, GRADIENT_BASE, SAMPLE_BASE, START_BASE,
                batch_size, samples_per_batch, features_per_sample,
                nworkers, worker_id)
    {}
     void run(const Configuration& config);

 private:
};

#endif  //_TASKS_H_
