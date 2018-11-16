#ifndef _SPARSE_LRMODEL_H_
#define _SPARSE_LRMODEL_H_

#include <vector>
#include <utility>
#include <Model.h>
#include <SparseDataset.h>
#include <ModelGradient.h>
#include <Configuration.h>
#include <unordered_map>

namespace cirrus {

/**
  * Logistic regression model
  * Model is represented with a vector of FEATURE_TYPEs
  */
class SparseLRModel : public CirrusModel {
 public:
    friend class AdaGrad;
    friend class Momentum;
    friend class Nesterov;
    friend class SGD;
    /**
      * SparseLRModel constructor
      * @param d Features dimension
      */
    explicit SparseLRModel(uint64_t d);

    /**
      * SparseLRModel constructor from weight vector
      * @param w Array of model weights
      * @param d Features dimension
      */
    SparseLRModel(const FEATURE_TYPE* w, uint64_t d);

    /**
     * Set the model weights to values between 0 and 1
     */
    void randomize() override;

    /**
     * Loads model weights from serialized memory
     * @param mem Memory where model is serialized
     */
    void loadSerialized(const void* mem) override;
    void loadSerialized(const void* mem, int server_id, int num_ps);

    void loadSerializedSparse(const FEATURE_TYPE* weights,
                              const uint32_t* weight_indices,
                              uint64_t num_weights,
                              const Configuration& config);

    /**
      * serializes this model into memory
      * @return pair of memory pointer and size of serialized model
      */
    std::pair<std::unique_ptr<char[]>, uint64_t>
        serialize() const override;

    /**
      * serializes this model into memory pointed by mem
      */
    void serializeTo(void* mem) const;

    /**
     * Create new model from serialized weights
     * @param data Memory where the serialized model lives
     * @param size Size of the serialized model
     */
    std::unique_ptr<CirrusModel> deserialize(void* data,
            uint64_t size) const override;

    /**
     * Performs a deep copy of this model
     * @return New model
     */
    std::unique_ptr<CirrusModel> copy() const override;

    /**
     * Performs an SGD update in the direction of the input gradient
     * @param learning_rate Learning rate to be used
     * @param gradient Gradient to be used for the update
     */
    void sgd_update(double learning_rate, const ModelGradient* gradient);

    /**
     * Performs an SGD update using ADAGRAD
     * @param learning_rate Learning rate to be used
     * @param gradient Gradient to be used for the update
     */
    void sgd_update_adagrad(
        double learning_rate, const ModelGradient* gradient);

    /**
      * Performs an SGD update using MOMENTUM
      * @param learning_rate Learning rate to be used
      * @param gradient Gradient to be used for the update
      */

    void sgd_update_momentum(
	double learning_rate, double momentum_beta, const ModelGradient* gradient);

    /**
     * Returns the size of the model weights serialized
     * @returns Size of the model when serialized
     */
    uint64_t getSerializedSize() const override;

    double dot_product(
        const std::vector<std::pair<int, FEATURE_TYPE>>& v1,
        const std::vector<FEATURE_TYPE>& weights_) const;

    /**
     * Compute a minibatch gradient
     * @param dataset Dataset to learn on
     * @param labels Labels of the samples
     * @param labels_size Size of the labels array
     * @param epsilon L2 Regularization rate
     * @return Newly computed gradient
     */
    std::unique_ptr<ModelGradient> minibatch_grad(
        const SparseDataset& dataset,
        const Configuration& config) const;

    std::unique_ptr<ModelGradient> minibatch_grad_sparse(
        const SparseDataset& dataset,
        const Configuration& config) const;
    /**
     * Compute the logistic loss of a given dataset on the current model
     * @param dataset Dataset to calculate loss on
     * @return Total loss of whole dataset
     */
    std::pair<double, double> calc_loss(SparseDataset& dataset, uint32_t) const override;

    /**
     * Return the size of the gradient when serialized
     * @return Size of gradient when serialized
     */
    uint64_t getSerializedGradientSize() const override;

    /**
      * Builds a gradient that is stored serialized
      * @param mem Memory address where the gradient is serialized
      * @return Pointer to new gradient object
      */
    std::unique_ptr<ModelGradient> loadGradient(void* mem) const override;

    /**
      * Compute checksum of the model
      * @return Checksum of the model
      */
    double checksum() const override;

    /**
      * Print the model's weights
      */
    void print() const;

    /**
      * Return model size (should match sample size)
      * @return Size of the model
      */
    uint64_t size() const;

    /** Check if model weights are good
      */
    void check() const;

    FEATURE_TYPE get_nth_weight(uint64_t n) const override {
      return weights_[n];
    }

    FEATURE_TYPE get_nth_weight_nesterov(
            uint64_t n, double nesterov_beta) const {
      return weights_[n] + (nesterov_beta * momentum_avg);
    }

    void update_weights(std::vector<FEATURE_TYPE> weights) {
      weights_ = weights;
    }

    std::vector<FEATURE_TYPE> get_weights() {
      return weights_;
    }

    std::vector<FEATURE_TYPE> get_weight_history() {
      return weights_hist_;
    }

    void update_weight_history(std::vector<FEATURE_TYPE> weight_hist) {
      weights_hist_ = weight_hist;
    }
 protected:
    std::vector<FEATURE_TYPE> weights_;
    std::vector<FEATURE_TYPE> weights_hist_;

 private:
    void ensure_preallocated_vectors(const Configuration&) const;
    /**
      * Check whether value n is an integer
      */
    bool is_integer(FEATURE_TYPE n) const;

    bool is_sparse_ = false;

    //mutable std::unordered_map<uint32_t, FEATURE_TYPE> weights_sparse_;
    mutable std::vector<FEATURE_TYPE> weights_sparse_;

    FEATURE_TYPE momentum_avg = 0.0;
    double grad_threshold_ = 0;

    // we keep these vectors preallocated for performance reasons
    mutable std::vector<FEATURE_TYPE> part2;
    mutable std::vector<FEATURE_TYPE> part3;
    mutable std::vector<int> unique_indices;
};

} // namespace cirrus

#endif  // _LRMODEL_H_
