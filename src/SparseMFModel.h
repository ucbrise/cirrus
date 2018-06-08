#ifndef _SPARSEMFMODEL_H_
#define _SPARSEMFMODEL_H_

#include <vector>
#include <utility>
#include <unordered_map>
#include <Model.h>
#include <MFModel.h>
#include <Matrix.h>
#include <Dataset.h>
#include <ModelGradient.h>
#include <SparseDataset.h>
#include <Configuration.h>

namespace cirrus {

/**
  * Matrix Factorization model
  * Model is represented with a vector of FEATURE_TYPEs
  */

class SparseMFModel : public CirrusModel {
 public:
    /**
      * MFModel constructor from serialized data
      * @param w Serialized data
      * @param minibatch_size 
      * @param num_items
      */
    SparseMFModel(const void* w, uint64_t minibatch_size, uint64_t num_items);
    SparseMFModel(uint64_t users, uint64_t items, uint64_t factors);

    /**
     * Set the model weights to values between 0 and 1
     */
    void randomize();

    /**
     * Loads model weights from serialized memory
     * @param mem Memory where model is serialized
     */
    void loadSerialized(const void*) { throw std::runtime_error("Not implemented"); }
    void loadSerialized(const void* mem, uint64_t, uint64_t);

    /**
      * serializes this model into memory
      * @return pair of memory pointer and size of serialized model
      */
    std::pair<std::unique_ptr<char[]>, uint64_t>
        serialize() const;

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
            uint64_t size) const;

    /**
     * Performs a deep copy of this model
     * @return New model
     */
    std::unique_ptr<CirrusModel> copy() const;

    /**
     * Performs an SGD update in the direction of the input gradient
     * @param learning_rate Learning rate to be used
     * @param gradient Gradient to be used for the update
     */
    void sgd_update(double /*learning_rate*/, const ModelGradient* /*gradient*/) {
      throw std::runtime_error("Not implemented");
    }

    /**
     * Returns the size of the model weights serialized
     * @returns Size of the model when serialized
     */
    uint64_t getSerializedSize() const;

    /**
     * Compute a minibatch gradient
     * @param dataset Dataset to learn on
     * @param epsilon L2 Regularization rate
     * @return Newly computed gradient
     */
    //std::unique_ptr<ModelGradient> minibatch_grad(
    //        const Matrix& m,
    //        FEATURE_TYPE* labels,
    //        uint64_t labels_size,
    //        double epsilon) const;

    std::unique_ptr<ModelGradient> minibatch_grad(
        const SparseDataset& dataset,
        const Configuration&,
        uint64_t);

     
    //void sgd_update(double learning_rate,
    //            uint64_t base_user,
    //            const SparseDataset&);

    /**
     * Return the size of the gradient when serialized
     * @return Size of gradient when serialized
     */
    uint64_t getSerializedGradientSize() const;

    /**
      * Builds a gradient that is stored serialized
      * @param mem Memory address where the gradient is serialized
      * @return Pointer to new gradient object
      */
    std::unique_ptr<ModelGradient> loadGradient(void* mem) const;

    /**
      * Compute checksum of the model
      * @return Checksum of the model
      */
    double checksum() const;

    /**
      * Print the model's weights
      */
    void print() const;

    /**
      * Return model size (should match sample size)
      * @return Size of the model
      */
    //uint64_t size() const;

    void serializeFromDense(
        MFModel& model,
        uint32_t base_user, uint32_t minibatch_size, uint32_t k_items,
        const char* item_data_ptr, char* holder) const;

 public:

    // for each user we have in order:
    // 1. user id
    // 2. user_bias
    // 3. user weights
    std::vector<
      std::tuple<int, FEATURE_TYPE,
        std::vector<FEATURE_TYPE>>> user_models;
    
    // for each item we have in order:
    // 1. item id
    // 2. item_bias
    // 3. item weights
    std::pair<FEATURE_TYPE, std::vector<FEATURE_TYPE>> item_models[17770];

    FEATURE_TYPE& get_user_weights(uint64_t userId, uint64_t factor);
    FEATURE_TYPE& get_item_weights(uint64_t itemId, uint64_t factor);

    FEATURE_TYPE predict(uint32_t userId, uint32_t itemId);

    void initialize_weights(uint64_t, uint64_t, uint64_t);

    FEATURE_TYPE user_bias_reg_;
    FEATURE_TYPE item_bias_reg_;

    FEATURE_TYPE item_fact_reg_;
    FEATURE_TYPE user_fact_reg_;

    uint64_t nusers_;
    uint64_t nitems_;
    uint64_t nfactors_;

    FEATURE_TYPE global_bias_ = 0;

private:
    void check() const;
};

} // namespace cirrus

#endif  // _MFMODEL_H_
