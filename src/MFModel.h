#ifndef _MFMODEL_H_
#define _MFMODEL_H_

#include <vector>
#include <utility>
#include <Model.h>
#include <Matrix.h>
#include <Dataset.h>
#include <ModelGradient.h>
#include <SparseDataset.h>

namespace cirrus {

/**
  * Matrix Factorization model
  * Model is represented with a vector of FEATURE_TYPEs
  */

class MFModel : public CirrusModel {
 public:
    /**
      * MFModel constructor from weight vector
      * @param w Array of model weights
      * @param d Features dimension
      */
    MFModel(const void*, uint64_t, uint64_t, uint64_t);
    MFModel(uint64_t users, uint64_t items, uint64_t factors);

    /**
     * Set the model weights to values between 0 and 1
     */
    void randomize();

    /**
     * Loads model weights from serialized memory
     * @param mem Memory where model is serialized
     */
    void loadSerialized(const void* mem);

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
    void sgd_update(double learning_rate, const ModelGradient* gradient);

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
    std::unique_ptr<ModelGradient> minibatch_grad(
            const Matrix& m,
            FEATURE_TYPE* labels,
            uint64_t labels_size,
            //const SparseDataset& dataset,
            double epsilon) const;

    std::unique_ptr<ModelGradient> minibatch_grad(
        double learning_rate,
        uint64_t base_user,
        const SparseDataset& dataset,
        double epsilon) const;

     
    void sgd_update(double learning_rate,
                uint64_t base_user,
                const SparseDataset&,
                double epsilon);

    /**
     * Compute the logistic loss of a given dataset on the current model
     * @param dataset Dataset to calculate loss on
     * @return Total loss of whole dataset
     */
    std::pair<double, double> calc_loss(Dataset& dataset) const;
    
    std::pair<double, double> calc_loss(SparseDataset& dataset) const;
    
    std::pair<double, double> calc_loss(SparseDataset& dataset, uint32_t start_index) const;

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
    uint64_t size() const;

 private:
    void initialize_reg_params();
    void initialize_data(uint64_t, uint64_t, uint64_t);
    
    // predict rating that userId gives to itemId
    FEATURE_TYPE predict(uint32_t userId, uint32_t itemId) const;

    uint64_t nusers_;
    uint64_t nitems_;
    uint64_t nfactors_;

 public:
    const FEATURE_TYPE& get_user_weights(uint64_t userId, uint64_t factor) const;
    const FEATURE_TYPE& get_item_weights(uint64_t itemId, uint64_t factor) const;
    FEATURE_TYPE& get_user_weights(uint64_t userId, uint64_t factor);
    FEATURE_TYPE& get_item_weights(uint64_t itemId, uint64_t factor);
    FEATURE_TYPE& get_user_bias(uint64_t userId);
    FEATURE_TYPE& get_item_bias(uint64_t itemId);

    std::vector<FEATURE_TYPE> user_weights_;
    std::vector<FEATURE_TYPE> item_weights_;

    std::vector<FEATURE_TYPE> user_bias_;
    std::vector<FEATURE_TYPE> item_bias_;

    FEATURE_TYPE user_bias_reg_;
    FEATURE_TYPE item_bias_reg_;

    FEATURE_TYPE item_fact_reg_;
    FEATURE_TYPE user_fact_reg_;

    FEATURE_TYPE global_bias_ = 0;
};

} // namespace cirrus

#endif  // _MFMODEL_H_
