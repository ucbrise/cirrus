#ifndef _SOFTMAXMODEL_H_
#define _SOFTMAXMODEL_H_

#include <Model.h>
#include <utility>
#include <vector>
#include <config.h>

namespace cirrus {

class SoftmaxModel : public CirrusModel {
 public:
    SoftmaxModel(uint64_t classes, uint64_t d);
    SoftmaxModel(const FEATURE_TYPE* data, uint64_t nclasses, uint64_t d);
    SoftmaxModel(std::vector<std::vector<FEATURE_TYPE>> data,
            uint64_t nclasses, uint64_t d);

    /**
      * Set the model weights to values between 0 and 1
      */
    void randomize();

    /**
      * Loads model weights from serialized memory
      * @param data Pointer to serialized model in memory
      */
    void loadSerialized(const void* data) override;

    /**
      * Serialized model into memory
      * @returns Pointer to memory and size of serialized model
      */
    std::pair<std::unique_ptr<char[]>, uint64_t>
    serialize() const;

    /**
      * serializes this model into memory pointed by mem
      */
    void serializeTo(void* mem) const;

    /**
      * Create new model from serialized weights
      * @param data Memory where serialized model lives
      * @param size Size of model when serialized
      * @return Deserialized model
      */
    std::unique_ptr<CirrusModel> deserialize(void* data,
                                       uint64_t size) const;

    /**
      * Performs a deep copy of this model
      * @return Copy of model
      */
    std::unique_ptr<CirrusModel> copy() const override;

    /**
      * Performsa stochastic gradient update in the direction of the input gradient
      * @param learning_rate SGD learning rate
      * @param Gradient used for the SGD update
      */
    void sgd_update(double learning_rate, const ModelGradient* gradient);

    /**
      * Returns the size of the model weights serialized
      * @return Model size when serialized
      */
    uint64_t getSerializedSize() const override;

    /**
      * Compute a minibatch gradient
      * @param dataset
      * @param labels
      * @param epsilon
      * @return
      */
    std::unique_ptr<ModelGradient> minibatch_grad(
            const Matrix& dataset,
            FEATURE_TYPE* labels,
            uint64_t labels_size,
            double epsilon) const override;
    /**
      * Compute the logistic loss of a given dataset on the current model
      * @areturn 
      */
    std::pair<double, double> calc_loss(Dataset& dataset) const override;

    /**
      * Return the size of the gradient when serialized
      * @return
      */
    uint64_t getSerializedGradientSize() const override;

    /**
      * Create new gradient from serialized gradient
      * @param mem
      * @return 
      */
    std::unique_ptr<ModelGradient> loadGradient(void* mem) const override;

    /**
      * Compute a checksum of this model's weights
      * @return Checksum
      */
    double checksum() const override;

 private:
    uint64_t nclasses;  //< number of classes in the dataset
    uint64_t d;         //< number of features for each sample

    std::vector<std::vector<FEATURE_TYPE>> weights;  //< model weights
};

} // namespace cirrus

#endif  // _SOFTMAXMODEL_H_
