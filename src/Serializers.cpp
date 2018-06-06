#include <Serializers.h>
#include <iostream>

uint64_t lr_model_serializer::size(const LRModel& model) const {
    return model.getSerializedSize();
}

void lr_model_serializer::serialize(const LRModel& model, void* mem) const {
    model.serializeTo(mem);
}

LRModel
lr_model_deserializer::operator()(const void* data, unsigned int des_size) {
    LRModel model(n);
    if (des_size != model.getSerializedSize()) {
        throw std::runtime_error(
           std::string("Wrong deserializer size at lr_model_deserializer") +
           " Expected: " + std::to_string(model.getSerializedSize()) +
           " Got: " + std::to_string(des_size));
    }
    model.loadSerialized(data);

    return model;
}

uint64_t lr_gradient_serializer::size(const LRGradient& g) const {
    return g.getSerializedSize();
}

void lr_gradient_serializer::serialize(const LRGradient& g, void* mem) const {
    g.serialize(mem);
}

LRGradient
lr_gradient_deserializer::operator()(const void* data, unsigned int des_size) {
    LRGradient gradient(n);
    if (des_size != gradient.getSerializedSize()) {
      throw std::runtime_error(
          std::string("Wrong deserializer size at lr_gradient_deserializer") +
          " Expected: " + std::to_string(gradient.getSerializedSize()) +
          " Got: " + std::to_string(des_size));
    }

    gradient.loadSerialized(data);
    return gradient;
}


/*************************************************************************
  ************************************************************************
  * Softmax Serializers
  ************************************************************************
  ************************************************************************
  */
uint64_t sm_gradient_serializer::size(const SoftmaxGradient& g) const {
    return g.getSerializedSize();
}

void sm_gradient_serializer::serialize(
    const SoftmaxGradient& g, void* mem) const {
    g.serialize(mem);
}

SoftmaxGradient
sm_gradient_deserializer::operator()(const void* data, unsigned int des_size) {
    SoftmaxGradient gradient(nclasses, d);
    if (des_size != gradient.getSerializedSize()) {
        throw std::runtime_error(
                "Wrong deserializer size at sm_gradient_deserializer");
    }

    gradient.loadSerialized(data);
    return gradient;
}

uint64_t sm_model_serializer::size(const SoftmaxModel& model) const {
    return model.getSerializedSize();
}

void sm_model_serializer::serialize(
    const SoftmaxModel& model, void* mem) const {
    model.serializeTo(mem);
}

SoftmaxModel
sm_model_deserializer::operator()(const void* data, unsigned int des_size) {
    SoftmaxModel model(nclasses, d);
    if (des_size != model.getSerializedSize()) {
        throw std::runtime_error(
                "Wrong deserializer size at lr_model_deserializer");
    }
    model.loadSerialized(data);

    return model;
}
