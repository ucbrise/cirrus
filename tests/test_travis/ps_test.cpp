#include <Configuration.h>
#include <Tasks.h>

cirrus::Configuration config;
config.read("configs/criteo_kaggle.cfg");
int main() {
  cirrus::PSSparseServerTask st((1 << config.get_model_bits()) + 1,
        config.get_minibatch_size(), config.get_minibatch_size(), config.get_num_features(),
        1, 1, "127.0.0.1", 1337);
  st.run(config);
}