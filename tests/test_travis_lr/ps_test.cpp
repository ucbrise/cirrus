#include <Configuration.h>
#include <Tasks.h>

cirrus::Configuration config = cirrus::Configuration("configs/test_config.cfg");
int main() {
  cirrus::PSSparseServerTask st(
      (1 << config.get_model_bits()) + 1, config.get_minibatch_size(),
      config.get_minibatch_size(), config.get_num_features(), 2, 1, "127.0.0.1",
      1337);
  st.run(config);

  return 0;
}
