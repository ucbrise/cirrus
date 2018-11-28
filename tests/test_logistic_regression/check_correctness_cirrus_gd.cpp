#include <cstdlib>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>

#include <InputReader.h>
#include <LRModel.h>

//typedef float FEATURE_TYPE;
const std::string INPUT_PATH = "criteo_data/day_1_100k_filtered";

template<typename Out>
void split(const std::string &s, char delim, Out result) {
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    *(result++) = item;
  }
}

std::vector<std::string> split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  split(s, delim, std::back_inserter(elems));
  return elems;
}

void print_info(const auto& samples) {
  std::cout << "Number of samples: " << samples.size() << std::endl;
  std::cout << "Number of cols: " << samples[0].size() << std::endl;
}

void check_error(auto model, auto dataset) {
  auto ret = model.calc_loss(dataset);
  auto loss = ret.first;
  auto num_samples = dataset.num_samples();
  auto avg_loss = loss / num_samples;
  std::cout << "total loss: " << loss
    << " avg loss: " << avg_loss
    << std::endl;
}

int main() {
  cirrus::InputReader input;
  cirrus::Dataset dataset = input.read_input_csv(INPUT_PATH, "\t", 1, 10000, 14,
                                                 true);  // normalize=true
  dataset.check();
  dataset.print_info();

  uint64_t num_cols = 13;
  cirrus::LRModel model(num_cols);

  double epsilon = 0.00001;
  double learning_rate = 0.0000001;

  for (uint64_t i = 0; 1; ++i) {
    auto gradient = model.minibatch_grad(dataset.samples_,
        const_cast<FEATURE_TYPE*>(dataset.labels_.get()),
        dataset.num_samples(), epsilon);
    model.sgd_update(learning_rate, gradient.get());

    if (i % 1024 == 0) {
      check_error(model, dataset);
    }
  }

  return 0;
}
