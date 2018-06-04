#include <unistd.h>
#include <cstdlib>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <InputReader.h>
#include <MFModel.h>

#define INPUT_PATH "movielens_data/ratings01.csv"

std::unique_ptr<MFModel> mf_model;

void movielens() {
  InputReader input;
  int number_movies, number_users;

  std::cout << "Reading movie dataset" << std::endl;
  SparseDataset dataset = input.read_movielens_ratings(INPUT_PATH, &number_users, &number_movies);
  dataset.check();
  dataset.print_info();

  // Initialize the model with initial values from dataset
  int nfactors = 200;
  mf_model.reset(new MFModel(number_users, number_movies, nfactors));

  std::cout << "Starting SGD learning" << std::endl;

  double learning_rate = 1;

  // SGD learning
  uint64_t batch_size = 200;
  double prev_loss = 0;
  double epsilon = 0.00001;
  for (uint64_t i = 0; 1; i += batch_size) {
    SparseDataset ds = dataset.sample_from(i, batch_size);

    // we update the model here
    mf_model->sgd_update(learning_rate, i, ds, epsilon);

    if (i % 100 == 0) {
      std::pair<double, double> ret = mf_model->calc_loss(dataset, i);
      double loss = ret.first;
      std::cout 
        << "Iteration " << i
        << " MSE: " << loss << std::endl;


      if (prev_loss == loss) {
        learning_rate *= 0.8;
        std::cout << "learning_rate: " << learning_rate << std::endl;
      }
      prev_loss = loss;
    }
  }
}

void netflix() {
  InputReader input;
  int number_movies, number_users;

  std::cout << "Reading movie dataset" << std::endl;
  SparseDataset dataset = input.read_netflix_ratings("nf_parsed", &number_users, &number_movies);
  dataset.check();
  dataset.print_info();
  std::cout
    << "Netflix number of users: " << number_users
    << "Netflix number of movies: " << number_movies
    << std::endl;

  // Initialize the model with initial values from dataset
  int nfactors = 10;
  mf_model.reset(new MFModel(number_users, number_movies, nfactors));

  std::cout << "Starting SGD learning" << std::endl;

  double learning_rate = 0.01;

  // SGD learning
  uint64_t batch_size = 20;
  double prev_loss = 0;
  double epsilon = 0.00001;
      
  while (1) {
    for (uint64_t i = 0; i + batch_size < number_users; i += batch_size) {
      //std::cout
      //  << "i: " << i
      //  << std::endl;
      SparseDataset ds = dataset.sample_from(i, batch_size);
  
      // we update the model here
      mf_model->sgd_update(learning_rate, i, ds, epsilon);

      //if (i == 2600000) {
      //  learning_rate *= 0.5;
      //}

      if (i % 100000 == 0) {
        std::pair<double, double> ret = mf_model->calc_loss(dataset, i);
        double loss = ret.first;
        std::cout 
          << "Iteration " << i
          << " MSE: " << loss
          << std::endl;

        if (prev_loss == loss) {
          learning_rate *= 0.9;
          std::cout << "learning_rate: " << learning_rate << std::endl;
        }
        prev_loss = loss;
      }
    }
  }
}

int main() {
  netflix();
  return 0;
}

