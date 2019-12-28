# Logistic Regression on Diabetes Dataset
from random import seed
from random import randrange
from csv import reader
from math import exp
import time
import mmh3
import sys

HASH_SIZE = 2000

# result is a list of tuples (label, list of tuples (index, value))
# Load a CSV file
def load_csv_sparse_tab(filename):
  dataset = list()
  with open(filename, 'r') as file:
    csv_reader = reader(file, dialect="excel-tab")
    for row in csv_reader:
      tuples_list = []

      # add numerical values to list
      for i in range(1, 14):
        if row[i] == '':
          row[i] = "0"
        tuples_list.append((i-1, float(row[i])))

      global HASH_SIZE
      dic = {}
      for i in range(14, len(row)):
        if row[i] == '':
          continue
        hashed = mmh3.hash(row[i], 42)
        hashed = hashed % HASH_SIZE + 14

        if hashed in dic:
          dic[hashed] += 1
        else:
          dic[hashed] = 1

      for i in dic.keys():
        tuples_list.append((i, float(dic[i])))

      if len(tuples_list) == 0:
        print("ERROR")

      row_entry = (float(row[0]), tuples_list)
      dataset.append(row_entry)
  return dataset

# Convert string column to float
def str_column_to_float(dataset, column):
  for row in dataset:
    if row[column] == '':
      row[column] = "0"
    row[column] = float(row[column].strip())

# Find the min and max values for each column
def dataset_minmax(dataset):
  col_values = dict()
  col_counts = dict()
  col_min_max = dict()

  for row in dataset:
    row_values = row[1]
    for i in range(len(row_values)):
      index = row_values[i][0]
      value = row_values[i][1]

      if index in col_values:
        col_values[index] += value
        col_counts[index] += 1
        col_min_max[index] = ( \
                min(col_min_max[index][0], value),\
                max(col_min_max[index][1], value))
      else:
        col_values[index] = value
        col_counts[index] = 1
        col_min_max[index] = (value, value)

  return col_min_max

# Rescale dataset columns to the range 0-1
def normalize_dataset(dataset, minmax):
  for row in dataset:
    row_values = row[1]
    for i in range(len(row_values)):
      index = row_values[i][0]
      value = row_values[i][1]

      if minmax[index][0] == minmax[index][1]:
        continue
      new_value = (value - minmax[index][0])\
                  / (minmax[index][1] - minmax[index][0])

      row_values[i] = (row_values[i][0], new_value)

# Split a dataset into k folds
def cross_validation_split(dataset, n_folds):
  dataset_split = []
  dataset_copy = list(dataset)
  fold_size = int(len(dataset) / n_folds)
  for i in range(n_folds):
    fold = []
    while len(fold) < fold_size:
      index = randrange(len(dataset_copy))
      fold.append(dataset_copy.pop(index))
    print("len fold:", len(fold))
    dataset_split.append(fold)
  return dataset_split

# Calculate accuracy percentage
def accuracy_metric(actual, predicted):
  correct = 0
  for i in range(len(actual)):
    if actual[i] == predicted[i]:
      correct += 1
  return correct / float(len(actual)) * 100.0

# Evaluate an algorithm using a cross validation split
def evaluate_algorithm(dataset, algorithm, n_folds, *args):
  print("evaluate_algorithm")
  folds = cross_validation_split(dataset, n_folds)
  scores = []
  for fold in folds:
    start_time = time.time()
    print "Evaluating new fold (out of %d)" % len(folds)
    train_set = list(folds)
    train_set.remove(fold)
    train_set = sum(train_set, [])
    test_set = []
    for row in fold:
      row_copy = list(row)
      test_set.append(row_copy)
    predicted = algorithm(train_set, test_set, *args)
    actual = [row[0] for row in fold]
    accuracy = accuracy_metric(actual, predicted)
    print("Acc:", accuracy)
    scores.append(accuracy)
    print("Elapsed time fold: ", time.time() - start_time)
  return scores

# Make a prediction with coefficients
def predict(row_values, coefficients):
  yhat = coefficients[0]
  for i in range(len(row_values)):
    index = row_values[i][0]
    value = row_values[i][1]
    yhat += coefficients[index + 1] * value
  return 1.0 / (1.0 + exp(-yhat))

# Estimate logistic regression coefficients using stochastic gradient descent
def coefficients_sgd(train, l_rate, n_epoch):
  global HASH_SIZE
  coef = [0.00001 for i in range(HASH_SIZE+14 + 1)] # +1 for the bias
  for epoch in range(n_epoch):
    #print("epoch:", epoch)
    for row in train:
      label = row[0]
      values = row[1]

      yhat = predict(values, coef)
      error = label - yhat
      #print("yhat:", yhat)
      #print("error:", error)
      coef[0] = coef[0] + l_rate * error * yhat * (1.0 - yhat)
      for i in range(len(values)):
        index = values[i][0]
        value = values[i][1]
        coef[index + 1] = coef[index + 1] + l_rate * error * yhat * (1.0 - yhat) * value

  return coef

# Linear Regression Algorithm With Stochastic Gradient Descent
def logistic_regression(train, test, l_rate, n_epoch):
  print("logistic_regression")
  predictions = []
  coef = coefficients_sgd(train, l_rate, n_epoch)
  for row in test:
    label = row[0]
    values = row[1]

    yhat = predict(values, coef)
    yhat = round(yhat)
    predictions.append(yhat)
  return(predictions)


# Test the logistic regression algorithm on the diabetes dataset
seed(1)
# load and prepare data
#filename = 'day_1_1K.csv'
#filename = 'filtered_data_shuffled.csv'
filename = '/home/joao/Downloads/criteo_kaggle/train.txt_10K'
#filename = 'criteo_data/day_1_400K_shuffled'

print("Loading dataset")
dataset = load_csv_sparse_tab(filename)

#print("Processing dataset")
#for i in range(len(dataset[0])):
#    str_column_to_float(dataset, i)

# it's already normalized
# normalize
minmax = dataset_minmax(dataset)
normalize_dataset(dataset, minmax)
#print(dataset)

# evaluate algorithm
n_folds = 2
l_rate = 0.0000000001
n_epoch = 50

print("Evaluating algorithm")
scores = evaluate_algorithm(dataset, logistic_regression, n_folds, l_rate, n_epoch)
print('Scores: %s' % scores)
print('Mean Accuracy: %.3f%%' % (sum(scores)/float(len(scores))))
