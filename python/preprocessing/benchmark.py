import preprocessing
import time

for j in range(3):
    start = time.time()
    preprocessing.normalize("criteo-kaggle-19b", "criteo-kaggle-19b", preprocessing.NormalizationType.MIN_MAX, 
        0, 1, [str(i) for i in range(1, 10**j + 1)], True)
    print("{0} lambdas: {1} seconds".format(10**j, time.time() - start))
