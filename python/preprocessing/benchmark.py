import preprocessing
import time

start = time.time()
# preprocessing.normalize("criteo-kaggle-19b", "bucket-neel", preprocessing.NormalizationType.MIN_MAX, 0, 1)
preprocessing.normalize("criteo-kaggle-19b", "bucket-neel", preprocessing.NormalizationType.NORMAL)
print("Total time: {0} seconds".format(time.time() - start))
