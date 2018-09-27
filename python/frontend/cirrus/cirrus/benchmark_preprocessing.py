import preprocessing
import time

start = time.time()
# preprocessing.normalize("criteo-kaggle-19b", "bucket-neel", preprocessing.NormalizationType.MIN_MAX, True, 0, 1, [str(i) for i in range(1, 100)])
preprocessing.normalize("criteo-kaggle-19b", "bucket-neel", preprocessing.NormalizationType.MIN_MAX, True, 0, 1)
# preprocessing.normalize("criteo-kaggle-19b", "bucket-neel", preprocessing.NormalizationType.NORMAL)
print("Total time: {0} seconds".format(time.time() - start))
