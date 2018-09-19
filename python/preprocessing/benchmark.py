from aggregate import MinMaxScaler
import time

for j in range(3):
    start = time.time()
    MinMaxScaler("criteo-kaggle-19b", [str(i) for i in range(1, 10**j + 1)], 0, 1)
    print("{0}: {1}".format(10**j, time.time() - start))
