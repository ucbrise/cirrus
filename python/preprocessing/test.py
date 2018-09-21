import sklearn.datasets
import time

start = time.time()
path = "../../tests/test_data/criteo.train.min.svm"
print("[{0} s] Reading file...".format(time.time() - start))
X, y = sklearn.datasets.load_svmlight_file(path)
print("[{0} s] Finished reading file...".format(time.time() - start))
batch = []
batch_num = 1
batch_size = 0
for r in range(X.shape[0]):
    if r % 10000 == 0:
        print("[{0} s] On row {1}...".format(time.time() - start, r))
    rows, cols = X[r, :].nonzero()
    curr_row = []
    for c in cols:
        curr_row.append((c, X[r, c]))
    batch.append(curr_row)
    batch_size += 1
    if batch_size == 50000:
        print("[{0} s] Writing batch {1}...".format(time.time() - start, batch_num))
        # TODO: Write to S3.
        
        batch = []
        batch_num += 1
        batch_size = 0

if batch_size > 0:
    print("[{0} s] Writing final batch {1}...".format(time.time() - start, batch_num))
    # TODO
