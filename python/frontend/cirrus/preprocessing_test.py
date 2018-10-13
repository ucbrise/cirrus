""" Preprocessing module for Cirrus """

import time
from threading import Thread

import sklearn.datasets

import boto3
from botocore.exceptions import ClientError
import mmh3
from cirrus.preprocessing import Preprocessing, Normalization
from cirrus.utils import get_all_keys, delete_all_keys, get_data_from_s3, \
    launch_lambdas

MAX_THREADS = 400
HASH_SEED = 42  # Must be equal to the seed in feature_hashing_helper.py


class SimpleTest(Thread):
    """ Test that the data is within the correct bounds. """

    def __init__(self, obj_key, s3_bucket_input, s3_bucket_output, min_v,
                 max_v):
        Thread.__init__(self)
        self.s3_bucket_input = s3_bucket_input
        self.s3_bucket_output = s3_bucket_output
        self.min_v = min_v
        self.max_v = max_v
        self.obj_key = obj_key

    def run(self):
        client = boto3.client("s3")
        src_obj = get_data_from_s3(client, self.s3_bucket_input, self.obj_key)
        dest_obj = get_data_from_s3(
            client, self.s3_bucket_output, self.obj_key)
        for idx, row in enumerate(src_obj):
            for idx2, val in enumerate(row):
                try:
                    if dest_obj[idx][idx2][0] != val[0]:
                        print("[TEST_SIMPLE] Missing column {0} " +
                              "on row {1} of object {2}".format(
                                  val[0], idx, self.obj_key))
                        return
                    if dest_obj[idx][idx2][1] < self.min_v or \
                          dest_obj[idx][idx2][1] > self.max_v:
                        print("[TEST_SIMPLE] Value {0} at column {1} on " +
                              "row {2} of object {3} falls out of bounds"
                              .format(val[1], val[0], idx, self.obj_key))
                        return
                except (IndexError, KeyError) as exc:
                    print("[TEST_SIMPLE] Caught error on row {0}, column {1} " +
                          "of object {2}: {3}".format(
                              idx, idx2, self.obj_key, exc))
                    return


class HashTest(Thread):
    """ Test that the columns were hashed correctly. """

    def __init__(self, obj_key, s3_bucket_input, s3_bucket_output,
                 columns, n_buckets):
        Thread.__init__(self)
        self.s3_bucket_input = s3_bucket_input
        self.s3_bucket_output = s3_bucket_output
        self.columns = set([int(i) for i in columns])
        self.n_buckets = n_buckets
        self.obj_key = obj_key

    def run(self):
        client = boto3.client("s3")
        src_obj = get_data_from_s3(client, self.s3_bucket_input, self.obj_key)
        dest_obj = get_data_from_s3(
            client, self.s3_bucket_output, self.obj_key)
        for idx, src_row in enumerate(src_obj):
            row_old = {}
            for idx2, val in src_row:
                if idx2 in self.columns:
                    hash_val = mmh3.hash(str(val), HASH_SEED, signed=False)
                    bucket = hash_val % self.n_buckets
                    if bucket not in row_old:
                        row_old[bucket] = 0
                    row_old[bucket] += 1
                else:
                    row_old[idx2] = val
            row_new = {}
            for idx2, val in dest_obj[idx]:
                row_new[idx2] = val
            assert len(row_new) == len(row_old)
            for k in row_new:
                assert row_new[k] == row_old[k]


def load_data(path):
    """ Load a libsvm file. """
    data, labels = sklearn.datasets.load_svmlight_file(path)
    return data, labels


def test_load_libsvm(src_file, s3_bucket_output, wipe_keys=False,
                     no_load=False):
    """ Test the load libsvm to S3 function. """
    if wipe_keys:
        start_time = time.time()
        print("[TEST_LOAD] Wiping keys in bucket")
        delete_all_keys(s3_bucket_output)
        print("[TEST_LOAD] Took {0} s to wipe all keys in bucket".format(
            time.time() - start_time))
    if not no_load:
        start_time = time.time()
        print("[TEST_LOAD] Loading libsvm file into S3")
        Preprocessing.load_libsvm(src_file, s3_bucket_output)
        print("[TEST_LOAD] Took {0} s to load libsvm file into S3".format(
            time.time() - start_time))
    start_time = time.time()
    print("[TEST_LOAD] Getting keys from bucket")
    objects = get_all_keys(s3_bucket_output)
    print("[TEST_LOAD] Took {0} s to get keys from bucket".format(
        time.time() - start_time))
    start_time = time.time()
    print("[TEST_LOAD] Loading libsvm file into memory")
    data = load_data(src_file)[0]
    print("[TEST_LOAD] Took {0} s to load libsvm into memory".format(
        time.time() - start_time))
    start_time = time.time()
    print("[TEST_LOAD] Checking that all values are present")
    obj_num = 0
    obj_idx = -1
    client = boto3.client("s3")
    obj = get_data_from_s3(client, s3_bucket_output, objects[obj_num])
    for row in range(data.shape[0]):
        cols = data[row, :].nonzero()[1]
        obj_idx += 1
        if obj_idx >= 50000:
            obj_idx = 0
            obj_num += 1
            print("[TEST_LOAD] Finished chunk {0} at {1}".format(
                obj_num - 1, time.time() - start_time))
            try:
                obj = get_data_from_s3(
                    client, s3_bucket_output, objects[obj_num])
            except ClientError as exc:
                print("[TEST_LOAD] Error: Not enough chunks given" +
                      " the number of rows in original data. Finished " +
                      "on chunk index {0}, key {1}. Exception: {0}"
                      .format(obj_num, objects[obj_num], exc))
                return False
        for idx, col in enumerate(cols):
            v_orig = data[row, col]
            try:
                v_obj = obj[obj_idx][idx]
            except IndexError as exc:
                print("[TEST_LOAD] Found error on row {0}, column {1} of the " +
                      "source data, row {2}, column {3} of chunk {4}".format(
                          row, col, obj_idx, idx, obj_num))
                return False
            if v_obj[0] != col:
                print("[TEST_LOAD] Value on row {0} of S3 object {1} has" +
                      " column {2}, expected column {3}".format(
                          obj_idx, obj_num, v_obj[0], col))
                continue
            if abs(v_obj[1] - v_orig) > .01:
                print("[TEST_LOAD] Value on row {0}, column {1} of S3" +
                      " object {2} is {3}, expected {4} from row {5}," +
                      " column {6} of original data"
                      .format(obj_idx, col, obj_num, v_obj[1], v_orig,
                              row, col))
    print("[TEST_LOAD] Testing all chunks of data took {0} s".format(
        time.time() - start_time))
    return True


def test_simple(s3_bucket_input, s3_bucket_output, min_v, max_v,
                objects=(), preprocess=False, wipe_keys=False,
                skip_bounds=False):
    """ Make sure all data is in bounds in output, and all data
    is present from input """
    if wipe_keys:
        start_time = time.time()
        print("[TEST_SIMPLE] Wiping keys in bucket")
        delete_all_keys(s3_bucket_output)
        print("[TEST_SIMPLE] Took {0} to wipe keys".format(
            time.time() - start_time))
    start_time = time.time()
    print("[TEST_SIMPLE] Getting all keys")
    if not objects:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)
    print("[TEST_SIMPLE] Took {0} s to get all keys".format(
        time.time() - start_time))
    if preprocess:
        last_time = time.time()
        print("[TEST_SIMPLE] Running preprocessing")
        Preprocessing.normalize(s3_bucket_input, s3_bucket_output,
                                Normalization.MIN_MAX, min_v, max_v,
                                objects, True, False, skip_bounds)
        print("[TEST_SIMPLE] Took {0} s to run preprocessing".format(
            time.time() - last_time))
    start_time = time.time()
    print("[TEST_SIMPLE] Starting threads for each object")
    launch_lambdas(SimpleTest, objects, 400, s3_bucket_input,
                   s3_bucket_output, min_v, max_v)
    print("[TEST_SIMPLE] Took {0} s for all threads to finish".format(
        time.time() - start_time))


def test_hash(s3_bucket_input, s3_bucket_output, columns, n_buckets,
              objects=(), feature_hashing=True):
    """ Test that feature hashing was correct """
    start_time = time.time()
    print("[TEST_HASH] Getting all keys")
    if not objects:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)
    print("[TEST_HASH] Took {0} s to get all keys".format(
        time.time() - start_time))
    if feature_hashing:
        last_time = time.time()
        print("[TEST_HASH] Running feature hashing")
        Preprocessing.feature_hashing(
            s3_bucket_input, s3_bucket_output, columns, n_buckets, objects)
        print("[TEST_HASH] Took {0} s to run feature hashing".format(
            time.time() - last_time))
    start_time = time.time()
    print("[TEST_HASH] Starting threads for each object")
    launch_lambdas(HashTest, objects, 400, s3_bucket_input,
                   s3_bucket_output, columns, n_buckets)
    print("[TEST_HASH] Took {0} s for all threads to finish".format(
        time.time() - start_time))


def test_exact(src_file, s3_bucket_output, min_v, max_v, objects=(),
               preprocess=False):
    """ Check that data was scaled correctly, assuming src_file was serialized
    sequentially into the keys specified in "objects". """
    original_obj = objects
    start_time = time.time()
    if not objects:
        objects = get_all_keys(s3_bucket_output)
    print("[TEST_EXACT] Took {0} s to get all keys".format(
        time.time() - start_time))
    if preprocess:
        last_time = time.time()
        print("[TEST_EXACT] Running load_libsvm")
        Preprocessing.load_libsvm(src_file, s3_bucket_output)
        print("[TEST_EXACT] Took {0} s to run load_libsvm".format(
            time.time() - last_time))
        last_time = time.time()
        if not original_obj:
            print("[TEST_EXACT] Fetching new objects list")
            objects = get_all_keys(s3_bucket_output)
            print("[TEST_EXACT] Fetched {0} objects in {1} s".format(
                len(objects), time.time() - last_time))
        last_time = time.time()
        print("[TEST_EXACT] Running preprocessing")
        Preprocessing.normalize(
            s3_bucket_output, s3_bucket_output, Normalization.MIN_MAX,
            min_v, max_v, objects)
        print("[TEST_EXACT] Took {0} s to run preprocessing".format(
            time.time() - last_time))
    start_time = time.time()
    print("[TEST_EXACT] Loading all data")
    data = load_data(src_file)[0]
    print("[TEST_EXACT] Took {0} s to load all data".format(
        time.time() - start_time))
    start_time = time.time()
    print("[TEST_EXACT] Constructing global map")
    g_map = {}  # Map of min / max by column
    for row in range(data.shape[0]):
        cols = data[row, :].nonzero()[1]
        for col in cols:
            val = data[row, col]
            if col not in g_map:
                g_map[col] = [val, val]
            if val < g_map[col][0]:
                g_map[col][0] = val
            if val > g_map[col][1]:
                g_map[col][1] = val
    print("[TEST_EXACT] Took {0} s to construct global map".format(
        time.time() - start_time))
    start_time = time.time()
    client = boto3.client("s3")
    obj_num = 0
    obj_idx = -1
    obj = get_data_from_s3(client, s3_bucket_output, objects[obj_num])
    for row in range(data.shape[0]):
        cols = data[row, :].nonzero()[1]
        obj_idx += 1
        if obj_idx >= 50000:
            obj_idx = 0
            obj_num += 1
            try:
                obj = get_data_from_s3(
                    client, s3_bucket_output, objects[obj_num])
            except ClientError as exc:
                print("[TEST_LOAD] Error: Not enough chunks given the " +
                      "number of rows in original data. Finished on " +
                      "chunk index {0}, key {1}. Exception: {1}".format(
                          obj_num, objects[obj_num], exc))
                return
        for idx, col in enumerate(cols):
            v_orig = data[row, col]
            v_obj = obj[obj_idx][idx]
            if v_obj[0] != col:
                print("[TEST_EXACT] Value on row {0} of S3 object {1}" +
                      " has column {2}, expected column {3}".format(
                          obj_idx, obj_num, v_obj[0], col))
                continue
            obj_min_v, obj_max_v = g_map[v_obj[0]]
            scaled = (v_orig - obj_min_v) / (obj_max_v - obj_min_v)
            scaled *= (max_v - min_v)
            scaled += min_v
            if abs(scaled - v_obj[1]) / v_orig > .01:
                print("[TEST_EXACT] Value on row {0}, column {1} of" +
                      " S3 object {2} is {3}, expected {4} from row " +
                      "{5}, column {6} of original data".format(
                          obj_idx, col, obj_num, v_obj[1], scaled, row, col))
    print("[TEST_EXACT] Testing all chunks of data took {0} s".format(
        time.time() - start_time))
