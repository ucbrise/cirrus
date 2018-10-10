# Preprocessing module for Cirrus

# TODO: Pytest

from Preprocessing import *
from utils import *
import boto3
from threading import Thread
import sklearn.datasets
import time
from collections import deque
import hashlib

MAX_THREADS = 400


class SimpleTest(Thread):
    def __init__(self, s3_bucket_input, s3_bucket_output, min_v, max_v, obj_key):
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
        for idx, r in enumerate(src_obj):
            for idx2, v in enumerate(r):
                try:
                    if dest_obj[idx][idx2][0] != v[0]:
                        print("[TEST_SIMPLE] Missing column {0} on row {1} of object {2}".format(
                            v[0], idx, self.obj_key))
                        return
                    if dest_obj[idx][idx2][1] < self.min_v or dest_obj[idx][idx2][1] > self.max_v:
                        print("[TEST_SIMPLE] Value {0} at column {1} on row {2} of object {3} falls out of bounds".format(
                            v[1], v[0], idx, self.obj_key
                        ))
                        return
                except Exception as e:
                    print("[TEST_SIMPLE] Caught error on row {0}, column {1} of object {2}: {3}".format(
                        idx, idx2, self.obj_key, e
                    ))
                    return


class HashTest(Thread):
    def __init__(self, s3_bucket_input, s3_bucket_output, columns, N, obj_key):
        Thread.__init__(self)
        self.s3_bucket_input = s3_bucket_input
        self.s3_bucket_output = s3_bucket_output
        self.columns = set([int(i) for i in columns])
        self.N = N
        self.obj_key = obj_key

    def run(self):
        client = boto3.client("s3")
        src_obj = get_data_from_s3(client, self.s3_bucket_input, self.obj_key)
        dest_obj = get_data_from_s3(
            client, self.s3_bucket_output, self.obj_key)
        for idx, r in enumerate(src_obj):
            row_old = {}
            for idx2, v in r:
                if idx2 in self.columns:
                    hasher = hashlib.sha256()
                    hasher.update(str(v))
                    c = int(hasher.digest(), 16) % N
                    if c not in row_old:
                        row_old[c] = 0
                    row_old[c] += 1
                else:
                    row_old[idx2] = v
            row_new = {}
            for idx2, v in dest_obj[idx]:
                row_new[idx2] = v
            assert len(row_new) == len(row_old)
            for k in row_new:
                assert row_new[k] == row_old[k]


def load_data(path):
    X, y = sklearn.datasets.load_svmlight_file(path)
    return X, y


def test_load_libsvm(src_file, s3_bucket_output, wipe_keys=False, no_load=False):
    if wipe_keys:
        t0 = time.time()
        print("[TEST_LOAD] Wiping keys in bucket")
        delete_all_keys(s3_bucket_output)
        print("[TEST_LOAD] Took {0} s to wipe all keys in bucket".format(
            time.time() - t0))
    if not no_load:
        t0 = time.time()
        print("[TEST_LOAD] Loading libsvm file into S3")
        Preprocessing.load_libsvm(src_file, s3_bucket_output)
        print("[TEST_LOAD] Took {0} s to load libsvm file into S3".format(
            time.time() - t0))
    t0 = time.time()
    print("[TEST_LOAD] Getting keys from bucket")
    objects = get_all_keys(s3_bucket_output)
    print("[TEST_LOAD] Took {0} s to get keys from bucket".format(
        time.time() - t0))
    t0 = time.time()
    print("[TEST_LOAD] Loading libsvm file into memory")
    X, y = load_data(src_file)
    print("[TEST_LOAD] Took {0} s to load libsvm into memory".format(
        time.time() - t0))
    t0 = time.time()
    print("[TEST_LOAD] Checking that all values are present")
    obj_num = 0
    obj_idx = -1
    client = boto3.client("s3")
    obj = get_data_from_s3(client, s3_bucket_output, objects[obj_num])
    # TODO: Potentially parallelize.
    for r in range(X.shape[0]):
        rows, cols = X[r, :].nonzero()
        obj_idx += 1
        if obj_idx >= 50000:
            obj_idx = 0
            obj_num += 1
            print("[TEST_LOAD] Finished chunk {0} at {1}".format(
                obj_num - 1, time.time() - t0))
            try:
                obj = get_data_from_s3(
                    client, s3_bucket_output, objects[obj_num])
            except Exception as e:
                print("[TEST_LOAD] Error: Not enough chunks given the number of rows in original data. Finished on chunk index {0}, key {1}.".format(
                    obj_num, objects[obj_num]))
                return False
        for idx, c in enumerate(cols):
            v_orig = X[r, c]
            try:
                v_obj = obj[obj_idx][idx]
            except Exception as e:
                print("[TEST_LOAD] Found error on row {0}, column {1} of the source data, row {2}, column {3} of chunk {4}".format(
                    r, c, obj_idx, idx, obj_num))
                return False
            if v_obj[0] != c:
                print("[TEST_LOAD] Value on row {0} of S3 object {1} has column {2}, expected column {3}".format(
                    obj_idx, obj_num, v_obj[0], c))
                continue
            if abs(v_obj[1] - v_orig) > .01:
                print("[TEST_LOAD] Value on row {0}, column {1} of S3 object {2} is {3}, expected {4} from row {5}, column {6} of original data".format(
                    obj_idx, c, obj_num, v_obj[1], v_orig, r, c))
    print("[TEST_LOAD] Testing all chunks of data took {0} s".format(
        time.time() - t0))


def test_simple(s3_bucket_input, s3_bucket_output, min_v, max_v, objects=[], preprocess=False, wipe_keys=False, skip_bounds=False):
    # Make sure all data is in bounds in output, and all data is present from input
    if wipe_keys:
        t0 = time.time()
        print("[TEST_SIMPLE] Wiping keys in bucket")
        delete_all_keys(s3_bucket_output)
        print("[TEST_SIMPLE] Took {0} to wipe keys".format(time.time() - t0))
    t0 = time.time()
    print("[TEST_SIMPLE] Getting all keys")
    if len(objects) == 0:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)
    print("[TEST_SIMPLE] Took {0} s to get all keys".format(time.time() - t0))
    if preprocess:
        t1 = time.time()
        print("[TEST_SIMPLE] Running preprocessing")
        Preprocessing.normalize(s3_bucket_input, s3_bucket_output,
                                Normalization.MIN_MAX, min_v, max_v, objects, True, False, skip_bounds)
        print("[TEST_SIMPLE] Took {0} s to run preprocessing".format(
            time.time() - t1))
    t0 = time.time()
    print("[TEST_SIMPLE] Starting threads for each object")
    threads = deque()
    # TODO: Turn into assertion; potentially add errors to list and check length of list.
    for o in objects:
        while len(threads) > MAX_THREADS:
            t = threads.popleft()
            t.join()
        t = SimpleTest(s3_bucket_input, s3_bucket_output, min_v, max_v, o)
        t.start()
        threads.append(t)
    print("[TEST_SIMPLE] Took {0} s to start all threads".format(
        time.time() - t0))
    for t in threads:
        t.join()
    print("[TEST_SIMPLE] Took {0} s for all threads to finish".format(
        time.time() - t0))


def test_hash(s3_bucket_input, s3_bucket_output, columns, N, objects=[], feature_hashing=True):
    t0 = time.time()
    print("[TEST_HASH] Getting all keys")
    if len(objects) == 0:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)
    print("[TEST_HASH] Took {0} s to get all keys".format(time.time() - t0))
    if feature_hashing:
        t1 = time.time()
        print("[TEST_HASH] Running feature hashing")
        Preprocessing.feature_hashing(
            s3_bucket_input, s3_bucket_output, columns, N, objects)
        print("[TEST_HASH] Took {0} s to run feature hashing".format(
            time.time() - t1))
    t0 = time.time()
    print("[TEST_HASH] Starting threads for each object")
    threads = deque()
    # TODO: Turn into assertion; potentially add errors to list and check length of list.
    for o in objects:
        while len(threads) > MAX_THREADS:
            t = threads.popleft()
            t.join()
        t = HashTest(s3_bucket_input, s3_bucket_output, columns, N, o)
        t.start()
        threads.append(t)
    print("[TEST_HASH] Took {0} s to start all threads".format(
        time.time() - t0))
    for t in threads:
        t.join()
    print("[TEST_HASH] Took {0} s for all threads to finish".format(
        time.time() - t0))


def test_exact(src_file, s3_bucket_output, min_v, max_v, objects=[], preprocess=False):
    """ Check that data was scaled correctly, assuming src_file was serialized sequentially into the keys
    specified in "objects". """
    original_obj = objects
    t0 = time.time()
    if len(objects) == 0:
        objects = get_all_keys(s3_bucket_output)
    print("[TEST_EXACT] Took {0} s to get all keys".format(time.time() - t0))
    if preprocess:
        t1 = time.time()
        print("[TEST_EXACT] Running load_libsvm")
        Preprocessing.load_libsvm(src_file, s3_bucket_output)
        print("[TEST_EXACT] Took {0} s to run load_libsvm".format(
            time.time() - t1))
        t1 = time.time()
        if len(original_obj) == 0:
            print("[TEST_EXACT] Fetching new objects list")
            objects = get_all_keys(s3_bucket_output)
            print("[TEST_EXACT] Fetched {0} objects in {1} s".format(
                len(objects), time.time() - t1))
        t1 = time.time()
        print("[TEST_EXACT] Running preprocessing")
        Preprocessing.normalize(
            s3_bucket_output, s3_bucket_output, Normalization.MIN_MAX, min_v, max_v, objects)
        print("[TEST_EXACT] Took {0} s to run preprocessing".format(
            time.time() - t1))
    t0 = time.time()
    print("[TEST_EXACT] Loading all data")
    X, y = load_data(src_file)
    print("[TEST_EXACT] Took {0} s to load all data".format(time.time() - t0))
    t0 = time.time()
    print("[TEST_EXACT] Constructing global map")
    g_map = {}  # Map of min / max by column
    for r in range(X.shape[0]):
        rows, cols = X[r, :].nonzero()
        for c in cols:
            v = X[r, c]
            if c not in g_map:
                g_map[c] = [v, v]
            if v < g_map[c][0]:
                g_map[c][0] = v
            if v > g_map[c][1]:
                g_map[c][1] = v
    print("[TEST_EXACT] Took {0} s to construct global map".format(
        time.time() - t0))
    t0 = time.time()
    client = boto3.client("s3")
    obj_num = 0
    obj_idx = -1
    obj = get_data_from_s3(client, s3_bucket_output, objects[obj_num])
    # TODO: Potentially parallelize.
    for r in range(X.shape[0]):
        rows, cols = X[r, :].nonzero()
        obj_idx += 1
        if obj_idx >= 50000:
            obj_idx = 0
            obj_num += 1
            try:
                obj = get_data_from_s3(
                    client, s3_bucket_output, objects[obj_num])
            except Exception as e:
                print("[TEST_LOAD] Error: Not enough chunks given the number of rows in original data. Finished on chunk index {0}, key {1}.".format(
                    obj_num, objects[obj_num]))
                return
        for idx, c in enumerate(cols):
            v_orig = X[r, c]
            v_obj = obj[obj_idx][idx]
            if v_obj[0] != c:
                print("[TEST_EXACT] Value on row {0} of S3 object {1} has column {2}, expected column {3}".format(
                    obj_idx, obj_num, v_obj[0], c))
                continue
            obj_min_v, obj_max_v = g_map[v_obj[0]]
            scaled = (v_orig - obj_min_v) / (obj_max_v -
                                             obj_min_v) * (max_v - min_v) + min_v
            if abs(scaled - v_obj[1]) / v_orig > .01:
                print("[TEST_EXACT] Value on row {0}, column {1} of S3 object {2} is {3}, expected {4} from row {5}, column {6} of original data".format(
                    obj_idx, c, obj_num, v_obj[1], scaled, r, c))
    print("[TEST_EXACT] Testing all chunks of data took {0} s".format(
        time.time() - t0))
