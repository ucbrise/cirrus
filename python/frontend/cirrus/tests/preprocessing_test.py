""" Tests for the preprocessing module for Cirrus.

You can get the test data from
{CIRRUS_ROOT}/tests/test_data/small_test_criteo.sh """

import os
import pickle
from threading import Thread
from enum import Enum

import boto3
from botocore.exceptions import ClientError
import mmh3
import sklearn.datasets

from context import cirrus
from cirrus.preprocessing import Preprocessing, Normalization
from cirrus.utils import get_all_keys, delete_all_keys, get_data_from_s3, \
    launch_threads, prefix_print, Timer

MAX_THREADS = 400
HASH_SEED = 42  # Must be equal to the seed in feature_hashing_helper.py
EPSILON = .0001 # Epsilon to determine if floating points are equal
LIBSVM_FILE = "criteo.small.svm"
INPUT_BUCKET = "criteo-kaggle-19b"
OUTPUT_BUCKET = "bucket-neel"

class Test(Enum):
    """ Indicate which test to run. """
    LOAD_LIBSVM = 1
    SIMPLE_TEST = 2
    MIN_MAX_TEST = 3
    HASH_TEST = 4
    NORMAL_TEST = 5
    ALL = 6
    NONE = 7

RUN_TEST = Test.NORMAL_TEST

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
        printer = prefix_print("TEST_SIMPLE")
        for idx, row in enumerate(src_obj):
            for idx2, val in enumerate(row):
                try:
                    if dest_obj[idx][idx2][0] != val[0]:
                        printer("Missing column {0} ".format(val[0]) +
                                "on row {1} of object {2}".format(
                                    idx, self.obj_key))
                        return
                    if dest_obj[idx][idx2][1] < self.min_v or \
                          dest_obj[idx][idx2][1] > self.max_v:
                        printer("Value {0} at column {1} on ".format(
                            val[1], val[0]) +
                                "row {2} of object {3} falls out of bounds"
                                .format(idx, self.obj_key))
                        return
                except (IndexError, KeyError) as exc:
                    printer("Caught error on row {0}, column {1} ".format(
                        idx, idx2) +
                            "of object {2}: {3}".format(
                                self.obj_key, exc))
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
                    row_old[self.n_buckets + idx2] = val
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
                     no_load=False, check_output=False):
    """ Test the load libsvm to S3 function. """
    printer = prefix_print("TEST_LOAD")
    timer = Timer("TEST_LOAD", verbose=True)
    if wipe_keys:
        timer.set_step("Wiping keys in bucket")
        delete_all_keys(s3_bucket_output)
        timer.timestamp()
    if not no_load:
        timer.set_step("Loading libsvm file into S3")
        Preprocessing.load_libsvm(src_file, s3_bucket_output)
        timer.timestamp()

    if not check_output:
        return True

    timer.set_step("Getting keys from bucket")
    objects = get_all_keys(s3_bucket_output)
    timer.timestamp().set_step("Loading libsvm file into memory")
    data = load_data(src_file)[0]
    timer.timestamp().set_step("Checking chunk 0")
    printer("Checking that all values are present")
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
            timer.timestamp().set_step("Checking chunk {0}".format(obj_num))
            try:
                obj = get_data_from_s3(
                    client, s3_bucket_output, objects[obj_num])
            except ClientError as exc:
                printer("Error: Not enough chunks given" +
                        " the number of rows in original data. Finished " +
                        "on chunk index {0}, key {1}. Exception: {0}"
                        .format(obj_num, objects[obj_num], exc))
                return False
        for idx, col in enumerate(cols):
            v_orig = data[row, col]
            try:
                v_obj = obj[obj_idx][idx]
            except IndexError as exc:
                printer("Found error on row {0}, column {1} of the ".format(
                    row, col) +
                        "source data, row {2}, column {3} of chunk {4}".format(
                            obj_idx, idx, obj_num))
                return False
            if v_obj[0] != col:
                printer("Value on row {0} of S3 object {1} has".format(
                    obj_idx, objects[obj_num]) +
                        " column {2}, expected column {3}".format(
                            v_obj[0], col))
                continue
            if abs(v_obj[1] - v_orig) > .01:
                printer("Value on row {0}, column {1} of S3".format(
                    obj_idx, col) +
                        " object {2} is {3}, expected {4} from row {5},".format(
                            objects[obj_num], v_obj[1], v_orig, row) +
                        " column {6} of original data"
                        .format(col))
    return True


def test_simple(s3_bucket_input, s3_bucket_output, min_v, max_v,
                objects=(), preprocess=True, wipe_keys=False,
                skip_bounds=False):
    """ Make sure all data is in bounds in output, and all data
    is present from input """
    timer = Timer("TEST_SIMPLE", verbose=True)
    if wipe_keys:
        timer.set_step("Wiping keys in bucket {}".format(s3_bucket_output))
        delete_all_keys(s3_bucket_output)
        timer.timestamp()
    timer.set_step("Getting all keys")
    if not objects:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)
    timer.timestamp()
    if preprocess:
        timer.set_step("Running preprocessing")
        Preprocessing.normalize(s3_bucket_input, s3_bucket_output,
                                Normalization.MIN_MAX, min_v, max_v,
                                objects, True, False, skip_bounds)
        timer.timestamp()
    timer.set_step("Running threads for each object")
    launch_threads(SimpleTest, objects, 400, s3_bucket_input,
                   s3_bucket_output, min_v, max_v)
    timer.timestamp()


def test_hash(s3_bucket_input, s3_bucket_output, columns, n_buckets,
              objects=(), feature_hashing=True):
    """ Test that feature hashing was correct """
    timer = Timer("TEST_HASH", verbose=True).set_step("Getting all keys")
    if not objects:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)
    timer.timestamp()
    if feature_hashing:
        timer.set_step("Running feature hashing")
        Preprocessing.feature_hashing(
            s3_bucket_input, s3_bucket_output, columns, n_buckets, objects)
        timer.timestamp()
    timer.set_step("Running threads for each object")
    launch_threads(HashTest, objects, 400, s3_bucket_input,
                   s3_bucket_output, columns, n_buckets)
    timer.timestamp()


def setup_test(timer, wipe_keys, preprocess, skip_load, src_file,
               s3_bucket_output):
    """ Wipe keys and load_libsvm if needed. """
    if wipe_keys:
        timer.set_step("Wiping keys in bucket")
        delete_all_keys(s3_bucket_output)
        timer.timestamp()
    if preprocess and not skip_load:
        timer.set_step("Running load_libsvm")
        Preprocessing.load_libsvm(src_file, s3_bucket_output)
        timer.timestamp()


def test_min_max(src_file, s3_bucket_output, min_v, max_v, objects=(),
                 preprocess=False, skip_load=False, wipe_keys=False,
                 check_global_map_cache=True):
    """ Check that data was scaled correctly, assuming src_file was serialized
    sequentially into the keys specified in "objects". """
    timer = Timer("TEST_MIN_MAX", verbose=True)
    setup_test(timer, wipe_keys, preprocess, skip_load, src_file,
               s3_bucket_output)
    timer.set_step("Getting all keys")
    if not objects:
        objects = get_all_keys(s3_bucket_output)
    timer.timestamp()
    if preprocess:
        timer.set_step("Running preprocessing")
        Preprocessing.normalize(
            s3_bucket_output, s3_bucket_output, Normalization.MIN_MAX,
            min_v, max_v, objects)
        timer.timestamp()
    timer.set_step("Loading all data")
    data = load_data(src_file)[0]
    timer.timestamp().set_step("Constructing global map")
    if check_global_map_cache and os.path.isfile("global_map"):
        with open("global_map", "rb") as f:
            g_map = pickle.load(f)
    else:
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
        with open("global_map", "wb") as f:
            pickle.dump(g_map, f)
    timer.timestamp().set_step("Testing all chunks")
    client = boto3.client("s3")
    obj_num = 0
    obj_idx = -1
    n_errors = 0
    obj = get_data_from_s3(client, s3_bucket_output, objects[obj_num])
    printer = prefix_print("TEST_MIN_MAX")
    for row in range(data.shape[0]):
        # row is the row in the libsvm file object
        # obj_idx is the row in the S3 object
        cols = data[row, :].nonzero()[1]
        obj_idx += 1
        if obj_idx >= 50000:
            obj_idx = 0
            obj_num += 1
            try:
                obj = get_data_from_s3(
                    client, s3_bucket_output, objects[obj_num])
            except ClientError as exc:
                printer("Error: Not enough chunks given the " +
                        "number of rows in original data. Finished on " +
                        "chunk index {0}, key {1}. Exception: {1}".format(
                            obj_num, objects[obj_num], exc))
                return
        for idx, col in enumerate(cols):
            try:
                v_orig = data[row, col]
                v_obj = obj[obj_idx][idx]
                if v_obj[0] != col:
                    # Ensure the column is correct
                    printer("Value on row {0} of S3 object {1}".format(
                        obj_idx, obj_num) +
                            " has column {2}, expected column {3}".format(
                                v_obj[0], col))
                    n_errors += 1
                    if n_errors == 10:
                        return
                obj_min_v, obj_max_v = g_map[col]
                scaled = (min_v + max_v) / 2.0
                if abs(obj_max_v - obj_min_v) > EPSILON:
                    scaled = (v_orig - obj_min_v) / (obj_max_v - obj_min_v)
                    scaled *= (max_v - min_v)
                    scaled += min_v
                if abs(scaled - v_obj[1]) > EPSILON:
                    printer("Value on row {0}, column {1} of".format(
                        obj_idx, col) +
                            " S3 object {0} is value {1}, expected {2} from row"
                            .format(obj_num, v_obj[1], scaled) +
                            " {0}, column {1} of libsvm file".format(
                                row, col))
                    printer("Min value {0}, max value {1}"
                            .format(obj_min_v, obj_max_v))
                    n_errors += 1
                    if n_errors == 10:
                        return
            except IndexError as exc:
                print("Exception on row {0} of S3 object {1}".format(
                    obj_idx, obj_num) +
                      ", row {0} col {1} of libsvm file".format(
                          row, col))
                print("Original data cols: {0}".format(cols))
                print("S3 data cols: {0}".format(obj[obj_idx]))
                raise exc
    timer.timestamp()


def test_normal(src_file, s3_bucket_output, objects=(),
                preprocess=False, skip_load=False, wipe_keys=False,
                check_global_map_cache=True):
    """ Check that data was normal scaled correctly. """
    timer = Timer("TEST_NORMAL", verbose=True)
    setup_test(timer, wipe_keys, preprocess, skip_load, src_file,
               s3_bucket_output)
    timer.set_step("Getting all keys")
    if not objects:
        objects = get_all_keys(s3_bucket_output)
    if preprocess:
        timer.set_step("Running preprocessing")
        Preprocessing.normalize(
            s3_bucket_output, s3_bucket_output, Normalization.NORMAL,
            objects)
        timer.timestamp()
    timer.set_step("Loading all data")
    data = load_data(src_file)[0]
    timer.timestamp().set_step("Constructing global map")
    if check_global_map_cache and os.path.isfile("global_map"):
        with open("global_map", "rb") as f:
            g_map = pickle.load(f)
    else:
        g_map = {}  # Map of sum(X), sum(X^2) and N by column
        for row in range(data.shape[0]):
            cols = data[row, :].nonzero()[1]
            for col in cols:
                val = data[row, col]
                if col not in g_map:
                    g_map[col] = [0, 0, 0]
                g_map[col][0] += val
                g_map[col][1] += val**2
                g_map[col][2] += 1
        with open("global_map", "wb") as f:
            pickle.dump(g_map, f)
    timer.timestamp().set_step("Testing all chunks")
    client = boto3.client("s3")
    obj_num = 0
    obj_idx = -1
    n_errors = 0
    obj = get_data_from_s3(client, s3_bucket_output, objects[obj_num])
    printer = prefix_print("TEST_NORMAL")
    for row in range(data.shape[0]):
        # row is the row in the libsvm file object
        # obj_idx is the row in the S3 object
        cols = data[row, :].nonzero()[1]
        obj_idx += 1
        if obj_idx >= 50000:
            obj_idx = 0
            obj_num += 1
            try:
                obj = get_data_from_s3(
                    client, s3_bucket_output, objects[obj_num])
            except ClientError as exc:
                printer("Error: Not enough chunks given the " +
                        "number of rows in original data. Finished on " +
                        "chunk index {0}, key {1}. Exception: {1}".format(
                            obj_num, objects[obj_num], exc))
                return
        for idx, col in enumerate(cols):
            try:
                v_orig = data[row, col]
                v_obj = obj[obj_idx][idx]
                if v_obj[0] != col:
                    # Ensure the column is correct
                    printer("Value on row {0} of S3 object {1}".format(
                        obj_idx, obj_num) +
                            " has column {2}, expected column {3}".format(
                                v_obj[0], col))
                    n_errors += 1
                    if n_errors == 10:
                        return
                sum_x, sum_x_squared, n_vals = g_map[col]
                mean = sum_x / float(n_vals)
                std_dev = 0
                variance = sum_x_squared / float(n_vals) - mean**2
                if abs(variance) < EPSILON:
                    std_dev = 0
                else:
                    std_dev = (variance)**.5
                scaled = 0
                if std_dev != 0:
                    scaled = (v_orig - mean) / std_dev
                if abs(scaled - v_obj[1]) > EPSILON:
                    printer("Value on row {0}, column {1} of".format(
                        obj_idx, col) +
                            " S3 object {0} is value {1}, expected {2} from row"
                            .format(obj_num, v_obj[1], scaled) +
                            " {0}, column {1} of libsvm file".format(
                                row, col))
                    printer("Original value {0}".format(v_orig))
                    printer("Sum(X) {0}, Sum(X^2) {1}, N {2}"
                            .format(sum_x, sum_x_squared, n_vals))
                    printer("E[X] {0}, E[X]^2 {1}, E[X^2] {2}"
                            .format(sum_x / float(n_vals),
                                    (sum_x / float(n_vals))**2,
                                    sum_x_squared / float(n_vals)))
                    n_errors += 1
                    if n_errors == 10:
                        return
            except IndexError as exc:
                print("Exception on row {0} of S3 object {1}".format(
                    obj_idx, obj_num) +
                      ", row {0} col {1} of libsvm file".format(
                          row, col))
                print("Original data cols: {0}".format(cols))
                print("S3 data cols: {0}".format(obj[obj_idx]))
                raise exc
    timer.timestamp()


def main():
    """ Run all of the tests. """
    timer = Timer("MAIN", verbose=True).set_step("Running load_libsvm")
    if RUN_TEST == Test.ALL or RUN_TEST == Test.LOAD_LIBSVM:
        test_load_libsvm(LIBSVM_FILE, OUTPUT_BUCKET, wipe_keys=True)

    timer.timestamp().set_step("Running test_simple")
    if RUN_TEST == Test.ALL or RUN_TEST == Test.SIMPLE_TEST:
        test_simple(INPUT_BUCKET, OUTPUT_BUCKET, 0.0, 1.0,
                    objects=["1", "2", "3"],
                    preprocess=True, wipe_keys=True)

    timer.timestamp().set_step("Running test_min_max")
    if RUN_TEST == Test.ALL or RUN_TEST == Test.MIN_MAX_TEST:
        test_min_max(LIBSVM_FILE, OUTPUT_BUCKET, 0.0, 1.0,
                     objects=["1", "2", "3"], preprocess=True, skip_load=False,
                     wipe_keys=True, check_global_map_cache=False)

    timer.timestamp().set_step("Running test_normal")
    if RUN_TEST == Test.ALL or RUN_TEST == Test.NORMAL_TEST:
        test_normal(LIBSVM_FILE, OUTPUT_BUCKET, objects=["1", "2", "3"],
                    preprocess=True, skip_load=False, wipe_keys=True,
                    check_global_map_cache=False)

    timer.timestamp().set_step("Running test_hash")
    if RUN_TEST == Test.ALL or RUN_TEST == Test.HASH_TEST:
        test_hash(INPUT_BUCKET, OUTPUT_BUCKET, ["40189"], 1000,
                  objects=["1", "2", "3"])
    timer.timestamp().global_timestamp()

if __name__ == "__main__":
    main()
