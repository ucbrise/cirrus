""" Preprocessing module for Cirrus """

import time

import sklearn.datasets
from enum import Enum

import boto3
import feature_hashing
import min_max_scaler
import normal_scaler
from utils import serialize_data

ROWS_PER_CHUNK = 50000


class Normalization(Enum):
    """ An enum to distinguish between normalization types.
    Use with the Preprocessing.normalize function. """
    MIN_MAX = 1,
    NORMAL = 2


class Preprocessing:
    """ Static preprocessing module for Min Max scaling, normal scaling. """
    def __init__(self):
        raise Exception, "Static class"

    @staticmethod
    def normalize(s3_bucket_input, s3_bucket_output, normalization_type, *args):
        """ Usage:
        Preprocessing.normalize(s3_bucket_input, s3_bucket_output, Normalization.MIN_MAX, 0.0, 1.0)
        Preprocessing.normalize(s3_bucket_input, s3_bucket_output, Normalization.NORMAL)
        """
        if normalization_type == Normalization.MIN_MAX:
            assert len(args) >= 2, "Must specify min and max."
            print(
                "[Preprocessing] Calling MinMaxScaler with args {0}".format(args))
            min_max_scaler.min_max_scaler(s3_bucket_input, s3_bucket_output, *args)
        elif normalization_type == Normalization.NORMAL:
            normal_scaler.normal_scaler(s3_bucket_input, s3_bucket_output, *args)

    @staticmethod
    def feature_hashing(s3_bucket_input, s3_bucket_output, columns, N, objects=()):
        """ Perform feature hashing on the specifiec columns.
        All other columns are untouched. """
        feature_hashing.feature_hashing(
            s3_bucket_input, s3_bucket_output, columns, N, objects)

    @staticmethod
    def load_libsvm(path, s3_bucket):
        """ Load a libsvm file into S3 in the specified bucket. """
        client = boto3.client("s3")
        start = time.time()
        print("[{0} s] Reading file...".format(time.time() - start))
        X, labels = sklearn.datasets.load_svmlight_file(path)
        print("[{0} s] Finished reading file...".format(time.time() - start))
        batch = []
        batch_num = 1
        batch_size = 0
        for row in range(X.shape[0]):
            # Iterate through the rows
            if row % 10000 == 0:
                print("[{0} s] On row {1}...".format(time.time() - start, row))
            rows, cols = X[row, :].nonzero()
            curr_row = []
            for col_idx in cols:
                curr_row.append((c, X[row, col_idx]))
            batch.append(curr_row)
            batch_size += 1
            if batch_size == ROWS_PER_CHUNK:
                # Put the lines in S3, 50000 lines at a time
                print("[{0} s] Writing batch {1}...".format(
                    time.time() - start, batch_num))
                serialized = serialize_data(batch)
                client.put_object(Bucket=s3_bucket, Key=str(batch_num),
                                  Body=serialized)
                batch = []
                batch_num += 1
                batch_size = 0

        if batch_size > 0:
            # Put any remaining lines in S3
            print("[{0} s] Writing final batch {1}...".format(
                time.time() - start, batch_num))
            serialized = serialize_data(batch)
            client.put_object(Bucket=s3_bucket, Key=str(batch_num),
                              Body=serialized)

        print("[{0} s] Finished writing to S3.".format(time.time() - start))
