""" Preprocessing module for Cirrus """

from enum import Enum
import sklearn.datasets

import boto3
import cirrus.feature_hashing as feature_hashing
import cirrus.min_max_scaler as min_max_scaler
import cirrus.normal_scaler as normal_scaler
from cirrus.utils import serialize_data, Timer

ROWS_PER_CHUNK = 50000


class Normalization(Enum):
    """ An enum to distinguish between normalization types.
    Use with the Preprocessing.normalize function. """
    MIN_MAX = 1,
    NORMAL = 2


class Preprocessing(object):
    """ Static preprocessing module for Min Max scaling, normal scaling. """
    def __init__(self):
        raise Exception, "Static class"

    @staticmethod
    def normalize(s3_bucket_input, s3_bucket_output, normalization_type, *args):
        """ Usage:
        Preprocessing.normalize(s3_bucket_input, s3_bucket_output,
                                Normalization.MIN_MAX, 0.0, 1.0)
        Preprocessing.normalize(s3_bucket_input, s3_bucket_output,
                                Normalization.NORMAL)
        """
        if normalization_type == Normalization.MIN_MAX:
            assert len(args) >= 2, "Must specify min and max."
            print(
                "[PREPROCESSING] Calling MinMaxScaler with args {0}"
                .format(args))
            min_max_scaler.min_max_scaler(s3_bucket_input, s3_bucket_output,
                                          *args)
        elif normalization_type == Normalization.NORMAL:
            normal_scaler.normal_scaler(s3_bucket_input, s3_bucket_output,
                                        *args)

    @staticmethod
    def feature_hashing(s3_bucket_input, s3_bucket_output, columns,
                        n_buckets, objects=()):
        """ Perform feature hashing on the specified columns.
        All other columns are untouched. """
        feature_hashing.feature_hashing(
            s3_bucket_input, s3_bucket_output, columns, n_buckets, objects)

    @staticmethod
    def load_libsvm(path, s3_bucket):
        """ Load a libsvm file into S3 in the specified bucket. """
        client = boto3.client("s3")
        timer = Timer("LOAD_LIBSVM").set_step("Reading file")
        data = sklearn.datasets.load_svmlight_file(path)[0]
        timer.timestamp().set_step("Starting loop")
        batch = [0] * ROWS_PER_CHUNK
        batch_num = 1
        batch_size = 0

        timer.timestamp().set_step("To lil")
        lil = data.tolil(copy=False) # Convert to list of lists format
        for row, (row_list, data) in enumerate(zip(lil.rows, lil.data)):
            # Iterate through the rows
            if row % 10000 == 0:
                timer.timestamp().set_step("Reading 10000 rows")
            curr_row = []
            for j, val in zip(row_list, data):
                curr_row.append((j, val))
            batch[batch_size] = curr_row
            batch_size += 1
            if batch_size == ROWS_PER_CHUNK:
                # Put the lines in S3, 50000 lines at a time
                timer.timestamp().set_step("Writing batch of {0} to S3"
                                           .format(ROWS_PER_CHUNK))
                serialized = serialize_data(batch)
                client.put_object(Bucket=s3_bucket, Key=str(batch_num),
                                  Body=serialized)
                batch = [0] * ROWS_PER_CHUNK
                batch_num += 1
                batch_size = 0
                timer.timestamp().set_step("Starting next batch")

        if batch_size > 0:
            # Put any remaining lines in S3
            timer.set_step("Trimming final batch")
            batch = batch[0:batch_size]
            timer.timestamp().set_step("Writing final batch to S3")
            serialized = serialize_data(batch)
            client.put_object(Bucket=s3_bucket, Key=str(batch_num),
                              Body=serialized)
            timer.timestamp()

        timer.global_timestamp()
