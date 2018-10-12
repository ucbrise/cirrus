""" Apply feature hashing to specified columns. """

import json
import time
from threading import Thread

import boto3
from lambda_thread import LambdaThread
from utils import get_all_keys, launch_lambdas

MAX_LAMBDAS = 400


class HashingThread(LambdaThread):
    """ Thread to hash the columns for a given chunk. """
    def __init__(self, s3_key, s3_bucket_input, s3_bucket_output, columns, N):
        Thread.__init__(self)
        self.lamdba_dict = {
            "s3_bucket_input": s3_bucket_input,
            "s3_bucket_output": s3_bucket_output,
            "s3_key": s3_key,
            "action": "FEATURE_HASHING",
            "columns": columns,
            "N": N,
            "use_redis": "1"
        }


def feature_hashing(s3_bucket_input, s3_bucket_output, columns, N, objects=()):
    """ Take a list of integer values (column indices) to perform the feature hashing
    with, for N buckets. """
    if len(objects) == 0:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)

    # Hash the appropriate columns for each chunk
    start_hash = time.time()
    # Launch one HashingThread for each object.
    launch_lambdas(HashingThread, objects, MAX_LAMBDAS,
                   s3_bucket_input, s3_bucket_output, columns, N)

    print("Feature hashing took {0} s".format(time.time() - start_hash))
