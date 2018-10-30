""" Apply feature hashing to specified columns. """

from cirrus.lambda_thread import LambdaThread
from cirrus.utils import get_all_keys, launch_threads, Timer,\
    get_redis_creds

MAX_LAMBDAS = 400


class HashingThread(LambdaThread):
    """ Thread to hash the columns for a given chunk. """
    def __init__(self, s3_key, s3_bucket_input, s3_bucket_output,
                 columns, n_buckets, creds):
        LambdaThread.__init__(self)
        self.lamdba_dict = {
            "s3_bucket_input": s3_bucket_input,
            "s3_bucket_output": s3_bucket_output,
            "s3_key": s3_key,
            "action": "FEATURE_HASHING",
            "columns": columns,
            "n_buckets": n_buckets,
            "use_redis": "1",
            "redis_host": creds["host"],
            "redis_db": creds["db"],
            "redis_password": creds["password"],
            "redis_port": creds["port"]
        }


def feature_hashing(s3_bucket_input, s3_bucket_output, columns,
                    n_buckets, objects=()):
    """ Take a list of integer values (column indices) to perform
    the feature hashing for n_buckets buckets. """
    if not objects:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)

    # Hash the appropriate columns for each chunk
    timer = Timer("FEATURE_HASHING")
    # Launch one HashingThread for each object.
    creds = get_redis_creds()
    launch_threads(HashingThread, objects, MAX_LAMBDAS,
                   s3_bucket_input, s3_bucket_output, columns, n_buckets,
                   creds)

    timer.global_timestamp()
