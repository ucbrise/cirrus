# Apply the hashing trick to specified columns

import json
import time
import boto3
from collections import deque
from serialization import LambdaThread, get_all_keys
from threading import Thread
import random

class HashingThread(LambdaThread):
    def __init__(self, s3_bucket_input, s3_bucket_output, s3_key, columns, N, invocation):
        Thread.__init__(self)
        self.d = {
            "s3_bucket_input": s3_bucket_input,
            "s3_bucket_output": s3_bucket_output,
            "s3_key": s3_key,
            "action": "HASHING_TRICK",
            "columns": columns,
            "N": N,
            "redis": "1",
            "invocation": invocation,
            "nonce": (random.random() * 1000000) // 1.0
        }

def HashingTrick(s3_bucket_input, s3_bucket_output, columns, N, objects=[]):
    # Take a list of integer values (column indices) to perform the hash trick with, for N buckets.
    s3_resource = boto3.resource("s3")
    if len(objects) == 0:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)
    # Calculate bounds for each chunk.
    start_hash = time.time()
    l_client = boto3.client("lambda")
    threads = deque()
    invocation = (random.random() * 100000) // 1.0
    for i in objects:
        while len(threads) > 400:
            t = threads.popleft()
            t.join()
        l = HashingThread(s3_bucket_input, s3_bucket_output, i, columns, N, invocation)
        l.start()
        threads.append(l)

    for t in threads:
        t.join()

    print("Hashing trick took {0} s".format(time.time() - start_hash))
