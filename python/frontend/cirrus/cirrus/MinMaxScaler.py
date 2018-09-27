# MinMaxScaler normalization

import json
import time
import boto3
from collections import deque
from serialization import LambdaThread, get_all_keys
from threading import Thread
import random

MAX_LAMBDAS = 400

class LocalBounds(LambdaThread):
    def __init__(self, s3_bucket_input, s3_key, redis, invocation):
        Thread.__init__(self)
        redis_s = "1"
        if not redis:
            redis_s = "0"
        self.d = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "action": "LOCAL_BOUNDS",
            "normalization": "MIN_MAX",
            "redis": redis_s,
            "invocation": invocation,
            "nonce": (random.random() * 1000000) // 1.0
        }

class LocalScale(LambdaThread):
    def __init__(self, s3_bucket_input, s3_key, s3_bucket_output, lower, upper, redis, invocation):
        Thread.__init__(self)
        redis_s = "1"
        if not redis:
            redis_s = "0"
        self.d = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "s3_bucket_output": s3_bucket_output,
            "action": "LOCAL_SCALE",
            "min_v": lower,
            "max_v": upper,
            "normalization": "MIN_MAX",
            "redis": redis_s,
            "invocation": invocation,
            "nonce": (random.random() * 1000000) // 1.0
        }

def MinMaxScaler(s3_bucket_input, s3_bucket_output, lower, upper, objects=[], redis=True, dry_run=False):
    invocation = (random.random() * 1000000) // 1.0
    s3_resource = boto3.resource("s3")
    if len(objects) == 0:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)
    # Calculate bounds for each chunk.
    start_bounds = time.time()
    l_client = boto3.client("lambda")
    b_threads = deque()
    for i in objects:
        while len(b_threads) > MAX_LAMBDAS:
            t = b_threads.popleft()
            t.join()
        l = LocalBounds(s3_bucket_input, i, redis, invocation)
        l.start()
        b_threads.append(l)

    for t in b_threads:
        t.join()

    start_global = time.time()
    print("LocalBounds took {0} seconds...".format(start_global - start_bounds))
    if not redis:
        client = boto3.client("s3")
        f_ranges = {}
        # Get global min/max map.
        for i in objects:
            obj = client.get_object(Bucket=s3_bucket_input, Key=str(i) + "_bounds")["Body"].read()
            d = json.loads(obj.decode("utf-8"))
            for idx in d["min"]:
                v = d["min"][idx]
                if idx not in f_ranges:
                    f_ranges[idx] = [v, v]
                if v < f_ranges[idx][0]:
                    f_ranges[idx][0] = v
            for idx in d["max"]:
                v = d["max"][idx]
                if idx not in f_ranges:
                    f_ranges[idx] = [v, v]
                if v > f_ranges[idx][1]:
                    f_ranges[idx][1] = v

        end_global = time.time()
        print("Creating the global map took {0} seconds...".format(end_global - start_global))
        # Update local min/max maps.
        for i in objects:
            s3_obj = s3_resource.Object(s3_bucket_input, str(i) + "_bounds")
            obj = s3_obj.get()["Body"].read()
            d = json.loads(obj.decode("utf-8"))
            for idx in d["min"]:
                d["min"][idx] = f_ranges[idx][0]
            for idx in d["max"]:
                d["max"][idx] = f_ranges[idx][1]
            s = json.dumps(d)
            client.put_object(Bucket=s3_bucket_input, Key=str(i) + "_final_bounds", Body=s)

        print("Putting local maps took {0} seconds...".format(time.time() - end_global))
    start_scale = time.time()
    g_threads = deque()
    if not dry_run:
        for i in objects:
            if len(g_threads) > MAX_LAMBDAS:
                t = g_threads.popleft()
                t.join()
            g = LocalScale(s3_bucket_input, i, s3_bucket_output, lower, upper, redis, invocation)
            g.start()
            g_threads.append(g)

        for t in g_threads:
            t.join()
    end_scale = time.time()
    print("Local scaling took {0} seconds...".format(end_scale - start_scale))

    for i in objects:
        s3_resource.Object(s3_bucket_input, str(i) + "_bounds").delete()
        if not redis:
            s3_resource.Object(s3_bucket_input, str(i) + "_final_bounds").delete()

    print("Deleting local maps took {0} seconds...".format(time.time() - end_scale))
