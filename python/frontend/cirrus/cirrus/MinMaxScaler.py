# MinMaxScaler normalization

import json
import time
import boto3
from collections import deque
from utils import get_all_keys, launch_lambdas
from lambdathread import LambdaThread
from threading import Thread

MAX_LAMBDAS = 400


class LocalBounds(LambdaThread):
    def __init__(self, s3_key, s3_bucket_input, use_redis):
        Thread.__init__(self)
        redis_signal = str(int(use_redis))
        self.lamdba_dict = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "action": "LOCAL_BOUNDS",
            "normalization": "MIN_MAX",
            "use_redis": redis_signal
        }


class LocalScale(LambdaThread):
    def __init__(self, s3_key, s3_bucket_input, s3_bucket_output, lower, upper, use_redis):
        Thread.__init__(self)
        redis_signal = str(int(use_redis))
        self.lamdba_dict = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "s3_bucket_output": s3_bucket_output,
            "action": "LOCAL_SCALE",
            "min_v": lower,
            "max_v": upper,
            "normalization": "MIN_MAX",
            "use_redis": redis_signal
        }


def MinMaxScaler(s3_bucket_input, s3_bucket_output, lower, upper, 
        objects=[], use_redis=True, dry_run=False, skip_bounds=False):
    """ Scale the values in a dataset to the range [lower, upper]. """
    s3_resource = boto3.resource("s3")
    if len(objects) == 0:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)

    # Calculate bounds for each chunk.
    start_bounds = time.time()
    if not skip_bounds:
        # Get the bounds
        launch_lambdas(LocalBounds, objects, max_lambdas=MAX_LAMBDAS, 
            s3_bucket_input, use_redis)

    print("LocalBounds took {0} seconds...".format(time.time() - start_bounds))
    # Aggregate the local maps if no Redis
    if not use_redis:
        no_redis_alternative(s3_bucket_input, objects)

    start_scale = time.time()
    if not dry_run:
        # Scale the chunks
        launch_lambdas(LocalScale, objects, max_lambdas=MAX_LAMBDAS, 
            s3_bucket_input, s3_bucket_output, lower, upper, use_redis)

    end_scale = time.time()
    print("Local scaling took {0} seconds...".format(end_scale - start_scale))

    # Delete any intermediary values in S3
    for i in objects:
        s3_resource.Object(s3_bucket_input, str(i) + "_bounds").delete()
        if not use_redis:
            s3_resource.Object(s3_bucket_input, str(i) +
                               "_final_bounds").delete()

    print("Deleting local maps took {0} seconds...".format(
        time.time() - end_scale))


def no_redis_alternative(s3_bucket_input, objects):
    start_global = time.time()
    client = boto3.client("s3")
    f_ranges = {}
    # Get global min/max map.
    for i in objects:
        obj = client.get_object(Bucket=s3_bucket_input,
                                Key=str(i) + "_bounds")["Body"].read()
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
    print("Creating the global map took {0} seconds...".format(
        end_global - start_global))
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
        client.put_object(Bucket=s3_bucket_input,
                          Key=str(i) + "_final_bounds", Body=s)

    print("Putting local maps took {0} seconds...".format(
        time.time() - end_global))
