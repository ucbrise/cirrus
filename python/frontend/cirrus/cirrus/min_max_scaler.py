""" MinMaxScaler normalization """

import json

import boto3
from cirrus.lambda_thread import LambdaThread
from cirrus.utils import get_all_keys, launch_threads, wipe_redis,\
    Timer, get_redis_creds

MAX_LAMBDAS = 400


class LocalBounds(LambdaThread):
    """ Calculate the max and min values for a given chunk """
    def __init__(self, s3_key, s3_bucket_input, use_redis, creds):
        LambdaThread.__init__(self)
        redis_signal = str(int(use_redis))
        self.lamdba_dict = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "action": "LOCAL_BOUNDS",
            "normalization": "MIN_MAX",
            "use_redis": redis_signal,
            "redis_host": creds["host"],
            "redis_db": creds["db"],
            "redis_password": creds["password"],
            "redis_port": creds["port"]
        }


class LocalScale(LambdaThread):
    """ Scale a chunk using the global max and min values """
    def __init__(self, s3_key, s3_bucket_input, s3_bucket_output,
                 lower, upper, use_redis, creds):
        LambdaThread.__init__(self)
        redis_signal = str(int(use_redis))
        self.lamdba_dict = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "s3_bucket_output": s3_bucket_output,
            "action": "LOCAL_SCALE",
            "min_v": lower,
            "max_v": upper,
            "normalization": "MIN_MAX",
            "use_redis": redis_signal,
            "redis_host": creds["host"],
            "redis_db": creds["db"],
            "redis_password": creds["password"],
            "redis_port": creds["port"]
        }


def min_max_scaler(s3_bucket_input, s3_bucket_output, lower, upper,
                   objects=(), use_redis=True, dry_run=False,
                   skip_bounds=False, delete_redis_keys=True):
    """ Scale the values in a dataset to the range [lower, upper]. """
    s3_resource = boto3.resource("s3")
    if not objects:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)

    # Wipe Redis from any previous runs
    if delete_redis_keys:
        wipe_redis()

    # Calculate bounds for each chunk.
    timer = Timer("MIN_MAX").set_step("LocalBounds")
    creds = get_redis_creds()
    if not skip_bounds:
        # Get the bounds
        launch_threads(LocalBounds, objects, MAX_LAMBDAS,
                       s3_bucket_input, use_redis, creds)

    timer.timestamp()
    # Aggregate the local maps if no Redis
    if not use_redis:
        no_redis_alternative(s3_bucket_input, objects)

    timer.set_step("LocalScale")
    if not dry_run:
        # Scale the chunks
        launch_threads(LocalScale, objects, MAX_LAMBDAS,
                       s3_bucket_input, s3_bucket_output,
                       lower, upper, use_redis, creds)

    timer.timestamp().set_step("Deleting local maps")

    # Delete any intermediary values in S3
    for i in objects:
        s3_resource.Object(s3_bucket_input, str(i) + "_bounds").delete()
        if not use_redis:
            s3_resource.Object(s3_bucket_input, str(i) +
                               "_final_bounds").delete()

    timer.timestamp()


def no_redis_alternative(s3_bucket_input, objects):
    """ Update the local maps with global maxes / mins using
    only S3. """
    timer = Timer("MIN_MAX").set_step("Creating the global map")
    client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    f_ranges = {}
    # Get global min/max map.
    for i in objects:
        obj = client.get_object(Bucket=s3_bucket_input,
                                Key=str(i) + "_bounds")["Body"].read()
        local_map = json.loads(obj.decode("utf-8"))
        for idx in local_map["min"]:
            val = local_map["min"][idx]
            if idx not in f_ranges:
                f_ranges[idx] = [val, val]
            if val < f_ranges[idx][0]:
                f_ranges[idx][0] = val
        for idx in local_map["max"]:
            val = local_map["max"][idx]
            if idx not in f_ranges:
                f_ranges[idx] = [val, val]
            if val > f_ranges[idx][1]:
                f_ranges[idx][1] = val

    timer.timestamp().set_step("Putting local maps")
    # Update local min/max maps.
    for i in objects:
        s3_obj = s3_resource.Object(s3_bucket_input, str(i) + "_bounds")
        obj = s3_obj.get()["Body"].read()
        local_map = json.loads(obj.decode("utf-8"))
        for idx in local_map["min"]:
            local_map["min"][idx] = f_ranges[idx][0]
        for idx in local_map["max"]:
            local_map["max"][idx] = f_ranges[idx][1]
        serialized = json.dumps(local_map)
        client.put_object(Bucket=s3_bucket_input,
                          Key=str(i) + "_final_bounds", Body=serialized)

    timer.timestamp()
