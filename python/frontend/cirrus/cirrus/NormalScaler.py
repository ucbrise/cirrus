# Unit normal normalization

import json
import time
import boto3
from collections import deque
from utils import get_all_keys
from LambdaThread import LambdaThread
from threading import Thread

MAX_LAMBDAS = 400


class LocalRange(LambdaThread):
    def __init__(self, s3_bucket_input, s3_key):
        Thread.__init__(self)
        self.lamdba_dict = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "action": "LOCAL_RANGE",
            "normalization": "NORMAL"
        }


class LocalScale(LambdaThread):
    def __init__(self, s3_bucket_input, s3_key, s3_bucket_output):
        Thread.__init__(self)
        self.lamdba_dict = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "s3_bucket_output": s3_bucket_output,
            "action": "LOCAL_SCALE",
            "normalization": "NORMAL"
        }


def NormalScaler(s3_bucket_input, s3_bucket_output, objects=[], dry_run=False):
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
        l = LocalRange(s3_bucket_input, i)
        l.start()
        b_threads.append(l)

    for t in b_threads:
        t.join()

    start_global = time.time()
    print("LocalRange took {0} seconds...".format(start_global - start_bounds))

    f_ranges = get_global_map(s3_bucket_input, objects)

    end_global = time.time()
    print("Creating the global map took {0} seconds...".format(
        end_global - start_global))

    update_local_maps(s3_bucket_input, objects, f_ranges)

    start_scale = time.time()
    print("Putting local maps took {0} seconds...".format(
        start_scale - end_global))
    g_threads = deque()
    if not dry_run:
        for i in objects:
            if len(g_threads) > MAX_LAMBDAS:
                t = g_threads.popleft()
                t.join()
            g = LocalScale(s3_bucket_input, i, s3_bucket_output)
            g.start()
            g_threads.append(g)

        for t in g_threads:
            t.join()
    end_scale = time.time()
    print("Local scaling took {0} seconds...".format(end_scale - start_scale))

    for i in objects:
        s3_resource.Object(s3_bucket_input, str(i) + "_final_bounds").delete()

    print("Deleting local maps took {0} seconds...".format(
        time.time() - end_scale))


def get_global_map(s3_bucket_input, objects):
    # Aggregate the sample means, std. devs., etc. to get the global map.
    client = boto3.client("s3")
    f_ranges = {}
    for i in objects:
        obj = client.get_object(Bucket=s3_bucket_input,
                                Key=str(i) + "_bounds")["Body"].read()
        d = json.loads(obj.decode("utf-8"))
        for idx in d:
            sample_mean_x_squared, sample_mean_x, n = d[idx]
            if idx not in f_ranges:
                f_ranges[idx] = [
                    sample_mean_x_squared * n, sample_mean_x * n, n]
            else:
                f_ranges[idx][0] += sample_mean_x_squared * n
                f_ranges[idx][1] += sample_mean_x * n
                f_ranges[idx][2] += n


def update_local_maps(s3_bucket_input, objects, f_ranges):
    # Update the local maps of means, std. devs., etc.
    for i in objects:
        s3_obj = s3_resource.Object(s3_bucket_input, str(i) + "_bounds")
        obj = s3_obj.get()["Body"].read()
        d = json.loads(obj.decode("utf-8"))
        for idx in d:
            mean_x_sq = f_ranges[idx][0] / f_ranges[idx][2]
            mean = f_ranges[idx][1] / f_ranges[idx][2]
            d[idx] = [(mean_x_sq - mean**2)**(.5), mean]
        s = json.dumps(d)
        client.put_object(Bucket=s3_bucket_input,
                          Key=str(i) + "_final_bounds", Body=s)
        s3_obj.delete()
