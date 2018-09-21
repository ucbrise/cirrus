# MinMaxScaler normalization

import json
import time
import boto3
from collections import deque
from serialization import LambdaThread

class LocalBounds(LambdaThread):
    def __init__(self, s3_bucket_input, s3_key):
        Thread.__init__(self)
        self.d = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "action": "LOCAL_BOUNDS",
            "normalization": "MIN_MAX"
        }

class LocalScale(LambdaThread):
    def __init__(self, s3_bucket_input, s3_key, s3_bucket_output, lower, upper):
        Thread.__init__(self)
        self.d = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "s3_bucket_output": s3_bucket_output,
            "action": "LOCAL_SCALE",
            "min_v": lower,
            "max_v": upper,
            "normalization": "MIN_MAX"
        }

def get_all_keys(bucket):
    s3 = boto3.client("s3")
    keys = []
    kwargs = {"Bucket": bucket}
    while True:
        r = s3.list_objects_v2(**kwargs)
        for o in r["Contents"]:
            keys.append(o["Key"])
        try:
            kwargs["ContinuationToken"] = r["NextContinuationToken"]
        except KeyError:
            break
    return keys

def MinMaxScaler(s3_bucket_input, s3_bucket_output, lower, upper, objects=[], dry_run=False):
    s3_resource = boto3.resource("s3")
    if len(objects) == 0:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)
        print("Found {0} chunks...".format(len(objects)))
    final_objects = []
    for o in objects:
        if "_" in o:
            s3_resource.Object(s3_bucket_input, o).delete()
        else:
            final_objects.append(o)
    objects = final_objects
    print("Chunks after pruning: {0}".format(len(objects)))
    # Calculate bounds for each chunk.
    start_bounds = time.time()
    l_client = boto3.client("lambda")
    b_threads = deque()
    for i in objects:
        while len(b_threads) > 400:
            t = b_threads.popleft()
            t.join()
        l = LocalBounds(s3_bucket_input, i)
        l.start()
        b_threads.append(l)

    for t in b_threads:
        t.join()

    start_global = time.time()
    print("LocalBounds took {0} seconds...".format(start_global - start_bounds))

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
        s3_obj.delete()

    start_scale = time.time()
    print("Putting local maps took {0} seconds...".format(start_scale - end_global))
    g_threads = deque()
    if not dry_run:
        for i in objects:
            if len(g_threads) > 400:
                t = g_threads.popleft()
                t.join()
            g = LocalScale(s3_bucket_input, i, s3_bucket_output, lower, upper)
            g.start()
            g_threads.append(g)

        for t in g_threads:
            t.join()
    end_scale = time.time()
    print("Local scaling took {0} seconds...".format(end_scale - start_scale))

    for i in objects:
        s3_resource.Object(s3_bucket_input, str(i) + "_final_bounds").delete()

    print("Deleting local maps took {0} seconds...".format(time.time() - end_scale))
