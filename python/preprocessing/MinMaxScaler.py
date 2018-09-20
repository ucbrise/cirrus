# MinMaxScaler normalization

import json
import time
import boto3
from threading import Thread

class LocalBounds(Thread):
    def __init__(self, s3_bucket_input, s3_key):
        Thread.__init__(self)
        self.d = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "action": "LOCAL_BOUNDS"
        }

    def run(self):
        l_client = boto3.client("lambda")
        l_client.invoke(FunctionName="neel_lambda", InvocationType="RequestResponse", LogType="Tail",
            Payload=json.dumps(self.d))

class LocalScale(LocalBounds):
    def __init__(self, s3_bucket_input, s3_key, s3_bucket_output, lower, upper):
        Thread.__init__(self)
        self.d = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "s3_bucket_output": s3_bucket_output,
            "action": "LOCAL_SCALE",
            "min_v": lower,
            "max_v": upper
        }

def MinMaxScaler(s3_bucket_input, s3_bucket_output, lower, upper, objects=[], dry_run=False):
    # TODO: Get number of keys to make objects list (strings of 1 ... n)
    start_bounds = time.time()
    l_client = boto3.client("lambda")
    b_threads = []
    for i in objects:
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
        s3_obj = client.Object(s3_bucket_input, str(i) + "_bounds")
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
    g_threads = []
    if not dry_run:
        for i in objects:
            g = LocalScale(s3_bucket_input, i, s3_bucket_output, lower, upper)
            g.start()
            g_threads.append(g)

        for t in g_threads:
            t.join()
    end_scale = time.time()
    print("Local scaling took {0} seconds...".format(end_scale - start_scale))

    for i in objects:
        client.Object(s3_bucket_input, str(i) + "_final_bounds").delete()

    print("Deleting local maps took {0} seconds...".format(time.time() - end_scale))