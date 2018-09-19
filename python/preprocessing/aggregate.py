import json
import boto3
from threading import Thread

class LocalBounds(Thread):
    def __init__(self, bucket, src_object):
        Thread.__init__(self)
        self.d = {
            "src_bucket": bucket,
            "action": "LOCAL_BOUNDS",
            "src_object": src_object
        }

    def run(self):
        l_client = boto3.client("lambda")
        l_client.invoke(FunctionName="neel_lambda", InvocationType="RequestResponse", LogType="Tail",
            Payload=json.dumps(self.d))

class LocalScale(LocalBounds):
    def __init__(self, bucket, src_object, lower, upper):
        Thread.__init__(self)
        self.d = {
            "src_bucket": bucket,
            "action": "LOCAL_SCALE",
            "src_object": src_object,
            "min_v": lower,
            "max_v": upper
        }

def MinMaxScaler(bucket_name, objects, lower, upper):
    l_client = boto3.client("lambda")
    b_threads = []
    for i in objects:
        l = LocalBounds(bucket_name, i)
        l.start()
        b_threads.append(l)

    for t in b_threads:
        t.join()

    client = boto3.client("s3")
    f_max = {}
    f_min = {}

    for i in objects:
        k = str(i) + "_bounds"
        obj = client.get_object(Bucket=bucket_name, Key=k)["Body"].read()
        d = json.loads(obj.decode("utf-8"))
        for idx in d["max"]:
            v = d["max"][idx]
            if idx not in f_max:
                f_max[idx] = v
            if v > f_max[idx]:
                f_max[idx] = v
        for idx in d["min"]:
            v = d["min"][idx]
            if idx not in f_min:
                f_min[idx] = v
            if v < f_min[idx]:
                f_min[idx] = v

    final = json.dumps({
        "max": f_max,
        "min": f_min
    })

    s = json.dumps(final)
    client.put_object(Bucket=bucket_name, Key=bucket_name + "_final_bounds", Body=s)
    g_threads = []
    for i in objects:
        g = LocalScale(bucket_name, i, lower, upper)
        g.start()
        g_threads.append(g)

    for t in g_threads:
        t.join()

