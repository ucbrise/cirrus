# Helper functions for S3.

from threading import Thread
import boto3
import json
import struct
import time

class LambdaThread(Thread):
    def run(self):
        l_client = boto3.client("lambda")
        failure = 1
        overall = time.time()
        while failure < 4:
            try:
                t0 = time.time()
                l_client.invoke(FunctionName="neel_lambda", InvocationType="RequestResponse", LogType="Tail",
                    Payload=json.dumps(self.d))
                print("Lambda for chunk {0} completed this attempt in {1}, all attempts in {2}".format(self.d["s3_key"], time.time() - t0, time.time() - overall))
                break
            except Exception as e:
                failure += 1
                if failure == 4:
                    raise e
                print("Lambda failed for chunk {0}: Launching attempt #{1}".format(self.d["s3_key"], failure))

def get_all_keys(bucket):
    s3 = boto3.client("s3")
    s3_resource = boto3.resource("s3")
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

    print("Found {0} chunks...".format(len(keys)))
    final_objects = []
    for o in keys:
        if "_" in o:
            s3_resource.Object(bucket, o).delete()
        else:
            final_objects.append(o)
    print("Chunks after pruning: {0}".format(len(final_objects)))
    return final_objects

def get_data_from_s3(client, src_bucket, src_object, keep_label=False):
    # Return a 2D list, where each element is a row of the dataset.
    print("Getting bytes from boto3")
    b = client.get_object(Bucket=src_bucket, Key=src_object)["Body"].read()
    print("Got {0} bytes".format(len(b)))
    data = []
    labels = []
    c = []
    idx = None
    label_bytes = None
    num_values = None
    seen = 0
    print("Set local variables")
    for i in range(8, len(b), 4):
        if label_bytes is None:
            label_bytes = b[i:i+4]
            if keep_label:
                labels.append(label_bytes)
            continue
        if num_values is None:
            num_values = struct.unpack("i", b[i:i+4])[0]
            continue
        if seen % 2 == 0:
            idx = struct.unpack("i", b[i:i+4])[0]
        else:
            c.append((idx, struct.unpack("f", b[i:i+4])[0]))
        seen += 1
        if seen == num_values * 2:
            data.append(c)
            c = []
            label_bytes = None
            num_values = None
            seen = 0
    if keep_label:
        return data, labels
    return data

def serialize_data(data, labels=None):
    DEFAULT = struct.pack("i", 0)
    lines = []
    num_bytes = 0
    for idx in range(len(data)):
        c = []
        l = DEFAULT
        if labels is not None:
            l = labels[idx]
        c.append(l)
        c.append(struct.pack("i", len(data[idx])))
        for idx2, v2 in data[idx]:
            c.append(struct.pack("i", int(idx2)))
            c.append(struct.pack("f", float(v2)))
        lines.append(b"".join(c))
        num_bytes += len(lines[-1])
    return struct.pack("i", num_bytes + 8) + struct.pack("i", len(labels)) + b"".join(lines)
