import boto3
import json
import struct

def get_data_bounds(data):
    # Return a dict of two lists, containing max and min for each column.
    # Assumes labels are being stored right now.
    max_in_col = {}
    min_in_col = {}
    for r in data:
        for idx, v in r:
            if idx not in max_in_col:
                max_in_col[idx] = v
            if idx not in min_in_col:
                min_in_col[idx] = v
            if v > max_in_col[idx]:
                max_in_col[idx] = v
            if v < min_in_col[idx]:
                min_in_col[idx] = v
    return {
        "max": max_in_col,
        "min": min_in_col
    }

def get_data_from_s3(client, src_bucket, src_object):
    # Return a 2D list, where each element is a row of the dataset.
    b = client.get_object(Bucket=src_bucket, Key=src_object)["Body"].read()
    data = []
    c = []
    idx = None
    ignore_label = True
    num_values = None
    seen = 0
    for i in range(8, len(b), 4):
        if ignore_label:
            ignore_label = False
            continue
        if num_values is None:
            num_values = struct.unpack("i", b[i:i+4])[0]
            continue
        if seen % 2 == 0:
            idx = struct.unpack("i", b[i:i+4])[0]
        else:
            c.append((idx, struct.unpack("f", b[i:i+4])[0]))
        seen += 1
        if seen == num_values:
            data.append(c)
            c = []
            ignore_label = True
            num_values = None
            seen = 0
    return data

def put_bounds_in_s3(client, bounds, dest_bucket, dest_object):
    # Add the dictionary of bounds to an S3 bucket. 
    s = json.dumps(bounds)
    client.put_object(Bucket=dest_bucket, Key=dest_object, Body=s)

def get_global_bounds(client, bucket):
    b = client.get_object(Bucket=bucket, Key=bucket + "_final_ranges")["Body"].read()
    return json.loads(b)

def scale_data(data, g, min_v, max_v):
    data_f = []
    for idx, v in data:
        data_f.append(
            (idx,
                (v - g["min"][idx]) / (g["max"][idx] - g["min"][idx]) * (max_v - min_v) + min_v
            )
        )
    return data_f

def serialize_data(data):
    # TODO
    pass

def handler(event, context):
    # Either calculates the local bounds, or scales data and puts the new data in
    # {src_object}_scaled.
    print(event["src_bucket"], event["src_object"])
    client = boto3.client("s3")
    d = get_data_from_s3(client, event["src_bucket"], event["src_object"])
    if event["action"] == "LOCAL_BOUNDS":
        b = get_data_bounds(d)
        put_bounds_in_s3(client, b, event["src_bucket"], event["src_object"] + "_bounds")
    elif event["action"] == "LOCAL_SCALE":
        b = get_global_bounds(client, event["src_bucket"])
        scaled = scale_data(d, b, event["min_v"], event=["max_v"])
        serialized = serialize_data(scaled)
        client.put_object(Bucket=event["src_bucket"], event["src_object"] + "_scaled", Body=serialized)
    return []
