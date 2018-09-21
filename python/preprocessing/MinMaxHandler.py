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

def put_bounds_in_s3(client, bounds, dest_bucket, dest_object):
    # Add the dictionary of bounds to an S3 bucket. 
    s = json.dumps(bounds)
    client.put_object(Bucket=dest_bucket, Key=dest_object, Body=s)

def get_global_bounds(client, bucket, src_object):
    # Get the bounds across all objects.
    b = client.get_object(Bucket=bucket, Key=src_object + "_final_bounds")["Body"].read()
    print("Global bounds are {0} bytes".format(len(b)))
    return json.loads(b.decode("utf-8"))

def scale_data(data, g, min_v, max_v):
    for r in data:
        for j in range(len(r)):
            idx_t, v = r[j]
            idx = str(idx_t)
            s = (max_v + min_v) / 2.0
            if g["min"][idx] != g["max"][idx]:
                s = (v - g["min"][idx]) / (g["max"][idx] - g["min"][idx]) * (max_v - min_v) + min_v
            r[j] = (idx, s)
    return data