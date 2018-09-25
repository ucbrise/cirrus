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

def put_bounds_in_db(client, bounds, dest_bucket, dest_object, redis):
    # Add the dictionary of bounds to an S3 bucket or Redis instance.
    s = json.dumps(bounds)
    if not redis:
        client.put_object(Bucket=dest_bucket, Key=dest_object, Body=s)
    else:
        client.set(dest_bucket + "/" + dest_object, s)
        max_f = client.register_script("local c = tonumber(redis.call('get', KEYS[1])); if c then if tonumber(ARGV[1]) > c then redis.call('set', KEYS[1], ARGV[1]) return tonumber(ARGV[1]) - c else return 0 end else return redis.call('set', KEYS[1], ARGV[1]) end")
        for idx in bounds["max"]:
            max_f([str(idx) + "_max"], [bounds["max"][idx]])
        min_f = client.register_script("local c = tonumber(redis.call('get', KEYS[1])); if c then if tonumber(ARGV[1]) < c then redis.call('set', KEYS[1], ARGV[1]) return tonumber(ARGV[1]) - c else return 0 end else return redis.call('set', KEYS[1], ARGV[1]) end")
        for idx in bounds["min"]:
            min_f([str(idx) + "_min"], [bounds["min"][idx]])

def get_global_bounds(client, bucket, src_object, redis):
    # Get the bounds across all objects, where each key is mapped to [min, max].
    if redis:
        b = client.get(bucket + "/" + src_object + "_bounds")
        m = json.loads(b)
        for idx in m["max"]:
            v1 = client.get(str(idx) + "_min")
            v2 = client.get(str(idx) + "_max")
            m["min"][idx] = v1
            m["max"][idx] = v2
    else:
        b = client.get_object(Bucket=bucket, Key=src_object + "_final_bounds")["Body"].read()
        print("Global bounds are {0} bytes".format(len(b)))
        m = json.loads(b.decode("utf-8"))
    return m

def scale_data(data, g, new_min, new_max):
    for r in data:
        for j in range(len(r)):
            idx_t, v = r[j]
            idx = str(idx_t)
            s = (max_v + min_v) / 2.0
            min_v = g["min"][idx]
            max_v = g["max"][idx]
            if min_v != max_v:
                s = (v - min_v) / (max_v - min_v) * (new_max - new_min) + new_min
            r[j] = (idx, s)
    return data
