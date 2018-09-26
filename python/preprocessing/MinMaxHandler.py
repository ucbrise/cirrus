import boto3
import json
import struct
import time

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

def put_bounds_in_db(s3_client, redis_client, bounds, dest_bucket, dest_object, redis, node_manager, all_columns=True):
    # Add the dictionary of bounds to an S3 bucket or Redis instance.
    s = json.dumps(bounds)
    s3_client.put_object(Bucket=dest_bucket, Key=dest_object, Body=s)
    if redis:
        max_f = redis_client.register_script("for i, v in ipairs(KEYS) do local c = tonumber(redis.call('get', KEYS[i])); if c then if tonumber(ARGV[i]) > c then redis.call('set', KEYS[i], ARGV[i]) end else redis.call('set', KEYS[i], ARGV[i]) end end")
        min_f = redis_client.register_script("for i, v in ipairs(KEYS) do local c = tonumber(redis.call('get', KEYS[i])); if c then if tonumber(ARGV[i]) < c then redis.call('set', KEYS[i], ARGV[i]) end else redis.call('set', KEYS[i], ARGV[i]) end end")
        max_k = []
        max_v = []
        min_k = []
        min_v = []
        t0 = time.time()
        for idx in bounds["max"]:
            max_k.append(str(idx) + "_max")
            max_v.append(bounds["max"][idx])
            min_k.append(str(idx) + "_min")
            min_v.append(bounds["min"][idx])
        print("Took {0} to make lists".format(time.time() - t0))
        t0 = time.time()
        c = time.time()
        slot_max_k = {}
        slot_max_vals = {}
        if all_columns:
            if node_manager is not None:
                t1 = time.time()
                for idx, k in enumerate(max_k):
                    slot = node_manager.keyslot(k)
                    if slot not in slot_max_k:
                        slot_max_k[slot] = []
                        slot_max_vals[slot] = []
                    slot_max_k[slot].append(k)
                    slot_max_vals[slot].append(max_v[idx])
                print("Took {0} to make slot maps for max_f".format(time.time() - t1))
                t1 = time.time()
                t2 = time.time()
                for idx, k in enumerate(slot_max_k):
                    if idx % 100 == 0:
                        print("Iteration {0} took {1}".format(idx, time.time() - t2))
                    t2 = time.time()
                    max_f(slot_max_k[k], slot_max_vals[k])
                print("Took {0} to make {1} max_f requests".format(time.time() - t1, len(slot_max_k)))
            else:
                max_f(max_k, max_v)
        else:
            for idx, k in enumerate(max_k):
                if idx % 100 == 0:
                    print("Iteration {0} of max_f took {1}".format(idx, time.time() - c))
                c = time.time()
                max_f([k], [max_v[idx]])
        print("max_f took {0}".format(time.time() - t0))
        t0 = time.time()
        slot_min_k = {}
        slot_min_vals = {}
        if all_columns:
            if node_manager is not None:
                for idx, k in enumerate(min_k):
                    slot = node_manager.keyslot(k)
                    if slot not in slot_min_k:
                        slot_min_k[slot] = []
                        slot_min_vals[slot] = []
                    slot_min_k[slot].append(k)
                    slot_min_vals[slot].append(min_v[idx])
                for k in slot_min_k:
                    min_f(slot_min_k[k], slot_min_vals[k])
            else:
                min_f(min_k, min_v)
        else:
            for idx, k in enumerate(min_k):
                if idx % 100 == 0:
                    print("Iteration {0} of min_f took {1}".format(idx, time.time() - c))
                c = time.time()
                min_f([k], [min_v[idx]])
        print("min_f took {0}".format(time.time() - t0))

def get_global_bounds(s3_client, redis_client, bucket, src_object, redis):
    # Get the bounds across all objects, where each key is mapped to [min, max].
    start = time.time()
    suffix = "_final_bounds"
    if redis:
        suffix = "_bounds"
    b = s3_client.get_object(Bucket=bucket, Key=src_object + suffix)["Body"].read().decode("utf-8")
    print("Global bounds are {0} bytes".format(len(b)))
    m = json.loads(b)
    if not redis:
        return m
    i = time.time()
    print("S3 took {0} seconds...".format(i - start))
    print("Going to make {0} * 2 requests to Redis".format(len(m["max"])))
    original = []
    max_k = []
    min_k = []
    for idx in m["max"]:
        original.append(idx)
        min_k.append(str(idx) + "_min")
        max_k.append(str(idx) + "_max")
    print("Constructing lists took {0} seconds".format(time.time() - i))
    i = time.time()
    max_v = redis_client.mget(max_k)
    min_v = redis_client.mget(min_k)
    print("Getting 2 * {0} elements from Redis took {1}...".format(len(m["max"]), time.time() - i))
    i = time.time()
    d = {"max":{},"min":{}}
    for idx, k in enumerate(original):
        d["max"][k] = max_v[idx]
        d["min"][k] = min_v[idx]
    print("Constructing the final dictionary took {0} seconds".format(time.time() - i))
    return d

def scale_data(data, g, new_min, new_max):
    for r in data:
        for j in range(len(r)):
            idx_t, v = r[j]
            idx = str(idx_t)
            s = (new_min + new_max) / 2.0
            min_v = g["min"][idx]
            max_v = g["max"][idx]
            if min_v != max_v:
                s = (v - min_v) / (max_v - min_v) * (new_max - new_min) + new_min
            r[j] = (idx, s)
    return data
