import boto3
import json
import struct
import time
from threading import Thread
from collections import deque


class ParallelFn(Thread):
    """ Run a function on a particular key value pair. """

    def __init__(self, fn, k, v=None, res=None, res_key=None):
        Thread.__init__(self)
        self.fn = fn
        self.k = k
        self.v = v
        self.res = res
        self.res_key = res_key

    def run(self):
        # If there is no value, just run it on the key
        if self.v is None:
            r = self.fn(self.k)
        else:
            r = self.fn(self.k, self.v)
        # If the invoker specified to store the response in a dictionary,
        # put it in
        if self.res is not None:
            self.res[self.res_key] = r


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


def push_keys_values_to_redis(node_manager, chunk, batch_push_to_redis, keys, values, redis_script):
    # Apply a Redis script to a list of Redis keys and values
    slot_k = {}
    slot_vals = {}
    if batch_push_to_redis:
        if node_manager is not None:
            # Separate the keys and values into slots determined by Redis
            t1 = time.time()
            for idx, k in enumerate(keys):
                slot = node_manager.keyslot(k)
                if slot not in slot_k:
                    slot_k[slot] = []
                    slot_vals[slot] = []
                slot_k[slot].append(k)
                slot_vals[slot].append(values[idx])
            print("[CHUNK{0}] Took {1} to make slot maps".format(
                chunk, time.time() - t1))
            t1 = time.time()
            w = deque()
            # Push the key / value batches in parallel
            for idx, k in enumerate(slot_k):
                if len(w) >= 4:
                    p2 = w.popleft()
                    p2.join()
                p = ParallelFn(redis_script, slot_k[k], slot_vals[k])
                p.start()
                w.append(p)
            for p in w:
                p.join()
            print("[CHUNK{0}] Took {1} to make {2} key / value requests".format(
                chunk, time.time() - t1, len(slot_k)))
        else:
            # If no node manager is specified, just push all at once
            redis_script(keys, values)
    else:
        # Push each key value pair one at a time
        for idx, k in enumerate(keys):
            redis_script([k], [values[idx]])


def put_bounds_in_db(s3_client, redis_client, bounds, dest_bucket, dest_object, use_redis, node_manager, chunk, batch_push_to_redis=True):
    # Add the dictionary of bounds to an S3 bucket or Redis instance.
    s = json.dumps(bounds)
    s3_client.put_object(Bucket=dest_bucket, Key=dest_object, Body=s)
    if not use_redis:
        # Stop here and let the master thread aggregate if not using Redis
        return

    max_f = redis_client.register_script(
        "for i, v in ipairs(KEYS) do local c = tonumber(redis.call('get', KEYS[i])); if c then if tonumber(ARGV[i]) > c then redis.call('set', KEYS[i], ARGV[i]) end else redis.call('set', KEYS[i], ARGV[i]) end end")
    min_f = redis_client.register_script(
        "for i, v in ipairs(KEYS) do local c = tonumber(redis.call('get', KEYS[i])); if c then if tonumber(ARGV[i]) < c then redis.call('set', KEYS[i], ARGV[i]) end else redis.call('set', KEYS[i], ARGV[i]) end end")
    max_k = []
    max_v = []
    min_k = []
    min_v = []
    t0 = time.time()
    # Add the maxima and minima for each key to a list
    for idx in bounds["max"]:
        # The maximum value for idx is at idx_max
        max_k.append(str(idx) + "_max")
        max_v.append(bounds["max"][idx])
        min_k.append(str(idx) + "_min")
        min_v.append(bounds["min"][idx])
    print("[CHUNK{0}] Took {1} to make lists".format(chunk, time.time() - t0))

    t0 = time.time()
    push_keys_values_to_redis(
        node_manager, chunk, batch_push_to_redis, max_k, max_v, max_f)
    print("[CHUNK{0}] max_f took {1}".format(chunk, time.time() - t0))

    t0 = time.time()
    push_keys_values_to_redis(
        node_manager, chunk, batch_push_to_redis, min_k, min_v, min_f)
    print("[CHUNK{0}] min_f took {1}".format(chunk, time.time() - t0))


def get_global_bounds(s3_client, redis_client, bucket, src_object, use_redis, chunk):
    # Get the bounds across all objects, where each key is mapped to [min, max].
    start = time.time()
    # Determine whether to get the aggregated global bounds from the master thread,
    # or the bounds from before and update (if using Redis)
    suffix = "_final_bounds"
    if use_redis:
        suffix = "_bounds"
    b = s3_client.get_object(
        Bucket=bucket, Key=src_object + suffix)["Body"].read().decode("utf-8")
    print("[CHUNK{0}] Global bounds are {1} bytes".format(chunk, len(b)))
    bounds = json.loads(b)
    if not use_redis:
        return bounds
    i = time.time()
    print("[CHUNK{0}] S3 took {1} seconds...".format(chunk, i - start))
    print(
        "[CHUNK{0}] Going to make {1} * 2 requests to Redis".format(chunk, len(bounds["max"])))
    # Get lists of the keys to get from Redis
    original = []
    max_k = []
    min_k = []
    for idx in bounds["max"]:
        original.append(idx)
        min_k.append(str(idx) + "_min")
        max_k.append(str(idx) + "_max")
    print("Constructing lists took {0} seconds".format(time.time() - i))
    i = time.time()
    res = {}
    # Get the keys in parallel
    p1 = ParallelFn(redis_client.mget, max_k, None, res, "max")
    p2 = ParallelFn(redis_client.mget, min_k, None, res, "min")
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    print("[CHUNK{0}] Getting 2 * {1} elements from Redis took {2}...".format(
        chunk, len(m["max"]), time.time() - i))
    # Format the map the way we want it
    max_v = res["max"]
    min_v = res["min"]
    i = time.time()
    d = {"max": {}, "min": {}}
    for idx, k in enumerate(original):
        d["max"][k] = max_v[idx]
        d["min"][k] = min_v[idx]
    print("[CHUNK{0}] Constructing the final dictionary took {1} seconds".format(
        chunk, time.time() - i))
    return d


def scale_data(data, g, new_min, new_max):
    for r in data:
        for j in range(len(r)):
            idx_t, v = r[j]
            idx = str(idx_t)
            s = (new_min + new_max) / 2.0
            min_v = float(g["min"][idx])
            max_v = float(g["max"][idx])
            if min_v != max_v:
                s = (v - min_v) / (max_v - min_v) * \
                    (new_max - new_min) + new_min
            r[j] = (idx, s)
    return data
