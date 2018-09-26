import boto3
import json
import struct
import time
from threading import Thread
from collections import deque

class ParallelFn(Thread):
    def __init__(self, fn, k, v=None, res=None, res_key=None):
        Thread.__init__(self)
        self.fn = fn
        self.k = k
        self.v = v
        self.res = res
        self.res_key = res_key

    def run(self):
        if self.v is None:
            r = self.fn(self.k)
        else:
            r = self.fn(self.k, self.v)
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

def put_bounds_in_db(s3_client, redis_client, bounds, dest_bucket, dest_object, redis, node_manager, chunk, all_columns=True):
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
        print("[CHUNK{0}] Took {1} to make lists".format(chunk, time.time() - t0))
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
                print("[CHUNK{0}] Took {1} to make slot maps for max_f".format(chunk, time.time() - t1))
                t1 = time.time()
                w = deque()
                for idx, k in enumerate(slot_max_k):
                    if len(w) >= 4:
                        p2 = w.popleft()
                        p2.join()
                    p = ParallelFn(max_f, slot_max_k[k], slot_max_vals[k])
                    p.start()
                    w.append(p)
                for p in w:
                    p.join()
                print("[CHUNK{0}] Took {1} to make {2} max_f requests".format(chunk, time.time() - t1, len(slot_max_k)))
            else:
                max_f(max_k, max_v)
        else:
            for idx, k in enumerate(max_k):
                # if idx % 100 == 0:
                #     print("Iteration {0} of max_f took {1}".format(idx, time.time() - c))
                c = time.time()
                max_f([k], [max_v[idx]])
        print("[CHUNK{0}] max_f took {1}".format(chunk, time.time() - t0))
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
                w = deque()
                for k in slot_min_k:
                    if len(w) >= 4:
                        p2 = w.popleft()
                        p2.join()
                    p = ParallelFn(min_f, slot_min_k[k], slot_min_vals[k])
                    p.start()
                    w.append(p)
                for p in w:
                    p.join()
            else:
                min_f(min_k, min_v)
        else:
            for idx, k in enumerate(min_k):
                # if idx % 100 == 0:
                #     print("Iteration {0} of min_f took {1}".format(idx, time.time() - c))
                c = time.time()
                min_f([k], [min_v[idx]])
        print("[CHUNK{0}] min_f took {1}".format(chunk, time.time() - t0))

def get_global_bounds(s3_client, redis_client, bucket, src_object, redis, chunk):
    # Get the bounds across all objects, where each key is mapped to [min, max].
    start = time.time()
    suffix = "_final_bounds"
    if redis:
        suffix = "_bounds"
    b = s3_client.get_object(Bucket=bucket, Key=src_object + suffix)["Body"].read().decode("utf-8")
    print("[CHUNK{0}] Global bounds are {0} bytes".format(chunk, len(b)))
    m = json.loads(b)
    if not redis:
        return m
    i = time.time()
    print("[CHUNK{0}] S3 took {1} seconds...".format(chunk, i - start))
    print("[CHUNK{0}] Going to make {1} * 2 requests to Redis".format(chunk, len(m["max"])))
    original = []
    max_k = []
    min_k = []
    for idx in m["max"]:
        original.append(idx)
        min_k.append(str(idx) + "_min")
        max_k.append(str(idx) + "_max")
    print("Constructing lists took {0} seconds".format(time.time() - i))
    i = time.time()
    res = {}
    p1 = ParallelFn(redis_client.mget, max_k, None, res, "max")
    # max_v = redis_client.mget(max_k)
    # print("max_v took {0} for {1} keys".format(time.time() - i, len(max_k)))
    # i2 = time.time()
    # min_v = redis_client.mget(min_k)
    # print("min_v took {0} for {1} keys".format(time.time() - i2, len(min_k)))
    p2 = ParallelFn(redis_client.mget, min_k, None, res, "min")
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    max_v = res["max"]
    min_v = res["min"]
    print("[CHUNK{0}] Getting 2 * {1} elements from Redis took {2}...".format(chunk, len(m["max"]), time.time() - i))
    i = time.time()
    d = {"max":{},"min":{}}
    for idx, k in enumerate(original):
        d["max"][k] = max_v[idx]
        d["min"][k] = min_v[idx]
    print("[CHUNK{0}] Constructing the final dictionary took {1} seconds".format(chunk, time.time() - i))
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
                s = (v - min_v) / (max_v - min_v) * (new_max - new_min) + new_min
            r[j] = (idx, s)
    return data
