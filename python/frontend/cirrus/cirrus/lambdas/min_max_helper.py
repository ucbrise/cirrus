""" Helper functions for min max scaling, including
Redis functions, getting and putting bounds in S3. """

import json
import time
from collections import deque
from threading import Thread

UPPER_BOUND_SCRIPT = "for i, v in ipairs(KEYS)" \
    " do local current = tonumber(redis.call('get', KEYS[i])); " \
    "if current then " \
    "if tonumber(ARGV[i]) > current then " \
    "redis.call('set', KEYS[i], ARGV[i]) end " \
    "else redis.call('set', KEYS[i], ARGV[i]) end " \
    "end"

LOWER_BOUND_SCRIPT = "for i, v in ipairs(KEYS)" \
    " do local current = tonumber(redis.call('get', KEYS[i])); " \
    "if current then " \
    "if tonumber(ARGV[i]) < current then " \
    "redis.call('set', KEYS[i], ARGV[i]) end " \
    "else redis.call('set', KEYS[i], ARGV[i]) end " \
    "end"


def put_bounds_in_db(s3_client, redis_client, bounds, dest_bucket,
                     dest_object, use_redis, node_manager, chunk,
                     batch_push_to_redis=True):
    """ Add the dictionary of bounds to an S3 bucket or Redis instance. """
    serialized = json.dumps(bounds)
    s3_client.put_object(Bucket=dest_bucket, Key=dest_object,
                         Body=serialized)
    if not use_redis:
        # Stop here and let the master thread aggregate if not using Redis
        return

    upper_bound_func = redis_client.register_script(UPPER_BOUND_SCRIPT)
    lower_bound_func = redis_client.register_script(LOWER_BOUND_SCRIPT)

    start = time.time()
    max_k, max_v, min_k, min_v = get_keys_values(bounds)
    print("[CHUNK{0}] Took {1} to make lists".format(chunk,
                                                     time.time() - start))

    start = time.time()
    push_keys_values_to_redis(
        node_manager, chunk, batch_push_to_redis, max_k, max_v,
        upper_bound_func)
    print("[CHUNK{0}] upper_bound_func took {1}".format(
        chunk, time.time() - start))

    start = time.time()
    push_keys_values_to_redis(
        node_manager, chunk, batch_push_to_redis, min_k, min_v,
        lower_bound_func)
    print("[CHUNK{0}] lower_bound_func took {1}".format(
        chunk, time.time() - start))


class ParallelFn(Thread):
    """ Run a function on a particular key value pair. """

    def __init__(self, fn, key, val=None, res=None, res_key=None):
        Thread.__init__(self)
        self.fn = fn
        self.key = key
        self.val = val
        self.res = res
        self.res_key = res_key

    def run(self):
        # If there is no value, just run it on the key
        if self.val is None:
            ret_val = self.fn(self.key)
        else:
            ret_val = self.fn(self.key, self.val)
        # If the invoker specified to store the response in a dictionary,
        # put it in
        if self.res is not None:
            self.res[self.res_key] = ret_val


def get_data_bounds(data):
    """ Return a dict of two lists, containing max and min for each column.
    Assumes labels are being stored right now. """
    max_in_col = {}
    min_in_col = {}
    for row in data:
        for idx, val in row:
            if idx not in max_in_col:
                max_in_col[idx] = val
            if idx not in min_in_col:
                min_in_col[idx] = val
            if val > max_in_col[idx]:
                max_in_col[idx] = val
            if val < min_in_col[idx]:
                min_in_col[idx] = val
    return {
        "max": max_in_col,
        "min": min_in_col
    }


def get_keys_values(bounds):
    """ Get lists of keys and values to push to Redis. """
    max_k = []
    max_v = []
    min_k = []
    min_v = []
    # Add the maxima and minima for each key to a list
    for idx in bounds["max"]:
        # The maximum value for idx is at idx_max
        max_k.append(str(idx) + "_max")
        max_v.append(bounds["max"][idx])
        min_k.append(str(idx) + "_min")
        min_v.append(bounds["min"][idx])
    return max_k, max_v, min_k, min_v


def push_keys_values_to_redis(node_manager, chunk, batch_push_to_redis,
                              keys, values, redis_script):
    """ Apply a Redis script to a list of Redis keys and values """
    slot_k = {}
    slot_vals = {}
    if not batch_push_to_redis:
        # Push each key value pair one at a time
        for idx, k in enumerate(keys):
            redis_script([k], [values[idx]])
        return
    if node_manager is None:
        # If no node manager is specified, just push all at once
        redis_script(keys, values)
        return
    # Separate the keys and values into slots determined by Redis
    start = time.time()
    for idx, k in enumerate(keys):
        slot = node_manager.keyslot(k)
        if slot not in slot_k:
            slot_k[slot] = []
            slot_vals[slot] = []
        slot_k[slot].append(k)
        slot_vals[slot].append(values[idx])
    print("[CHUNK{0}] Took {1} to make slot maps".format(
        chunk, time.time() - start))
    start = time.time()
    push_threads = deque()
    # Push the key / value batches in parallel
    for idx, k in enumerate(slot_k):
        if len(push_threads) >= 4:
            other = push_threads.popleft()
            other.join()
        thread = ParallelFn(redis_script, slot_k[k], slot_vals[k])
        thread.start()
        push_threads.append(thread)
    for thread in push_threads:
        thread.join()
    print("[CHUNK{0}] Took {1} to make {2} key / value requests".format(
        chunk, time.time() - start, len(slot_k)))


def get_global_bounds(s3_client, redis_client, bucket, src_object,
                      use_redis, chunk):
    """ Get the bounds across all objects, where each key is mapped
    to [min, max]. """
    start = time.time()
    # Determine whether to get the aggregated global bounds from the
    # master thread, or the bounds from before and update (if using
    # Redis)
    suffix = "_final_bounds"
    if use_redis:
        suffix = "_bounds"
    b_data = s3_client.get_object(
        Bucket=bucket, Key=src_object + suffix)["Body"].read().decode("utf-8")
    print("[CHUNK{0}] Global bounds are {1} bytes".format(chunk, len(b_data)))
    bounds = json.loads(b_data)
    if not use_redis:
        return bounds
    construct_time = time.time()
    print("[CHUNK{0}] S3 took {1} seconds...".format(chunk,
                                                     construct_time - start))
    print(
        "[CHUNK{0}] Going to make {1} * 2 " + \
        "requests to Redis".format(chunk, len(bounds["max"])))
    # Get lists of the keys to get from Redis
    original = []
    max_k = []
    min_k = []
    for idx in bounds["max"]:
        original.append(idx)
        min_k.append(str(idx) + "_min")
        max_k.append(str(idx) + "_max")
    print("Constructing lists took {0} seconds".format(
        time.time() - construct_time))
    get_keys_time = time.time()
    res = {}
    # Get the keys in parallel
    thread1 = ParallelFn(redis_client.mget, max_k, None, res, "max")
    thread2 = ParallelFn(redis_client.mget, min_k, None, res, "min")
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    print("[CHUNK{0}] Getting 2 * {1} elements from Redis took {2}...".format(
        chunk, len(bounds["max"]), time.time() - get_keys_time))
    # Format the map the way we want it
    max_v = res["max"]
    min_v = res["min"]
    final_dict_time = time.time()
    final_bounds = {"max": {}, "min": {}}
    for idx, k in enumerate(original):
        final_bounds["max"][k] = max_v[idx]
        final_bounds["min"][k] = min_v[idx]
    print("[CHUNK{0}] Constructing the final dictionary took" + \
          " {1} seconds".format(chunk, time.time() - final_dict_time))
    return final_bounds


def scale_data(data, global_bounds, new_min, new_max):
    """ Scale the values in data based on the minima / maxima specified in g,
    the global map of mins / maxes. """
    for row in data:
        for j, tup_val in enumerate(row):
            idx_t, val = tup_val
            idx = str(idx_t)
            scaled = (new_min + new_max) / 2.0
            min_v = float(global_bounds["min"][idx])
            max_v = float(global_bounds["max"][idx])
            if min_v != max_v:
                scaled = (val - min_v) / (max_v - min_v) * \
                    (new_max - new_min) + new_min
            row[j] = (idx, scaled)
    return data
