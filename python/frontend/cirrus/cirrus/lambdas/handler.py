import boto3
from redis import StrictRedis
from rediscluster import StrictRedisCluster
from rediscluster.nodemanager import NodeManager
import toml
import minmaxhandler
import normalhandler
import featurehashinghandler
from utils import *
import time
import random

cluster = False


def handler(event, context):
    total = time.time()
    assert "s3_bucket_input" in event and "s3_key" in event, "Must specify input bucket, and key."
    # Handle duplicate lambda launches
    unique_id = event["s3_key"] + "_nonce_" + str(event["dupe_nonce"])
    r = bool(int(event["use_redis"]))
    redis_client = None
    node_manager = None
    # Kill the function if this is a duplicate
    if r:
        signal = kill_duplicates(event["s3_key"], unique_id)
        if signal is None:
            return ["DUPLICATE"]
        redis_client, node_manager = signal

    print("[CHUNK{0}] Took {1} to determine if duplicate".format(
        event["s3_key"], time.time() - total))

    # Get data from S3
    print("[CHUNK{0}] Getting data from S3...".format(event["s3_key"]))
    t = time.time()
    s3_client = boto3.client("s3")
    d, l = get_data_from_s3(
        s3_client, event["s3_bucket_input"], event["s3_key"], keep_label=True)
    print("[CHUNK{0}] Getting S3 data took {1}".format(
        event["s3_key"], time.time() - t))

    # Call the appropriate handler
    if event["action"] == "FEATURE_HASHING":
        feature_hashing_handler(s3_client, d, l, event)
    elif event["normalization"] == "MIN_MAX":
        min_max_handler(s3_client, redis_client, d, l, node_manager, r, event)
    elif event["normalization"] == "NORMAL":
        normal_scaling_handler(s3_client, d, l, event)
    print("[CHUNK{0}] Total time was {1}".format(event["s3_key"], time.time() - total))
    return []

def kill_duplicates(chunk, unique_id):
    print("[CHUNK{0}] Opening redis.toml".format(chunk))
    with open("redis.toml", "r") as f:
        creds = toml.load(f)
    print("[CHUNK{0}] Opened redis.toml".format(chunk))
    redis_host = creds["host"]
    redis_port = int(creds["port"])
    redis_db = int(creds["db"])
    redis_password = creds["password"]
    startup_nodes = [{"host": redis_host,
                      "port": redis_port, "password": redis_password}]
    if not cluster:
        redis_client = StrictRedis(
            host=redis_host, port=redis_port, password=redis_password, db=redis_db)
    else:
        redis_client = StrictRedisCluster(
            startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)
    print("[CHUNK{0}] Initialized Redis client".format(chunk))
    node_manager = NodeManager(startup_nodes)
    print("[CHUNK{0}] Created NodeManager".format(chunk))
    k_signal = redis_client.getset(unique_id, "Y")
    print("[CHUNK{0}] Checked if lambda already launched".format(
        chunk))
    if k_signal == "Y":
        print(
            "[CHUNK{0}] Found duplicate - killing.".format(chunk))
        return None

    return redis_client, node_manager


def feature_hashing_handler(s3_client, d, l, event):
    # Handle a call for feature hashing
    t = time.time()
    print("[CHUNK{0}] Hashing data".format(event["s3_key"]))
    h = featurehashinghandler.hash_data(d, event["columns"], event["N"])
    print("[CHUNK{0}] Serializing data".format(event["s3_key"]))
    serialized = serialize_data(h, l)
    print("[CHUNK{0}] Putting object in S3".format(event["s3_key"]))
    s3_client.put_object(
        Bucket=event["s3_bucket_output"], Key=event["s3_key"], Body=serialized)
    print("[CHUNK{0}] Process took {1}".format(
        event["s3_key"], time.time() - t))


def min_max_handler(s3_client, redis_client, d, l, node_manager, r, event):
    # Either calculates the local bounds, or scales data and puts the new data in
    # {src_object}_scaled.
    t = time.time()
    if event["action"] == "LOCAL_BOUNDS":
        print("Getting local data bounds...")
        b = minmaxhandler.get_data_bounds(d)
        print("[CHUNK{0}] Calculating bounds took {1}".format(
            event["s3_key"], time.time() - t))
        t = time.time()
        print("Putting bounds in S3...")
        minmaxhandler.put_bounds_in_db(
            s3_client, redis_client, b, event["s3_bucket_input"], event["s3_key"] + "_bounds", r, node_manager, event["s3_key"])
        print(
            "[CHUNK{0}] Putting bounds in S3 / Redis took {1}".format(event["s3_key"], time.time() - t))
        print("[CHUNK{0}NONCE] LOCAL {1} GLOBAL {2} TIME {3}".format(
            event["s3_key"], (random.random() * 1000) // 1.0, event["dupe_nonce"], time.time() - t))
    elif event["action"] == "LOCAL_SCALE":
        assert "s3_bucket_output" in event, "Must specify output bucket."
        assert "min_v" in event, "Must specify min."
        assert "max_v" in event, "Must specify max."
        t = time.time()
        print("Getting global bounds...")
        b = minmaxhandler.get_global_bounds(
            s3_client, redis_client, event["s3_bucket_input"], event["s3_key"], r, event["s3_key"])
        print("[CHUNK{0}] Global bounds took {1} to get".format(
            event["s3_key"], time.time() - t))
        t = time.time()
        print("Scaling data...")
        scaled = minmaxhandler.scale_data(d, b, event["min_v"], event["max_v"])
        print("[CHUNK{0}] Scaling took {1}".format(
            event["s3_key"], time.time() - t))
        print("Serializing...")
        serialized = serialize_data(scaled, l)
        print("[CHUNK{0}] Putting in S3...".format(event["s3_key"]))
        s3_client.put_object(
            Bucket=event["s3_bucket_output"], Key=event["s3_key"], Body=serialized)


def normal_scaling_handler(s3_client, d, l, event):
    # Scale to a unit normal range
    t = time.time()
    if event["action"] == "LOCAL_RANGE":
        print("Getting local data ranges...")
        b = normalhandler.get_data_ranges(d)
        print("Putting ranges in S3...")
        minmaxhandler.put_bounds_in_s3(
            client, b, event["s3_bucket_input"], event["s3_key"] + "_bounds")
    elif event["action"] == "LOCAL_SCALE":
        assert "s3_bucket_output" in event, "Must specify output bucket."
        print("Getting global bounds...")
        b = minmaxhandler.get_global_bounds(
            client, event["s3_bucket_input"], event["s3_key"])
        print("Scaling data...")
        scaled = normalhandler.scale_data(d, b)
        print("Serializing...")
        serialized = serialize_data(scaled, l)
        print("Putting in S3...")
        s3_client.put_object(
            Bucket=event["s3_bucket_output"], Key=event["s3_key"], Body=serialized)
