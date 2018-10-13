""" AWS Lambda handler to be deployed by the deploy.sh script. """

import random
import time

from redis import StrictRedis

import boto3
import feature_hashing_helper
import min_max_helper
import normal_helper
import toml
from rediscluster import StrictRedisCluster
from rediscluster.nodemanager import NodeManager
from utils import get_data_from_s3, serialize_data

CLUSTER = False


def handler(event, context):
    """ First entry point for lambda function. """
    total = time.time()
    assert "s3_bucket_input" in event and "s3_key" in event, \
        "Must specify input bucket, and key."
    # Handle duplicate lambda launches
    unique_id = event["s3_key"] + "_nonce_" + str(event["dupe_nonce"])
    redis_flag = bool(int(event["use_redis"]))
    redis_client = None
    node_manager = None
    # Kill the function if this is a duplicate
    if redis_flag:
        signal = kill_duplicates(event["s3_key"], unique_id)
        if signal is None:
            return ["DUPLICATE"]
        redis_client, node_manager = signal

    print("[CHUNK{0}] Took {1} to determine if duplicate".format(
        event["s3_key"], time.time() - total))

    # Get data from S3
    print("[CHUNK{0}] Getting data from S3...".format(event["s3_key"]))
    get_data_time = time.time()
    s3_client = boto3.client("s3")
    data, labels = get_data_from_s3(
        s3_client, event["s3_bucket_input"], event["s3_key"], keep_label=True)
    print("[CHUNK{0}] Getting S3 data took {1}".format(
        event["s3_key"], time.time() - get_data_time))

    # Call the appropriate handler
    if event["action"] == "FEATURE_HASHING":
        feature_hashing_handler(s3_client, data, labels, event)
    elif event["normalization"] == "MIN_MAX":
        min_max_handler(s3_client, redis_client, data, labels,
                        node_manager, redis_flag, event)
    elif event["normalization"] == "NORMAL":
        normal_scaling_handler(s3_client, data, labels, event)
    print("[CHUNK{0}] Total time was {1}".format(
        event["s3_key"], time.time() - total))
    return []


def kill_duplicates(chunk, unique_id):
    """ Identify duplicates with Redis """
    print("[CHUNK{0}] Opening redis.toml".format(chunk))
    with open("redis.toml", "r") as f_handle:
        creds = toml.load(f_handle)
    print("[CHUNK{0}] Opened redis.toml".format(chunk))
    redis_host = creds["host"]
    redis_port = int(creds["port"])
    redis_db = int(creds["db"])
    redis_password = creds["password"]
    startup_nodes = [{"host": redis_host,
                      "port": redis_port, "password": redis_password}]
    if not CLUSTER:
        redis_client = StrictRedis(
            host=redis_host, port=redis_port, password=redis_password,
            db=redis_db)
    else:
        redis_client = StrictRedisCluster(
            startup_nodes=startup_nodes, decode_responses=True,
            skip_full_coverage_check=True)
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


def feature_hashing_handler(s3_client, data, labels, event):
    """ Handle a call for feature hashing """
    start = time.time()
    print("[CHUNK{0}] Hashing data".format(event["s3_key"]))
    hashed = feature_hashing_helper.hash_data(data,
                                              event["columns"],
                                              event["n_buckets"])
    print("[CHUNK{0}] Serializing data".format(event["s3_key"]))
    serialized = serialize_data(hashed, labels)
    print("[CHUNK{0}] Putting object in S3".format(event["s3_key"]))
    s3_client.put_object(
        Bucket=event["s3_bucket_output"], Key=event["s3_key"], Body=serialized)
    print("[CHUNK{0}] Process took {1}".format(
        event["s3_key"], time.time() - start))


def min_max_handler(s3_client, redis_client, data, labels,
                    node_manager, redis_flag, event):
    """ Either calculates the local bounds, or scales data and puts
    the new data in {src_object}_scaled. """
    start = time.time()
    if event["action"] == "LOCAL_BOUNDS":
        print("Getting local data bounds...")
        bounds = min_max_helper.get_data_bounds(data)
        print("[CHUNK{0}] Calculating bounds took {1}".format(
            event["s3_key"], time.time() - start))
        bounds_time = time.time()
        print("Putting bounds in S3...")
        min_max_helper.put_bounds_in_db(
            s3_client, redis_client, bounds,
            event["s3_bucket_input"], event["s3_key"] + "_bounds",
            redis_flag, node_manager, event["s3_key"])
        print(
            "[CHUNK{0}] Putting bounds in S3 / Redis took {1}".format(
                event["s3_key"], time.time() - bounds_time))
        print("[CHUNK{0}NONCE] LOCAL {1} GLOBAL {2} TIME {3}".format(
            event["s3_key"], (random.random() * 1000) // 1.0,
            event["dupe_nonce"], time.time() - start))
    elif event["action"] == "LOCAL_SCALE":
        assert "s3_bucket_output" in event, "Must specify output bucket."
        assert "min_v" in event, "Must specify min."
        assert "max_v" in event, "Must specify max."
        print("Getting global bounds...")
        bounds = min_max_helper.get_global_bounds(
            s3_client, redis_client, event["s3_bucket_input"], event["s3_key"],
            redis_flag, event["s3_key"])
        print("[CHUNK{0}] Global bounds took {1} to get".format(
            event["s3_key"], time.time() - start))
        scale_time = time.time()
        print("Scaling data...")
        scaled = min_max_helper.scale_data(data, bounds,
                                           event["min_v"], event["max_v"])
        print("[CHUNK{0}] Scaling took {1}".format(
            event["s3_key"], time.time() - scale_time))
        print("Serializing...")
        serialized = serialize_data(scaled, labels)
        print("[CHUNK{0}] Putting in S3...".format(event["s3_key"]))
        s3_client.put_object(
            Bucket=event["s3_bucket_output"],
            Key=event["s3_key"], Body=serialized)


def normal_scaling_handler(s3_client, data, labels, event):
    """ Scale to a unit normal range """
    if event["action"] == "LOCAL_RANGE":
        print("Getting local data ranges...")
        bounds = normal_helper.get_data_ranges(data)
        print("Putting ranges in S3...")
        min_max_helper.put_bounds_in_db(
            s3_client, None, bounds, event["s3_bucket_input"],
            event["s3_key"] + "_bounds",
            False, None, event["s3_key"])
    elif event["action"] == "LOCAL_SCALE":
        assert "s3_bucket_output" in event, "Must specify output bucket."
        print("Getting global bounds...")
        bounds = min_max_helper.get_global_bounds(
            s3_client, None, event["s3_bucket_input"], event["s3_key"],
            False, event["s3_key"])
        print("Scaling data...")
        scaled = normal_helper.scale_data(data, bounds)
        print("Serializing...")
        serialized = serialize_data(scaled, labels)
        print("Putting in S3...")
        s3_client.put_object(
            Bucket=event["s3_bucket_output"],
            Key=event["s3_key"], Body=serialized)
