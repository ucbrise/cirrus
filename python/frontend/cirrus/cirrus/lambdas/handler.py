import boto3
from redis import StrictRedis
from rediscluster import StrictRedisCluster
from rediscluster.nodemanager import NodeManager
import toml
import MinMaxHandler
import NormalHandler
from serialization import *
import time
import random

cluster = False

def handler(event, context):
    total = time.time()
    unique_id = str(event["invocation"]) + "_invocation_" + event["s3_key"] + "_nonce_" + str(event["nonce"])
    s3_client = boto3.client("s3")
    assert "s3_bucket_input" in event and "s3_key" in event and "redis" in event, "Must specify if Redis is used, input bucket, and key."
    r = False
    redis_client = None
    node_manager = None
    if event["redis"] == "1":
        r = True
        print("[CHUNK{0}] Opening redis.toml".format(event["s3_key"]))
        with open("redis.toml", "r") as f:
            creds = toml.load(f)
        print("[CHUNK{0}] Opened redis.toml".format(event["s3_key"]))
        redis_host = creds["host"]
        redis_port = int(creds["port"])
        redis_password = creds["password"]
        startup_nodes = [{"host": redis_host, "port": redis_port, "password": redis_password}]
        if not cluster:
            redis_client = StrictRedis(host=redis_host, port=redis_port, password=redis_password)
        else:
            redis_client = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)
        print("[CHUNK{0}] Initialized Redis client".format(event["s3_key"]))
        node_manager = NodeManager(startup_nodes)
        print("[CHUNK{0}] Created NodeManager".format(event["s3_key"]))
        k_signal = redis_client.getset(unique_id, "Y")
        print("[CHUNK{0}] Checked if lambda already launched".format(event["s3_key"]))
        if k_signal == "Y":
            print("[CHUNK{0}] Found duplicate - killing.".format(event["s3_key"]))
            return
    print("[CHUNK{0}] Took {1} to get past Redis".format(event["s3_key"], time.time() - total))
    print("[CHUNK{0}] Getting data from S3...".format(event["s3_key"]))
    t = time.time()
    d, l = get_data_from_s3(s3_client, event["s3_bucket_input"], event["s3_key"], keep_label=True)
    print("[CHUNK{0}] Getting S3 data took {1}".format(event["s3_key"], time.time() - t))
    t = time.time()
    if event["normalization"] == "MIN_MAX":
        # Either calculates the local bounds, or scales data and puts the new data in
        # {src_object}_scaled.
        if event["action"] == "LOCAL_BOUNDS":
            print("Getting local data bounds...")
            b = MinMaxHandler.get_data_bounds(d)
            print("[CHUNK{0}] Calculating bounds took {1}".format(event["s3_key"], time.time() - t))
            t = time.time()
            print("Putting bounds in S3...")
            MinMaxHandler.put_bounds_in_db(s3_client, redis_client, b, event["s3_bucket_input"], event["s3_key"] + "_bounds", r, node_manager, event["s3_key"])
            print("[CHUNK{0}] Putting bounds in S3 / Redis took {1}".format(event["s3_key"], time.time() - t))
            print("[CHUNK{0}] Total time was {1}".format(event["s3_key"], time.time() - total))
            print("[CHUNK{0}NONCE] LOCAL {1} GLOBAL {2} TIME {3}".format(event["s3_key"], (random.random() * 1000) // 1.0, event["nonce"], time.time() - total))
        elif event["action"] == "LOCAL_SCALE":
            assert "s3_bucket_output" in event, "Must specify output bucket."
            assert "min_v" in event, "Must specify min."
            assert "max_v" in event, "Must specify max."
            print("Getting global bounds...")
            b = MinMaxHandler.get_global_bounds(s3_client, redis_client, event["s3_bucket_input"], event["s3_key"], r, event["s3_key"])
            print("[CHUNK{0}] Global bounds took {1} to get".format(event["s3_key"], time.time() - t))
            t = time.time()
            print("Scaling data...")
            scaled = MinMaxHandler.scale_data(d, b, event["min_v"], event["max_v"])
            print("[CHUNK{0}] Scaling took {1}".format(event["s3_key"], time.time() - t))
            print("Serializing...")
            serialized = serialize_data(scaled, l)
            print("Putting in S3...")
            s3_client.put_object(Bucket=event["s3_bucket_output"], Key=event["s3_key"], Body=serialized)
            print("[CHUNK{0}] Total time was {1}".format(event["s3_key"], time.time() - total))
    elif event["normalization"] == "NORMAL":
        if event["action"] == "LOCAL_RANGE":
            print("Getting local data ranges...")
            b = NormalHandler.get_data_ranges(d)
            print("Putting ranges in S3...")
            MinMaxHandler.put_bounds_in_s3(client, b, event["s3_bucket_input"], event["s3_key"] + "_bounds")
        elif event["action"] == "LOCAL_SCALE":
            assert "s3_bucket_output" in event, "Must specify output bucket."
            print("Getting global bounds...")
            b = MinMaxHandler.get_global_bounds(client, event["s3_bucket_input"], event["s3_key"])
            print("Scaling data...")
            scaled = NormalHandler.scale_data(d, b)
            print("Serializing...")
            serialized = serialize_data(scaled, l)
            print("Putting in S3...")
            s3_client.put_object(Bucket=event["s3_bucket_output"], Key=event["s3_key"], Body=serialized)
    # if r:
    #     # Unregister lambda.
    #     redis_client.set(unique_id, "")
    return []
