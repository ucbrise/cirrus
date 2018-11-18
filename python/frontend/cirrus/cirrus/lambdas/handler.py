""" AWS Lambda handler to be deployed by the deploy.sh script. """

import boto3
from redis import StrictRedis
from rediscluster import StrictRedisCluster
from rediscluster.nodemanager import NodeManager

import feature_hashing_helper
import lambda_utils
import min_max_helper
import normal_helper
from utils import get_data_from_s3, serialize_data, Timer, prefix_print

CLUSTER = False

def handler(event, context):
    """ First entry point for lambda function. """
    timer = Timer("CHUNK{0}".format(event["s3_key"])).set_step(
        "Determining if duplicate")
    assert "s3_bucket_input" in event, "Must specify input bucket."
    # Handle duplicate lambda launches
    redis_flag = True
    if "use_redis" in event:
        redis_flag = bool(int(event["use_redis"]))
    redis_client = None
    node_manager = None
    # Kill the function if this is a duplicate
    if redis_flag:
        unique_id = event["s3_key"] + "_nonce_" + str(event["dupe_nonce"])
        signal = kill_duplicates(event["s3_key"], unique_id,
                                 event["redis_host"], event["redis_port"],
                                 event["redis_db"], event["redis_password"])
        if signal is None:
            return ["DUPLICATE"]
        redis_client, node_manager = signal

    timer.timestamp().set_step("Getting S3 data")

    # Get data from S3
    s3_client = boto3.client("s3")
    data, labels = get_data_from_s3(
        s3_client, event["s3_bucket_input"], event["s3_key"], keep_label=True)
    timer.timestamp()

    # Call the appropriate handler
    if event["action"] == "FEATURE_HASHING":
        feature_hashing_handler(s3_client, data, labels, event)
    elif event["normalization"] == "MIN_MAX":
        min_max_handler(s3_client, redis_client, data, labels,
                        node_manager, event)
    elif event["normalization"] == "NORMAL":
        normal_scaling_handler(s3_client, data, labels, event)
    timer.global_timestamp()
    return []


def kill_duplicates(chunk, unique_id, host, port, db, password):
    """ Identify duplicates with Redis """
    printer = prefix_print("CHUNK{0}".format(chunk))
    redis_host = host
    redis_port = int(port)
    redis_db = int(db)
    redis_password = password
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
    printer("Initialized Redis client")
    node_manager = NodeManager(startup_nodes)
    printer("Created NodeManager")
    k_signal = redis_client.getset(unique_id, "Y")
    printer("Checked if lambda already launched")
    if k_signal == "Y":
        printer("Found duplicate - killing.")
        return None

    return redis_client, node_manager


def feature_hashing_handler(s3_client, data, labels, event):
    """ Handle a call for feature hashing """
    timer = Timer("CHUNK{0}".format(event["s3_key"])).set_step(
        "Feature hashing")
    printer = prefix_print("CHUNK{0}".format(event["s3_key"]))
    printer("Hashing data")
    hashed = feature_hashing_helper.hash_data(data,
                                              event["columns"],
                                              event["n_buckets"])
    printer("Serializing data")
    serialized = serialize_data(hashed, labels)
    printer("Putting object in S3")
    s3_client.put_object(
        Bucket=event["s3_bucket_output"], Key=event["s3_key"], Body=serialized)
    timer.timestamp()


def min_max_handler(s3_client, redis_client, data, labels,
                    node_manager, event):
    """ Either calculates the local bounds, or scales data and puts
    the new data in {src_object}_scaled. """
    timer = Timer("CHUNK{0}".format(event["s3_key"]))
    if event["action"] == "LOCAL_BOUNDS":
        print("Getting local data bounds...")
        timer.set_step("Calculating bounds")
        bounds = min_max_helper.get_data_bounds(data)
        timer.timestamp().set_step("Putting bounds in S3 / Redis")
        print("Putting bounds in S3...")
        min_max_helper.put_bounds_in_db(
            s3_client, redis_client, bounds,
            event["s3_bucket_input"], event["s3_key"],
            node_manager, event["s3_key"])
        timer.timestamp()
    elif event["action"] == "LOCAL_SCALE":
        assert "s3_bucket_output" in event, "Must specify output bucket."
        assert "min_v" in event, "Must specify min."
        assert "max_v" in event, "Must specify max."
        print("Getting global bounds...")
        timer.set_step("Getting global bounds")
        bounds = min_max_helper.get_global_bounds(
            s3_client, redis_client, event["s3_bucket_input"], event["s3_key"],
            event["s3_key"])
        timer.timestamp().set_step("Scaling data")
        print("Scaling data...")
        scaled = min_max_helper.scale_data(data, bounds,
                                           event["min_v"], event["max_v"])
        timer.timestamp().set_step("Serializing")
        print("Serializing...")
        serialized = serialize_data(scaled, labels)
        timer.timestamp().set_step("Putting in S3")
        s3_client.put_object(
            Bucket=event["s3_bucket_output"],
            Key=event["s3_key"], Body=serialized)
        timer.timestamp()


def normal_scaling_handler(s3_client, data, labels, event):
    """ Scale to a unit normal range """
    if event["action"] == "LOCAL_RANGE":
        print("Getting local data ranges...")
        bounds = normal_helper.get_data_ranges(data)
        print("Putting ranges in S3...")
        lambda_utils.put_dict_in_s3(
            s3_client, bounds, event["s3_bucket_input"],
            event["s3_key"] + "_bounds")
    elif event["action"] == "LOCAL_SCALE":
        assert "s3_bucket_output" in event, "Must specify output bucket."
        print("Getting global bounds...")
        bounds = lambda_utils.get_dict_from_s3(
            s3_client, event["s3_bucket_input"],
            event["s3_key"] + "_final_bounds", event["s3_key"])
        print("Scaling data...")
        scaled = normal_helper.scale_data(data, bounds)
        print("Serializing...")
        serialized = serialize_data(scaled, labels)
        print("Putting in S3...")
        s3_client.put_object(
            Bucket=event["s3_bucket_output"],
            Key=event["s3_key"], Body=serialized)
