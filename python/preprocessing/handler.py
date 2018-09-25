import boto3
import redis
import MinMaxHandler
import NormalHandler
from serialization import *

def handler(event, context):
    client = boto3.client("s3")
    s3_client = client
    assert "s3_bucket_input" in event and "s3_key" in event and "redis" in event, "Must specify if Redis is used, input bucket, and key."
    print("Getting data from S3...")
    d, l = get_data_from_s3(client, event["s3_bucket_input"], event["s3_key"], keep_label=True)
    r = False
    if event["redis"] == "1":
        r = True
        redis_host = "neel-redis.lpfp73.0001.usw2.cache.amazonaws.com"
        redis_port = 6379
        redis_password = ""
        client = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
    if event["normalization"] == "MIN_MAX":
        # Either calculates the local bounds, or scales data and puts the new data in
        # {src_object}_scaled.
        if event["action"] == "LOCAL_BOUNDS":
            print("Getting local data bounds...")
            b = MinMaxHandler.get_data_bounds(d)
            print("Putting bounds in S3...")
            MinMaxHandler.put_bounds_in_db(client, b, event["s3_bucket_input"], event["s3_key"] + "_bounds", r)
        elif event["action"] == "LOCAL_SCALE":
            assert "s3_bucket_output" in event, "Must specify output bucket."
            print("Getting global bounds...")
            b = MinMaxHandler.get_global_bounds(client, event["s3_bucket_input"], event["s3_key"], r)
            print("Scaling data...")
            scaled = MinMaxHandler.scale_data(d, b, event["min_v"], event["max_v"])
            print("Serializing...")
            serialized = serialize_data(scaled, l)
            print("Putting in S3...")
            s3_client.put_object(Bucket=event["s3_bucket_output"], Key=event["s3_key"], Body=serialized)
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
    return []
