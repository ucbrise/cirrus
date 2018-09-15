import boto3
import json

def get_data_bounds(data):
    # Return a dict of two lists, containing max and min for each column.
    # Assumes labels are being stored right now.
    max_in_col = [float("-inf") for j in data[0]]
    min_in_col = [float("inf") for j in data[0]]
    for r in data:
        for idx, v in enumerate(r):
            if v > max_in_col[idx]:
                max_in_col[idx] = v
            if v < min_in_col[idx]:
                min_in_col[idx] = v
    return {
        "max": max_in_col,
        "min": min_in_col
    }

def get_data_from_s3(client, src_bucket, src_object):
    # Return a 2D list, where each element is a row of the dataset.
    obj = client.get_object(Bucket=src_bucket, Key=src_object)["Body"].read()
    data = []
    for idx, l in enumerate(obj.splitlines()):
        if idx == 0 or idx == 1:
            continue
        # TODO: Interpret the bytes 4... of the string as a float array.
    return data

def put_bounds_in_s3(client, bounds, dest_bucket, dest_object):
    # Add the dictionary of bounds to an S3 bucket. 
    s = json.dumps(bounds)
    client.put_object(Bucket=dest_bucket, Key=des_object, Body=s)

def handler(event, context):
    print(event["src_bucket"], event["src_object"])
    
    client = boto3.client("s3")
    d = get_data_from_s3(client, event["src_bucket"], event["src_object"])
    b = get_data_bounds(d)
    put_bounds_in_s3(client, b, event["src_bucket"], event["src_object"] + "_bounds")

    return []
