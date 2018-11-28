""" Useful functions for lambdas. """

import json
from utils import Timer

def put_dict_in_s3(s3_client, bounds, dest_bucket,
                   dest_object):
    """ Put a dictionary in S3. """
    serialized = json.dumps(bounds)

    s3_client.put_object(Bucket=dest_bucket, Key=dest_object,
                         Body=serialized)

def get_dict_from_s3(s3_client, s3_bucket, s3_object, chunk):
    """ Get a dict from S3. """
    timer = Timer("CHUNK{0}".format(chunk)).set_step("S3")
    b_data = s3_client.get_object(
        Bucket=s3_bucket, Key=s3_object)["Body"].read().decode("utf-8")
    print("[CHUNK{0}] Dict is {1} bytes".format(chunk, len(b_data)))
    timer.timestamp()
    return json.loads(b_data)
