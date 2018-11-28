""" Unit normal normalization """

import json

import boto3
from cirrus.lambda_thread import LambdaThread
from cirrus.utils import get_all_keys, launch_threads, Timer,\
    get_redis_creds

MAX_LAMBDAS = 400
EPSILON = .0001

class LocalRange(LambdaThread):
    """ Get the mean and standard deviation for this chunk """
    def __init__(self, s3_key, s3_bucket_input, creds):
        LambdaThread.__init__(self)
        self.lamdba_dict = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "action": "LOCAL_RANGE",
            "normalization": "NORMAL",
            "redis_host": creds["host"],
            "redis_db": creds["db"],
            "redis_password": creds["password"],
            "redis_port": creds["port"]
        }


class LocalScale(LambdaThread):
    """ Subtract the global mean and divide by the standard
    deviation """
    def __init__(self, s3_key, s3_bucket_input, s3_bucket_output, creds):
        LambdaThread.__init__(self)
        self.lamdba_dict = {
            "s3_bucket_input": s3_bucket_input,
            "s3_key": s3_key,
            "s3_bucket_output": s3_bucket_output,
            "action": "LOCAL_SCALE",
            "normalization": "NORMAL",
            "redis_host": creds["host"],
            "redis_db": creds["db"],
            "redis_password": creds["password"],
            "redis_port": creds["port"]
        }


def normal_scaler(s3_bucket_input, s3_bucket_output, objects=(), dry_run=False):
    """ Scale the values in a dataset to fit a unit normal distribution. """
    if not objects:
        # Allow user to specify objects, or otherwise get all objects.
        objects = get_all_keys(s3_bucket_input)

    # Calculate bounds for each chunk.
    timer = Timer("NORMAL_SCALING").set_step("LocalRange")
    creds = get_redis_creds()
    launch_threads(LocalRange, objects, MAX_LAMBDAS, s3_bucket_input, creds)

    timer.timestamp().set_step("Creating the global map")

    client = boto3.client("s3")
    f_ranges = get_global_map(s3_bucket_input, objects, client)

    timer.timestamp().set_step("Putting local maps")

    s3_resource = boto3.resource("s3")
    update_local_maps(s3_bucket_input, objects, f_ranges, client,
                      s3_resource)

    timer.timestamp().set_step("Local scaling")
    if not dry_run:
        # Scale the chunks and put them in the output bucket.
        launch_threads(LocalScale, objects, MAX_LAMBDAS,
                       s3_bucket_input, s3_bucket_output, creds)

    timer.timestamp().set_step("Deleting local maps")

    # Delete any intermediary keys.
    for i in objects:
        s3_resource.Object(s3_bucket_input, str(i) + "_final_bounds").delete()

    timer.timestamp()


def get_global_map(s3_bucket_input, objects, client):
    """ Aggregate the sample means, std. devs., etc. to get the global map. """
    f_ranges = {}
    for i in objects:
        obj = client.get_object(Bucket=s3_bucket_input,
                                Key=str(i) + "_bounds")["Body"].read()
        local_map = json.loads(obj.decode("utf-8"))
        for idx in local_map:
            sample_x_squared, sample_x, n_samples = local_map[idx]
            if idx not in f_ranges:
                f_ranges[idx] = [
                    sample_x_squared,
                    sample_x,
                    n_samples]
            else:
                f_ranges[idx][0] += sample_x_squared
                f_ranges[idx][1] += sample_x
                f_ranges[idx][2] += n_samples
    return f_ranges


def update_local_maps(s3_bucket_input, objects, f_ranges,
                      s3_client, s3_resource):
    """ Update the local maps of means, std. devs., etc. """
    for i in objects:
        s3_obj = s3_resource.Object(s3_bucket_input, str(i) + "_bounds")
        obj = s3_obj.get()["Body"].read()
        local_map = json.loads(obj.decode("utf-8"))
        for idx in local_map:
            mean_x_sq = f_ranges[idx][0] / float(f_ranges[idx][2])
            mean = f_ranges[idx][1] / float(f_ranges[idx][2])
            try:
                diff = mean_x_sq - mean**2
                std_dev = 0
                if abs(diff) > EPSILON:
                    std_dev = (diff)**(.5)
                local_map[idx] = [std_dev, mean]
            except Exception as exc:
                print(exc)
                print("Index: {0}, E[X^2]: {1}, E[X]^2: {2}".format(idx,
                                                                    mean_x_sq,
                                                                    mean**2))
                raise exc
        serialized = json.dumps(local_map)
        s3_client.put_object(Bucket=s3_bucket_input,
                             Key=str(i) + "_final_bounds", Body=serialized)
        s3_obj.delete()
