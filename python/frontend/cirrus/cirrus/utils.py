""" Utility functions for Cirrus """

import random
import struct
import time
from collections import deque

import boto3
from redis import StrictRedis
import toml

DEFAULT_LABEL = struct.pack("i", 0)
REDIS_TOML = "redis.toml"
ec2c = boto3.client('ec2')
lc = boto3.client('lambda')
iam_client = boto3.client('iam')

class Timer(object):
    """ A class to time functions. """

    DEFAULT_STEP = "Function"

    def __init__(self, tag="", verbose=False):
        """ Set an optional tag at the front of this timer's
        print statements. """
        self.global_time = time.time()
        self.last_time = time.time()
        self.step = Timer.DEFAULT_STEP
        self.tag = tag
        self.verbose = verbose
        self.__print__ = prefix_print(tag)
        if tag:
            self.__print__("Started timing {0}".format(self.tag))

    def set_step(self, name):
        """ Time the current step """
        self.step = name
        self.last_time = time.time()
        if self.verbose:
            self.__print__(name)
        return self

    def global_timestamp(self):
        """ Get the time elapsed since the timer was created. """
        self.__print__("Global time: {0} seconds"
                       .format(time.time() - self.global_time))
        return self

    def timestamp(self):
        """ Get the time for the current step """
        self.__print__("{0} took {1} seconds"
                       .format(self.step, time.time() - self.last_time))
        return self


def prefix_print(prefix):
    """ Get a function that prints with a prefix. """
    def printer(statement):
        """ Print with a prefix """
        tag = ""
        if prefix:
            tag = "[{0}] ".format(prefix)
        print("{0}{1}".format(tag, statement))
    return printer


def get_redis_creds():
    """ Get Redis credentials from TOML file. """
    with open("redis.toml", "r") as f_handle:
        creds = toml.load(f_handle)
    return {
        "host": creds["host"],
        "port": int(creds["port"]),
        "db": int(creds["db"]),
        "password": creds["password"]
    }


def wipe_redis():
    """ Wipe all keys from Redis """
    creds = get_redis_creds()
    redis_client = StrictRedis(
        host=creds["host"], port=creds["port"], password=creds["password"],
        db=creds["db"])
    redis_client.flushdb()


def launch_threads(lambda_cls, objects, max_lambdas=400, *params):
    """ Launch one thread for each of the objects passed in, with the
    specified parameters. """
    threads = deque()
    for i in objects:
        while len(threads) > max_lambdas:
            other = threads.popleft()
            other.join()
        thread = lambda_cls(i, *params)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


def retry_loop(func, exceptions=(), handle_exception=None, max_attempts=3,
               name="Function"):
    """ Retry a function however many times, but stop if
    one of the specified exceptions occurs. """
    curr_attempt = 1
    timer = Timer(name).set_step("Attempt #1")
    while curr_attempt <= max_attempts:
        try:
            func()
            timer.timestamp().global_timestamp()
            break
        except exceptions as exc:
            if handle_exception is not None:
                handle_exception(exc)
            curr_attempt += 1
        except Exception as exc:
            curr_attempt += 1
            if curr_attempt > max_attempts:
                raise exc
        print("{0}: Launching attempt #{1}".format(name, curr_attempt))
        timer.set_step("Attempt #{0}".format(curr_attempt))


def delete_all_keys(bucket):
    """ Delete all keys from an S3 bucket """
    return get_all_keys(bucket, "")


def get_all_keys(bucket, contains="_"):
    """ Get all keys from an S3 bucket, deleting any key that has
    the substring "contains" """
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    keys = []
    kwargs = {"Bucket": bucket}
    # Get all keys
    while True:
        result = s3_client.list_objects_v2(**kwargs)
        if "Contents" not in result:
            break
        for obj in result["Contents"]:
            keys.append(obj["Key"])
        try:
            kwargs["ContinuationToken"] = result["NextContinuationToken"]
        except KeyError:
            break

    print("Found {0} chunks...".format(len(keys)))
    # Delete the objects with keys that have the substring "contains"
    final_objects = []
    for obj in keys:
        if contains in obj:
            s3_resource.Object(bucket, obj).delete()
        else:
            final_objects.append(obj)
    print("Chunks after pruning: {0}".format(len(final_objects)))
    return final_objects


def get_data_from_s3(client, src_bucket, src_object, keep_label=False):
    """ Return a 2D list, where each element is a row of the dataset. """
    b_data = client.get_object(Bucket=src_bucket, Key=src_object)["Body"].read()
    data = []
    labels = []
    current_line = []
    idx = None
    label_bytes = None
    num_values = None
    seen = 0
    for i in range(8, len(b_data), 4):
        # Ignore the first 8 bytes, and get each 4 byte chunk one at a time
        if label_bytes is None:
            # If we haven't gotten the label for this line
            label_bytes = b_data[i:i + 4]
            if keep_label:
                labels.append(label_bytes)
            continue
        if num_values is None:
            # If we haven't gotten the number of values for this line
            num_values = struct.unpack("i", b_data[i:i + 4])[0]
            continue
        if seen % 2 == 0:
            # Index
            idx = struct.unpack("i", b_data[i:i + 4])[0]
        else:
            # Value
            current_line.append((idx, struct.unpack("f", b_data[i:i + 4])[0]))
        seen += 1
        if seen == num_values * 2:
            # If we've finished this line
            data.append(current_line)
            current_line = []
            label_bytes = None
            num_values = None
            seen = 0
    if keep_label:
        return data, labels
    return data


def serialize_data(data, labels=None):
    """ Serialize a sparse matrix for S3.
    The format is as follows:

    number_of_bytes
    number_of_data_rows
    {DATA_ROW_1}
    ...
    {DATA_ROW_n}

    where each data row is formatted as follows:

    label | num_col_for_row | col_idx1 | val1 | col_idx2 | val2 | ...
    """
    lines = []
    num_bytes = 0
    for idx, row in enumerate(data):
        current_line = []
        label = DEFAULT_LABEL
        if labels is not None:
            label = labels[idx]
        current_line.append(label)
        current_line.append(struct.pack("i", len(row)))
        for idx2, val in row:
            current_line.append(struct.pack("i", int(idx2)))
            current_line.append(struct.pack("f", float(val)))
        lines.append(b"".join(current_line))
        num_bytes += len(lines[-1])
    return struct.pack("i", num_bytes + 8) + \
        struct.pack("i", len(lines)) + b"".join(lines)


def get_random_color():
    """ Generates a random RGB color """
    def rand_256():
        """ Get a random integer from 0 to 255 """
        return random.randint(0, 255)
    return 'rgb(%d, %d, %d)' % (rand_256(), rand_256(), rand_256())


def get_all_lambdas():
    """ Get all lambda functions """
    return lc.list_functions()['Functions']


def public_dns_to_private_ip(public_dns):
    """ Convert an EC2 public DNS to private IP """
    filters = [{'Name': 'dns-name', 'Values': [public_dns]}]

    response = ec2c.describe_instances(Filters=filters)

    instances = response['Reservations'][0]['Instances']

    if instances:
        raise Exception('No EC2 with this: %s DNS name exists!' % public_dns)
    elif len(instances) > 1:
        raise Exception(
            'More than one EC2 with this: %s DNS name exists!' % public_dns)

    return instances[0]['PrivateIpAddress']


def lambda_exists(existing, name):
    """ lambda_exists(existing, name, size, zip_location)
    TODO: Check to see if uploaded SHA256 matches current bundle's SHA256
    Code below doesn't work, not sure if I need to hash zip or
    underlying code.
    with open(zip_location, 'rb') as f:
        zipped_code = f.read()
    bundle_sha = hashlib.sha256(zipped_code).hexdigest()
    """

    return any([lambda_['FunctionName'] == name for lambda_ in existing])


def create_lambda(fname, size=128):
    """ Create a lambda function """
    with open(fname, 'rb') as f:
        zipped_code = f.read()

    role = iam_client.get_role(RoleName="fix_lambda_role")

    fn = "testfunc1_%d" % size

    lc.create_function(
        FunctionName=fn,
        Runtime="python2.7",
        Handler='handler.handler',
        Code=dict(ZipFile=zipped_code),
        Timeout=300,
        Role=role['Role']['Arn'],
        Environment=dict(Variables=dict()),
        VpcConfig={
            'SubnetIds': ['subnet-bdb37ef4',
                          'subnet-db812abc',
                          'subnet-10082048'],
            'SecurityGroupIds': ['sg-63cfa618', 'sg-8bfd6af1', 'sg-36138a4e']},
        MemorySize=size)


def command_dict_to_file(command_dict):
    """ Takes a dictionary in the form of
    { 'machine-public-ip': ['list of commands'] }
    and creates a bash file for each machine that will
    run the command list """
    for key, num in zip(command_dict.keys(), range(len(command_dict.keys()))):
        lst = command_dict[key]

        with open("machine_%d.sh" % num, "w") as f_handle:
            for cmd in lst:
                f_handle.write(cmd + "\n\n")
