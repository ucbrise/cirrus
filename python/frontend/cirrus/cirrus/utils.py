import random
import struct
from collections import deque

import boto3


def launch_lambdas(lambda_cls, objects, max_lambdas=400, *params):
    """ Launch one lambda for each of the objects passed in. """
    threads = deque()
    for i in objects:
        while len(threads) > max_lambdas:
            t = threads.popleft()
            t.join()
        l = lambda_cls(i, *params)
        l.start()
        threads.append(l)

    for t in threads:
        t.join()

def retry_loop(func, exceptions=(), handle_exception=None, max_attempts=3, name="Function"):
    """ Retry a function however many times, but stop if
    one of the specified exceptions occurs. """
    curr_attempt = 1
    overall = time.time()
    while curr_attempt <= max_attempts:
        try:
            current = time.time()
            func()
            print("{0} completed this attempt in {1}, all attempts in {2}".format(
                name, time.time() - current, time.time() - overall))
            break
        except exceptions as e:
            if handle_exception is not None:
                handle_exception(e)
            curr_attempt += 1
        except Exception as e:
            curr_attempt += 1
            if curr_attempt > max_attempts:
                raise e
        print("{0}: Launching attempt #{1}".format(name, curr_attempt))

def delete_all_keys(bucket):
    # Delete all keys from an S3 bucket
    return get_all_keys(bucket, "")


def get_all_keys(bucket, contains="_"):
    # Get all keys from an S3 bucket, deleting any key that has the substring "contains"
    s3 = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    keys = []
    kwargs = {"Bucket": bucket}
    # Get all keys
    while True:
        r = s3.list_objects_v2(**kwargs)
        if "Contents" not in r:
            break
        for o in r["Contents"]:
            keys.append(o["Key"])
        try:
            kwargs["ContinuationToken"] = r["NextContinuationToken"]
        except KeyError:
            break

    print("Found {0} chunks...".format(len(keys)))
    # Delete the objects with keys that have the substring "contains"
    final_objects = []
    for o in keys:
        if contains in o:
            s3_resource.Object(bucket, o).delete()
        else:
            final_objects.append(o)
    print("Chunks after pruning: {0}".format(len(final_objects)))
    return final_objects


def get_data_from_s3(client, src_bucket, src_object, keep_label=False):
    # Return a 2D list, where each element is a row of the dataset.
    b = client.get_object(Bucket=src_bucket, Key=src_object)["Body"].read()
    data = []
    labels = []
    current_line = []
    idx = None
    label_bytes = None
    num_values = None
    seen = 0
    for i in range(8, len(b), 4):
        # Ignore the first 8 bytes, and get each 4 byte chunk one at a time
        if label_bytes is None:
            # If we haven't gotten the label for this line
            label_bytes = b[i:i + 4]
            if keep_label:
                labels.append(label_bytes)
            continue
        if num_values is None:
            # If we haven't gotten the number of values for this line
            num_values = struct.unpack("i", b[i:i + 4])[0]
            continue
        if seen % 2 == 0:
            # Index
            idx = struct.unpack("i", b[i:i + 4])[0]
        else:
            # Value
            current_line.append((idx, struct.unpack("f", b[i:i + 4])[0]))
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
    DEFAULT = struct.pack("i", 0)
    lines = []
    num_bytes = 0
    for idx in range(len(data)):
        current_line = []
        label = DEFAULT
        if labels is not None:
            label = labels[idx]
        current_line.append(label)
        current_line.append(struct.pack("i", len(data[idx])))
        for idx2, v2 in data[idx]:
            current_line.append(struct.pack("i", int(idx2)))
            current_line.append(struct.pack("f", float(v2)))
        lines.append(b"".join(current_line))
        num_bytes += len(lines[-1])
    return struct.pack("i", num_bytes + 8) + struct.pack("i", len(lines)) + b"".join(lines)


def get_random_color():
    # Generates a random RGB color
    def rand_256(): return random.randint(0, 255)
    return 'rgb(%d, %d, %d)' % (rand_256(), rand_256(), rand_256())


def command_dict_to_file(command_dict):
    """ Takes a dictionary in the form of { 'machine-public-ip': ['list of commands'] }
    and creates a bash file for each machine that will run the command list """
    for key, no in zip(command_dict.keys(), range(len(command_dict.keys()))):
        lst = command_dict[key]

        with open("machine_%d.sh" % no, "w") as f:
            for cmd in lst:
                f.write(cmd + "\n\n")
