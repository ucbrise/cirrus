import boto3
import json
import struct

def get_data_bounds(data):
    # Return a dict of two lists, containing max and min for each column.
    # Assumes labels are being stored right now.
    max_in_col = {}
    min_in_col = {}
    for r in data:
        for idx, v in r:
            if idx not in max_in_col:
                max_in_col[idx] = v
            if idx not in min_in_col:
                min_in_col[idx] = v
            if v > max_in_col[idx]:
                max_in_col[idx] = v
            if v < min_in_col[idx]:
                min_in_col[idx] = v
    return {
        "max": max_in_col,
        "min": min_in_col
    }

def get_data_from_s3(client, src_bucket, src_object, keep_label=False, data=None):
    # Return a 2D list, where each element is a row of the dataset.
    print("Getting bytes from boto3")
    b = data
    if data is None:
        b = client.get_object(Bucket=src_bucket, Key=src_object)["Body"].read()
    print("Got {0} bytes".format(len(b)))
    data = []
    labels = []
    c = []
    idx = None
    label_bytes = None
    num_values = None
    seen = 0
    print("Set local variables")
    for i in range(8, len(b), 4):
        if label_bytes is None:
            label_bytes = b[i:i+4]
            if keep_label:
                labels.append(label_bytes)
            continue
        if num_values is None:
            num_values = struct.unpack("i", b[i:i+4])[0]
            continue
        if seen % 2 == 0:
            idx = struct.unpack("i", b[i:i+4])[0]
        else:
            c.append((idx, struct.unpack("f", b[i:i+4])[0]))
        seen += 1
        if seen == num_values * 2:
            data.append(c)
            c = []
            label_bytes = None
            num_values = None
            seen = 0
    if keep_label:
        return data, labels
    return data

def put_bounds_in_s3(client, bounds, dest_bucket, dest_object):
    # Add the dictionary of bounds to an S3 bucket. 
    s = json.dumps(bounds)
    client.put_object(Bucket=dest_bucket, Key=dest_object, Body=s)

def get_global_bounds(client, bucket):
    # Get the bounds across all objects.
    b = client.get_object(Bucket=bucket, Key=bucket + "_final_bounds")["Body"].read()
    return json.loads(json.loads(b.decode("utf-8")))

def scale_data(data, g, min_v, max_v):
    data_f = []
    for r in data:
        r2 = []
        for idx_t, v in r:
            idx = str(idx_t)
            s = (max_v + min_v) / 2.0
            if g["min"][idx] != g["max"][idx]:
                s = (v - g["min"][idx]) / (g["max"][idx] - g["min"][idx]) * (max_v - min_v) + min_v
            r2.append((idx, s))
        data_f.append(r2)
    return data_f

def serialize_data(data, labels):
    lines = []
    num_bytes = 0
    for idx in range(len(data)):
        c = []
        c.append(labels[idx])
        c.append(struct.pack("i", len(data[idx])))
        for idx2, v2 in data[idx]:
            c.append(struct.pack("i", int(idx2)))
            c.append(struct.pack("f", float(v2)))
        lines.append(b"".join(c))
        num_bytes += len(lines[-1])
    return struct.pack("i", num_bytes + 8) + struct.pack("i", len(labels)) + b"".join(lines)

def handler(event, context):
    # Either calculates the local bounds, or scales data and puts the new data in
    # {src_object}_scaled.
    print(event["src_bucket"], event["src_object"])
    client = boto3.client("s3")
    if event["action"] == "LOCAL_BOUNDS":
        print("Getting data from S3...")
        d = get_data_from_s3(client, event["src_bucket"], event["src_object"])
        print("Getting local data bounds...")
        b = get_data_bounds(d)
        print("Putting bounds in S3...")
        put_bounds_in_s3(client, b, event["src_bucket"], event["src_object"] + "_bounds")
    elif event["action"] == "LOCAL_SCALE":
        d = get_data_from_s3(client, event["src_bucket"], event["src_object"], True)
        b = get_global_bounds(client, event["src_bucket"])
        scaled = scale_data(d[0], b, event["min_v"], event["max_v"])
        serialized = serialize_data(scaled, d[1])
        client.put_object(Bucket=event["src_bucket"], Key=event["src_object"] + "_scaled", Body=serialized)
    return []
