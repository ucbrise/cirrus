# Helper functions for S3.

from threading import Thread

class LambdaThread(Thread):
    def run(self):
        l_client = boto3.client("lambda")
        l_client.invoke(FunctionName="neel_lambda", InvocationType="RequestResponse", LogType="Tail",
            Payload=json.dumps(self.d))

def get_data_from_s3(client, src_bucket, src_object, keep_label=False):
    # Return a 2D list, where each element is a row of the dataset.
    print("Getting bytes from boto3")
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

def serialize_data(data, labels=None):
    DEFAULT = struct.pack("i", 0)
    lines = []
    num_bytes = 0
    for idx in range(len(data)):
        c = []
        l = DEFAULT
        if labels is not None:
            l = labels[idx]
        c.append(l)
        c.append(struct.pack("i", len(data[idx])))
        for idx2, v2 in data[idx]:
            c.append(struct.pack("i", int(idx2)))
            c.append(struct.pack("f", float(v2)))
        lines.append(b"".join(c))
        num_bytes += len(lines[-1])
    return struct.pack("i", num_bytes + 8) + struct.pack("i", len(labels)) + b"".join(lines)