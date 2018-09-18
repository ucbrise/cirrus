import json
import boto3

def MinMaxScaler(bucket_name, objects, lower, upper):
    l_client = boto3.client("lambda")
    d = {
        "src_bucket": bucket_name,
        "action": "LOCAL_BOUNDS"
    }
    for i in objects:
        d["src_object"] = i
        l_client.invoke(FunctionName="neel_lambda", InvocationType="RequestResponse", LogType="Tail",
                Payload=json.dumps(d))

    client = boto3.client("s3")
    f_max = {}
    f_min = {}

    for i in objects:
        k = str(i) + "_bounds"
        obj = client.get_object(Bucket=bucket_name, Key=k)["Body"].read()
        d = json.loads(obj.decode("utf-8"))
        for idx in d["max"]:
            v = d["max"][idx]
            if idx not in f_max:
                f_max[idx] = v
            if v > f_max[idx]:
                f_max[idx] = v
        for idx in d["min"]:
            v = d["min"][idx]
            if idx not in f_min:
                f_min[idx] = v
            if v < f_min[idx]:
                f_min[idx] = v

    final = json.dumps({
        "max": f_max,
        "min": f_min
    })

    s = json.dumps(final)
    client.put_object(Bucket=bucket_name, Key=bucket_name + "_final_bounds", Body=s)
    d["min_v"] = lower
    d["max_v"] = upper
    d["action"] = "LOCAL_SCALE"
    for i in objects:
        d["src_object"] = i
        l_client.invoke(FunctionName="neel_lambda", InvocationType="RequestResponse", LogType="Tail",
                Payload=json.dumps(d))

MinMaxScaler("criteo-kaggle-19b", ["1"], 0, 1)
