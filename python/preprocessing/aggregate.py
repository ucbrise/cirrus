import json

def MinMaxScaler(bucket_name, lower, upper):
    for i in range(999):
        # TODO: Invoke lambda to get the local mins / maxes.

    f_max = {}
    f_min = {}

    for i in range(999):
        k = str(i) + "_bounds"
        obj = client.get_object(Bucket=bucket_name, Key=k)["Body"].read()
        d = json.loads(obj)
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

    # TODO: Invoke lambda function to fit values in range.

MinMaxScaler("criteo-kaggle-19b", 0, 1)