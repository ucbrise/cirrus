import boto3
import json
import struct

def get_data_ranges(data):
    # Return a dict where each index is a list of [E[X^2], mean, n].
    # Assumes labels are being stored right now.
    x_squared_col = {}
    x_col = {}
    n_col = {}
    for r in data:
        for idx, v in r:
            if idx not in x_squared_col:
                x_squared_col[idx] = 0
                x_col[idx] = 0
                n_col[idx] = 0
            x_squared_col[idx] += float(v)**2
            x_col[idx] += v
            n_col[idx] += 1

    final = {}
    for k in x_squared_col:
        final[k] = [x_squared_col[k] / n_col[k], x_col[k] / n_col[k], n_col[k]]
    return final

def scale_data(data, g):
    # Takes g, a map to [std_dev, mean]
    for r in data:
        for j in range(len(r)):
            idx_t, v = r[j]
            idx = str(idx_t)
            s = 0
            if g[idx][0] != 0:
                s = (v - g[idx][1]) / g[idx][0]
            r[j] = (idx, s)
    return data
