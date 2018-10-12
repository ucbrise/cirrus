""" Helper functions for normal scaling. """

import json
import struct

import boto3


def get_data_ranges(data):
    """ Return a dict where each index is a list of [E[X^2], mean, n].
    Assumes labels are being stored. """
    x_squared_col = {}
    x_col = {}
    n_col = {}
    for row in data:
        for idx, val in row:
            if idx not in x_squared_col:
                x_squared_col[idx] = 0
                x_col[idx] = 0
                n_col[idx] = 0
            x_squared_col[idx] += float(val)**2
            x_col[idx] += v
            n_col[idx] += 1

    final = {}
    for k in x_squared_col:
        final[k] = [x_squared_col[k] / n_col[k], x_col[k] / n_col[k], n_col[k]]
    return final


def scale_data(data, global_map):
    """ Takes g, a map to [std_dev, mean] """
    for row in data:
        for j in range(len(row)):
            idx_t, val = row[j]
            idx = str(idx_t)
            scaled = 0
            if global_map[idx][0] != 0:
                scaled = (val - global_map[idx][1]) / global_map[idx][0]
            row[j] = (idx, scaled)
    return data
