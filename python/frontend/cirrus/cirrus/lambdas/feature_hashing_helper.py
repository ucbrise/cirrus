""" Helper functions for feature hashing """

import hashlib
import json
import struct

import boto3


def hash_data(data, columns, N):
    """ Replace the appropriate columns for this row. """
    col_set = set([int(i) for i in columns])
    for idx, row in enumerate(data):
        row_map = {}
        for col, val in row:
            s_c = int(col)
            if s_c in col_set:
                hasher = hashlib.sha256()
                hasher.update(str(val))
                hash_val = int(hasher.digest(), 16) % N
                if hash_val not in row_map:
                    row_map[hash_val] = 0
                row_map[h] += 1
            else:
                row_map[col] = val

        row_values = []
        for k in row_map:
            row_values.append((k, row_map[k]))
        data[idx] = row_values
    return data
