import boto3
import json
import struct
import hashlib


def hash_data(data, columns, N):
    # Replace the appropriate columns for this row.
    c = set([int(i) for i in columns])
    for idx, r in enumerate(data):
        row_map = {}
        for col, val in r:
            s_c = int(col)
            if s_c in c:
                hasher = hashlib.sha256()
                hasher.update(str(val))
                h = int(hasher.digest(), 16) % N
                if h not in row_map:
                    row_map[h] = 0
                row_map[h] += 1
            else:
                row_map[col] = val

        row = []
        for k in row_map:
            row.append((k, row_map[k]))
        data[idx] = row
    return data
