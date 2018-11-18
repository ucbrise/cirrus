""" Helper functions for feature hashing """

import mmh3

HASH_SEED = 42


def hash_data(data, columns, n_buckets):
    """ Replace the appropriate columns for this row. """
    col_set = set([int(i) for i in columns])
    for idx, row in enumerate(data):
        row_map = {}
        for col, val in row:
            s_c = int(col)
            if s_c in col_set:
                hash_val = mmh3.hash(str(val), HASH_SEED, signed=False)
                bucket = hash_val % n_buckets
                if bucket not in row_map:
                    row_map[bucket] = 0
                row_map[bucket] += 1
            else:
                row_map[n_buckets + col] = val

        row_values = []
        for k in row_map:
            row_values.append((k, row_map[k]))
        data[idx] = row_values
    return data
