# Preprocessing module for Cirrus

from enum import Enum
import MinMaxScaler

class NormalizationType(Enum):
    MIN_MAX = 1

def normalize(s3_bucket_input, s3_bucket_output, normalization_type, *args):
    if normalization_type == NormalizationType.MIN_MAX:
        assert len(args) >= 2 and len(args) <= 4, "Must specify min and max."
        MinMaxScaler.MinMaxScaler(s3_bucket_input, s3_bucket_output, *args)

def load_libsvm(path, s3_bucket):
    # TODO: Implement.
    pass
