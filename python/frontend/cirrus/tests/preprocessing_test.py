# Preprocessing module for Cirrus

# TODO: Pytest

from .context import cirrus
import sklearn.datasets

def load_data(path):
    X, y = sklearn.datasets.load_svmlight_file(path)
    return X, y

def test_simple(src_bucket, dest_bucket, min_v, max_v, objects):
    # Make sure all data is in bounds, and all data is present
    pass

def test_exact(src_file, dest_bucket, min_v, max_v, objects):
    # Test that all data is scaled correctly
    pass
