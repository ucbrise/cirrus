"""Some utility functions.
"""
import time
import logging
import random
import sys

# The maximum factor by which an exponential backoff wait will be jittered
#   smaller or larger.
MAX_JITTER_FACTOR = 0.9


def jittery_exponential_backoff(exception_names, initial_wait=0.1,
                                wait_multiplier=2, max_retries=float("inf")):
    """Makes function decorators that retry a call if it raises an exception.

    Args:
        exception_names (tuple[str]): The names of the exceptions that should
            trigger retries. Other exceptions are passed through.
        initial_wait (float): The number of seconds to wait before retrying for
            the first time. Must be positive.
        wait_multiplier (float): If the wait before the `i`-th retry was `w`,
            then the wait before the `i+1`-th retry will be `w*wait_multiplier`.
            Must be positive.
        max_retries (int): The maximum number of times to retry a call. Must be
            positive.

    Returns:
        func: A function decorator.
    """
    assert initial_wait > 0
    assert wait_multiplier > 0
    assert max_retries > 0

    exception_names = set(exception_names)

    def decorator(f):

        def new_f(*args, **kwargs):
            log = logging.getLogger(
                "cirrus.utilities.jittery_exponential_backoff")

            retries = 0
            wait = initial_wait

            while True:
                try:
                    log.debug("jittery_exponential_backoff: Making attempt #%d."
                              % (retries + 1))
                    return f(*args, **kwargs)
                except Exception as e:
                    name = type(e).__name__
                    if name in exception_names and retries < max_retries:
                        wait_min = (1 - MAX_JITTER_FACTOR) * wait
                        wait_max = (1 + MAX_JITTER_FACTOR) * wait
                        jittered_wait = random.uniform(wait_min, wait_max)
                        log.debug("jittery_exponential_backoff: Waiting %fs."
                                  % jittered_wait)
                        time.sleep(jittered_wait)
                        retries += 1
                        wait *= wait_multiplier
                    else:
                        raise

        return new_f

    return decorator


def set_logging_handler():
    """Set up a logging handler for Cirrus' logs."""
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(funcName)16s | %(threadName)15s] %(message)s")
    handler.setFormatter(formatter)
    logger = logging.getLogger("cirrus")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
