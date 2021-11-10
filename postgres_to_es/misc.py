import logging
import time
from functools import wraps
from config import config

FORMAT = "%(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT, level=config.LOGGER_LEVEL)


def backoff(exception, start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    A function to re-execute the function after a while if an error occurs.
    Uses a naive exponential growth of the retry time (factor) to the border timeout (border_sleep_time)
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            delay = start_sleep_time
            while True:
                try:
                    connection = func(*args, **kwargs)
                    return connection
                except exception as e:
                    logging.exception(f"Retry up connection after delay {delay} seconds\nError message: {e}")
                    if delay >= border_sleep_time:
                        delay = border_sleep_time
                    else:
                        delay += start_sleep_time * 2 ** factor
                    time.sleep(delay)
        return inner

    return func_wrapper