import time
from functools import wraps
import logging
from config import config

FORMAT = "%(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT, level=config.LOGGER_LEVEL)


def backoff(exception, start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param exception: exception class to catch
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
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