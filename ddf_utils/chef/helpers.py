# -*- coding: utf-8 -*-

from functools import wraps
import logging


def read_opt(options, key, required=False, default=None):
    if key in options.keys():
        return options.pop(key)
    if required:
        raise KeyError('field {} is mandantory'.format(key))
    return default


def log_shape(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        logging.info("%s,%s" % (func.__name__, result.shape))
        return result
    return wrapper


def log_dtypes(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        logging.info("%s,%s" % (func.__name__, result.dtypes))
        return result
    return wrapper


def log_procedure(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info("running %s" % (func.__name__))
        result = func(*args, **kwargs)
        return result
    return wrapper
