# -*- coding: utf-8 -*-

from functools import wraps, partial
import numpy as np
from .. import ops
import logging
import click


def prompt_select(selects, text_before=None):

    def value_proc(v):
        if v == 'n':
            return -1
        if v == 'q':
            import sys
            sys.exit()
        return int(v)

    if text_before:
        click.echo(text_before)
    select_text = []

    for i, v in enumerate(selects):
        select_text.append('{}: {}'.format(i+1, v))
    select_text = '\n'.join(select_text)
    click.echo(select_text)

    prompt_text = 'Please select a value ({} ~ {}), or "n" to skip, "q" to quit'\
                  .format(1, len(selects))
    val = click.prompt(prompt_text, value_proc=value_proc)
    return val


def read_opt(options, key, required=False, default=None):
    if key in options.keys():
        return options.pop(key)
    if required:
        raise KeyError('field {} is mandantory'.format(key))
    return default


def mkfunc(options):
    if isinstance(options, str):
        return getattr(np, options)
    else:
        func = getattr(ops, options.pop('function'))
        return partial(func, **options)


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
