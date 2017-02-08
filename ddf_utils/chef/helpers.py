# -*- coding: utf-8 -*-

from functools import wraps, partial
from .. import config
from .. import ops
import os
import logging
import click
import numpy as np


def prompt_select(selects, text_before=None):
    """ask user to choose in a list of options"""
    def value_proc(v):
        if v == 'n':
            return -1
        if v == 'q':
            import sys
            sys.exit(130)  # code for user interrupted
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
    if val == -1:
        return -1
    return selects[val-1]


def read_opt(options, key, required=False, default=None):
    """utility to read an attribute from an options dictionary

    Parameters
    ----------
    options : dict
        the option dictionary to read
    key : `str`
        the key to read

    Keyword Args
    ------------
    required : bool
        if true, raise error if the `key` is not in the option dict
    default : object
        a default to return if `key` is not in option dict and `required` is false
    """
    if key in options.keys():
        return options.pop(key)
    if required:
        raise KeyError('field {} is mandantory'.format(key))
    return default


def mkfunc(options):
    """create function warppers base on the options provided

    This function is used in procedures which have a function block. Such as
    :py:func:`ddf_utils.chef.procedure.groupby`. It will try to return functions
    from numpy or :py:mod:`ddf_utils.ops`.

    Parameters
    ----------
    options : `str` or dict
        if a dictionary provided, "function" should be a key in the dictionary
    """
    if isinstance(options, str):
        return getattr(np, options)
    else:
        func = getattr(ops, options.pop('function'))
        return partial(func, **options)


# below functions are not used in ddf_utils yet, but may be useful.
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


def debuggable(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'debug' in kwargs.keys():
            debug = kwargs.pop('debug')
            result = func(*args, **kwargs)
            if debug:
                if config.DEBUG_OUTPUT_PATH is None:
                    logging.warning('debug output path not set!')
                    config.DEBUG_OUTPUT_PATH = './_debug'  # TODO: handle the config better
                outpath = os.path.join(config.DEBUG_OUTPUT_PATH, result.ingred_id)
                if os.path.exists(outpath):
                    import shutil
                    shutil.rmtree(outpath)
                    os.mkdir(outpath)
                result.serve(outpath)
        else:
            result = func(*args, **kwargs)
        return result
    return wrapper
