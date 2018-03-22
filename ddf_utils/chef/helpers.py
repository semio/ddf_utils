# -*- coding: utf-8 -*-

import os
import sys
import hashlib
import logging
from functools import wraps, partial
from collections import Sequence, Mapping
from time import time

import click
import numpy as np
import pandas as pd
import dask.dataframe as dd

from . import ops


def create_dsk(data, parts=10):
    # TODO: check the best parts to use
    for k, v in data.items():
        if isinstance(v, pd.DataFrame):
            data[k] = dd.from_pandas(v, npartitions=parts)
    return data


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


def sort_df(df, key):
    if isinstance(key, str):
        key = [key]

    cols_minus_key = df.head().set_index(key).columns.values.tolist()

    cols_minus_key.sort()
    key.sort()
    cols_new = [*key, *cols_minus_key]

    # change categorical type to string.
    # we need to do this because we didn't define order for category
    # when we read the data
    cat_cols = df.select_dtypes(include=['category']).columns.values.tolist()
    for c in cat_cols:
        df[c] = df[c].astype('str')

    df = df.sort_values(by=key)

    return df[cols_new]


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
        raise KeyError('Field "{}" is mandatory. Please provide this field in the options.'.format(key))
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


def get_procedure(procedure, base_dir):
    """return a procedure function from the procedure name

    Parameters
    ----------
    procedure : `str`
        the procedure to get, supported formats are
        1. procedure: sub/dir/module.function
        2. procedure: module.function
    base_dir : `str`
        the path for searching procedures
    """
    import ddf_utils.chef.procedure as pc
    if '.' in procedure:
        assert 'base_dir' is not None, "please set procedure_dir in config if you have custom procedures"
        sys.path.insert(0, base_dir)
        module_name, func_name = procedure.split('.')
        _mod = __import__(module_name)
        func = getattr(_mod, func_name)
        sys.path.remove(base_dir)
    else:
        func = getattr(pc, procedure)

    return func


def gen_sym(key, others, options):
    """generate symbol for chef ingredient/procedure result"""
    tail = hashlib.sha256((str(others) + str(options) + str(time())).encode('utf8')).hexdigest()[:6]
    return '{}_{}'.format(key, tail)


def gen_query(conds, scope=None, available_scopes=None):
    """generate dataframe query from mongo-like queries"""
    comparison_keywords = {
        '$eq': '==',
        '$gt': '>',
        '$gte': '>=',
        '$lt': '<',
        '$lte': '>',
        '$ne': '!=',
        '$in': 'in',
        '$nin': 'not in'
    }
    logical_keywords = ['$and', '$or', '$not', '$nor']

    def process_values(n):
        if isinstance(n, str):
            return "'{}'".format(n)
        else:
            return str(n)

    res = []

    for key, val in conds.items():
        if key in logical_keywords:
            if key in ['$and', '$or']:
                to_join = ' {} '.format(key[1:])
                queries = [gen_query({k: v}, scope, available_scopes) for k, v in val.items()]
                return '({})'.format(to_join.join(filter(lambda x: x != '', queries)))
            if key == '$not':
                queries = [gen_query({k: v}, scope, available_scopes) for k, v in val.items()]
                return '~({})'.format(' and '.join(filter(lambda x: x != '', queries)))
            if key == '$nor':
                queries = [gen_query({k: v}, scope, available_scopes) for k, v in val.items()]
                return ' and '.join(['~({})'.format(filter(lambda x: x != '', queries))])
        else:
            if key in comparison_keywords.keys():
                assert not isinstance(val, Mapping)
                assert scope != None
                res.append("{} {} {}".format(scope,
                                             comparison_keywords[key],
                                             process_values(val)))
            elif isinstance(val, str):
                res.append("{} == {}".format(key, process_values(val)))
            elif isinstance(val, Sequence):
                res.append("{} in {}".format(key, process_values(val)))
            else:
                if available_scopes is None or key in available_scopes:
                    res.append(gen_query(val, scope=key, available_scopes=available_scopes))
    return ' and '.join(res)


def query(df, conditions, available_scopes=None):
    """query a dataframe with mongo-like queries"""
    q = gen_query(conditions, available_scopes=available_scopes)
    logging.debug("querying: {}".format(q))
    if q == '' or q == '()':
        logging.warning("empty query")
        return df
    return df.query(q)


def debuggable(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'debug' in kwargs.keys():
            debug = kwargs.pop('debug')
            result = func(*args, **kwargs)
            chef = args[0]
            if debug:
                debug_path = chef.config.get('debug_output_path', None)
                if debug_path is None:
                    logging.warning('debug output path not set!')
                    chef.add_config(debug_output_path='./_debug')
                    debug_path = './_debug'
                outpath = os.path.join(debug_path, result.ingred_id)
                if os.path.exists(outpath):
                    import shutil
                    shutil.rmtree(outpath)
                    os.mkdir(outpath)
                result.serve(outpath)
        else:
            if 'breakpoint' in kwargs.keys():
                bk = kwargs.pop('breakpoint')
                if bk:
                    import ipdb; ipdb.set_trace()
            result = func(*args, **kwargs)
        return result
    return wrapper
