# -*- coding: utf-8 -*-

import os
import sys
import hashlib
import logging
import json
from functools import wraps, partial
from collections import Sequence, Mapping
from time import time

import click
import numpy as np
import pandas as pd
import dask.dataframe as dd

from joblib import Memory
from tempfile import mkdtemp

from . import ops
from ..model.package import DDFcsv


memory = Memory(location=mkdtemp(), verbose=0)


@memory.cache
def read_local_ddf(ddf_id, base_dir='./'):
    if os.path.isabs(ddf_id):
        return DDFcsv.from_path(ddf_id).ddf
    else:
        path = os.path.join(base_dir, ddf_id)
        return DDFcsv.from_path(path).ddf


def create_dsk(data, parts=10):
    # TODO: check the best parts to use
    for k, v in data.items():
        if isinstance(v, pd.DataFrame):
            data[k] = dd.from_pandas(v, npartitions=parts)
    return data


def dsk_to_pandas(data):
    """The reverse for create_dsk function"""
    for k, v in data.items():
        if isinstance(v, dd.DataFrame):
            data[k] = v.compute()
    return data


def build_dictionary(chef, dict_def, ignore_case=False):
    """build a dictionary from a dictionary definition"""
    if (len(dict_def) == 3 and
            'base' in dict_def and
            'key' in dict_def and
            'value' in dict_def):
        value = dict_def['value']
        key = dict_def['key']
        if isinstance(key, str):
            keys = [key]
        else:
            keys = key
        ingredient = chef.dag.node_dict[dict_def['base']].evaluate()
        if ingredient.dtype == 'synonyms':
            di = ingredient.get_data()[value]   # synonyms data is already dict
            if ignore_case:
                res = dict()
                for k, v in di.items():
                    res[k.lower()] = v
            else:
                res = di.copy()
            return res
        elif ingredient.dtype == 'entities':
            df = ingredient.get_data()[ingredient.key]
            return build_dictionary_from_dataframe(df, keys, value, ignore_case)
        else:
            raise NotImplementedError('unsupported data type {}'.format(ingredient.dtype))
    elif isinstance(dict_def, str):
        base_path = chef.config['dictionaries_dir']
        path = os.path.join(base_path, dict_def)
        return build_dictionary_from_file(path)
    else:
        return dict_def


def build_dictionary_from_file(file_path):
    d = json.load(open(file_path, 'r'))
    assert isinstance(d, dict)
    return d


def build_dictionary_from_dataframe(df, keys, value, ignore_case=False):
    dic = dict()
    for k in keys:
        d = (df[[k, value]]
             .dropna(how='any')
             .set_index(k)[value]
             .to_dict())
        for i, v in d.items():
            if ignore_case:
                if isinstance(i, str):
                    i = i.lower()
            if i not in dic.keys():
                dic[i] = v
            elif dic[i] != v:
                raise KeyError("ambiguous key: {} is mapped "
                               "to both {} and {}".format(i, dic[i], v))
            else:
                continue
    return dic


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


def read_opt(options, key, required=False, default=None, method='get'):
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
        if method == 'get':
            return options.get(key)
        elif method == 'pop':
            return options.pop(key)
        else:
            raise ValueError("{} is not supported method".format(method))
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


def gen_sym(key, others=None, options=None):
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


def procedure(func, **options_dict):
    """The procedure wrapper, to create a procedure

    @procedure
    def procedure_func(**options):
        pass

    will automatically create this:

    def procedure_func(chef=chef, ingredients=[], result='result', **options) 

    """
    @wraps(func)
    @debuggable
    def procedure_func(chef, ingredients, result, *args, **kwargs):
        return
    return procedure_func


def debuggable(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'debug' in kwargs.keys():
            debug = kwargs.pop('debug')
        else:
            debug = False

        if 'breakpoint' in kwargs.keys():
            bk = kwargs.pop('breakpoint')
        else:
            bk = False

        if bk:
            import ipdb
            ipdb.pm()
            result = ipdb.runcall(func, *args, **kwargs)
        else:
            result = func(*args, **kwargs)

        if debug:
            chef = args[0]
            debug_path = chef.config.get('debug_output_path', None)
            if debug_path is None:
                logging.warning('debug output path not set!')
                chef.add_config(debug_output_path='./_debug')
                debug_path = './_debug'
            outpath = os.path.join(debug_path, result.id)
            if os.path.exists(outpath):
                import shutil
                shutil.rmtree(outpath)
                os.mkdir(outpath)
            result.serve(outpath)
        return result
    return wrapper
