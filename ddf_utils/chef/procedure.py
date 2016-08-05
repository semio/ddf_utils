# -*- coding: utf-8 -*-

"""all procedures for recipes"""

import pandas as pd
import json
from . config import *
from . ingredient import Ingredient

import logging


# TODO: _translate_header and _translate_column should be combined.
def translate_header(ingredient, *, result=None, **options):

    global DICT_PATH

    dictionary = options['dictionary']

    di = ingredient.get_data().copy()

    if isinstance(dictionary, dict):
        rm = dictionary
    else:
        rm = json.load(open(os.path.join(DICT_PATH, dictionary), 'r'))

    for k, df in di.items():

        if k in rm.keys():
            di[rm[k]] = di[k].rename(columns=rm)
            del(di[k])

        else:
            di[k] = di[k].rename(columns=rm)

    if not result:
        result = ingredient.ingred_id + '-translated'
    return Ingredient(result, result, ingredient.key, "*", data=di)


def translate_column(ingredient, *, result=None, **options):

    global DICT_PATH

    dictionary = options['dictionary']
    column = options['column']

    di = ingredient.get_data().copy()

    if isinstance(dictionary, dict):
        rm = dictionary
    else:
        rm = json.load(open(os.path.join(DICT_PATH, dictionary), 'r'))

    for k, df in di.items():

        df = df.set_index(column)
        di[k] = df.rename(index=rm).reset_index()

    if not result:
        result = ingredient.ingred_id + '-translated'
    return Ingredient(result, result, ingredient.key, "*", data=di)


def merge(left, right, *, result=None, **options):
    """the main merge function"""
    # TODO:
    # 1. add `op` parameter: merge left, right with the op function
    #     1.1 add dtype parameter. in run_recipe() we can get the dtype from recipe.
    #

    # deep merge is when we check every datapoint for existence
    # if false, overwrite is on the file level. If key-value
    # (e.g. geo,year-population_total) exists, whole file gets overwritten
    # if true, overwrite is on the row level. If values
    # (e.g. afr,2015-population_total) exists, it gets overwritten, if it doesn't it stays
    deep = options['deep']

    left_data = left.get_data().copy()
    right_data = right.get_data().copy()

    assert left.dtype == right.dtype

    if left.dtype == 'datapoints':

        if deep:
            for k, df in right_data.items():
                if k in left_data.keys():
                    left_data[k].update(df)  # TODO: maybe need to set_index before update
                else:
                    left_data[k] = df
        else:
            for k, df in right_data.items():
                left_data[k] = df

        res_data = left_data

    elif left.dtype == 'concepts':

        left_df = pd.concat(left_data.values())
        right_df = pd.concat(right_data.values())

        if deep:
            left_df = left_df.merge(right_df, how='outer')
            res_data = left_df
        else:
            res_data = right_df

    else:
        # TODO
        raise NotImplementedError('entity data do not support merging yet.')

    if not result:
        result = left.ingred_id + '-merged'
    return Ingredient(result, left.ddf_id, left.key, '*', data=res_data)


def identity(ingredient, *, result=None, **options):
    if 'copy' in options:
        ingredient.data = ingredient.get_data_copy()
    else:
        ingredient.data = ingredient.get_data()

    if result:
        ingredient.ingred_id = result
    return ingredient


def filter_row(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """filter an ingredient based on a set of options and return
    the result as new ingredient
    """
    data = ingredient.get_data()
    dictionary = options.pop('dictionary')

    res = {}

    for k, v in dictionary.items():
        from_name = v.pop('from')
        df = data[from_name]
        if len(v) == 0:
            res[k] = df.rename(columns={from_name: k})
            continue
        queries = []

        for col, val in v.items():
            if isinstance(val, list):
                queries.append("{} in {}".format(col, val))
            elif isinstance(val, str):
                queries.append("{} == '{}'".format(col, val))
            # TODO: support more query methods.
            else:
                raise ValueError("not supported in query: " + type(val))

        query_string = ' and '.join(queries)

        logging.debug('query sting: ' + query_string)

        df = df.query(query_string).copy()
        df = df.rename(columns={from_name: k})
        # drops a column if all values are same.
        newkey = ingredient.key
        for c in df.columns:
            if ingredient.dtype == 'datapoints' and len(df[c].unique()) == 1:
                df = df.drop(c, axis=1)
                keys = ingredient.key_to_list()
                keys.remove(c)
                newkey = ','.join(keys)
        res[k] = df

    if not result:
        result = ingredient.ingred_id + '-filtered'
    # TODO: the ingredient key need to be dropped too.
    return Ingredient(result, result, newkey, '*', data=res)


def filter_item(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """filter item from the ingredient data dict"""
    data = ingredient.get_data()
    items = options.pop('items')

    ingredient.data = dict([(k, data[k]) for k in data.keys() if k in items])

    return ingredient


def format_data():
    """format floating points"""
    pass


def align(to_align: Ingredient, base: Ingredient, *, result=None, **options) -> Ingredient:
    try:
        search_cols = options.pop('search_cols')
        to_find = options.pop('to_find')
        to_replace = options.pop('to_replace')
    except KeyError:
        raise KeyError("not enough parameters! please check your recipe")

    if len(base.get_data()) > 1:
        logging.warning(base.get_data().keys())
        raise NotImplementedError('align to base data with multiple dataframes is not supported yet.')

    logging.info("aligning: {} and {}".format(to_align.ingred_id, base.ingred_id))

    base_data = list(base.get_data().values())[0]
    ing_data = to_align.get_data()

    base_data = base_data.set_index(base.key)

    mapping = {}
    no_match = []

    for k, df in ing_data.items():
        for f in df[to_find].drop_duplicates().values:
            if f in mapping:
                continue
            # TODO: if I don't add drop_duplicates() below, I will get multiple same rows.
            # find out why.
            filtered = base_data[base_data[search_cols].values == f].drop_duplicates()
            if len(filtered) == 1:
                mapping[f] = filtered.index[0]
            elif len(filtered) > 1:
                logging.warning("multiple match found: "+f)
                mapping[f] = filtered.index[0]
            else:
                no_match.append(f)
        df = df[df[to_find].isin(mapping.keys())].copy()  # only keep those available in mappings.
        for old, new in mapping.items():
            df.loc[df[to_find] == old, to_replace] = new

        ing_data[k] = df

    logging.warning("no match found for: " + str(set(no_match)))

    if not result:
        result = to_align.ingred_id + '-aligned'
    if to_align.dtype == 'datapoints':
        newkey = to_align.key.replace(to_find, to_replace)
        return Ingredient(result, result, newkey, '*', data=ing_data)
    else:
        return Ingredient(result, result, to_replace, '*', data=ing_data)


def groupby(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    data = ingredient.get_data()
    by = options.pop('by')
    agg = options.pop('aggregate')

    for k, df in data.items():
        df = df.groupby(by=by).agg({k: agg})
        newkey = ','.join(df.index.names)
        data[k] = df.reset_index()

    if not 'result':
        result = ingredient.ingred_id + '-agg'
    return Ingredient(result, result, newkey, '*', data=data)


def run_op(ingredient: Ingredient, *, result=None, **options) -> Ingredient:

    assert ingredient.dtype == 'datapoints'

    data = ingredient.get_data()
    keys = ingredient.key_to_list()
    # TODO: load the op as ordered dict to spead up. (I can do some comman ops at the beginning.)
    ops = options['op']

    # concat all the datapoint dataframe first, and eval the ops
    # TODO: concat() may be expansive. should find a way to improve.
    to_concat = [v.set_index(keys) for v in data.values()]
    df = pd.concat(to_concat, axis=1)

    for k, v in ops.items():
        data[k] = df.eval(v).dropna().reset_index(name=k)

    logging.debug(data.keys())

    if not result:
        result = ingredient.ingred_id + '-op'
    return Ingredient(result, ingredient.ddf_id, ingredient.key, '*', data=data)
