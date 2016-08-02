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
    # if false, overwrite is on the file level. If key-value (e.g. geo,year-population_total) exists, whole file gets overwritten
    # if true, overwrite is on the row level. If values (e.g. afr,2015-population_total) exists, it gets overwritten, if it doesn't it stays
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


def filter_col(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """filter an ingredient based on a set of options and return
    the result as new ingredient
    """
    data = ingredient.get_data()
    dictionary = options.pop('dictionary')

    res = {}

    for k, v in dictionary.items():
        from_name = v.pop('from')
        df = data[from_name]
        # TODO: support more query methods.
        query = ' '.join(["{} == '{}'".format(x, y) for x, y in v.items()])

        logging.debug('query sting: ' + query)

        df = df.query(query).copy()
        df = df.drop(v.keys(), axis=1).rename(columns={from_name: k})
        res[k] = df

    if not result:
        result = ingredient.ingred_id + '-filtered'
    return Ingredient(result, result, ingredient.key, '*', data=res)


def align(to_align: Ingredient, base: Ingredient, *, result=None, **options) -> Ingredient:
    try:
        search_cols = options.pop('search_cols')
        to_find = options.pop('to_find')
        to_replace = options.pop('to_replace')
    except KeyError:
        raise KeyError("not enough parameters! please check your recipe")

    if len(base.get_data()) > 1:
        raise NotImplementedError('align to base data with multiple dataframes is not supported yet.')

    base_data = list(base.get_data().values())[0]
    ing_data = to_align.get_data()

    base_data = base_data.set_index(base.key)

    mapping = {}

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
                logging.warning("No match found: "+f)

        df = df[df[to_find].isin(mapping.keys())]  # only keep those available in mappings.
        for old, new in mapping.items():
            df.ix[df[to_find] == old, to_replace] = new

        ing_data[k] = df

    if not result:
        result = to_align.ingred_id + '-aligned'
    return Ingredient(result, result, to_replace, '*', data=ing_data)



def groupby(ingredient, **options):
    pass


def run_op(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    data = ingredient.get_data()

    ops = options['op']

    for k, v in ops.items():
        df = data[k]
        data[k][k] = df.eval('{} '.format(k) + v)

    if not result:
        result = ingredient.ingred_id + '-op'
    return Ingredient(result, ingredient.ddf_id, ingredient.key, '*', data=data)
