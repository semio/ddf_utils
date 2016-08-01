# -*- coding: utf-8 -*-

"""all procedures for recipes"""

import pandas as pd
import json
from . config import *
from . ingredient import Ingredient

import logging


# TODO: _translate_header and _translate_column should be combined.
def translate_header(ingredient, result, **options):

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

    return Ingredient(result, result, ingredient.key, "*", data=di)


def translate_column(ingredient, result, **options):

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

    return Ingredient(result, result, ingredient.key, "*", data=di)


def merge(left, right, **options):
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

        return left_data

    elif left.dtype == 'concepts':

        left_df = pd.concat(left_data.values())
        right_df = pd.concat(right_data.values())

        if deep:
            left_df = left_df.merge(right_df, how='outer')
            return left_df
        else:
            return right_df

    else:
        # TODO
        raise NotImplementedError('entity data do not support merging yet.')


def identity(ingredient, **options):
    try:
        if options.pop('copy'):
            return ingredient.get_data_copy()
    except KeyError:
        pass
    return ingredient.get_data()


def filter_col(ingredient: Ingredient, **options) -> dict:
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

    return res


def align(ingredient, base, **options):
    pass


def groupby(ingredient, **options):
    pass


def run_op(ingredient: Ingredient, **options) -> dict:
    data = ingredient.get_data()

    ops = options['op']

    for k, v in ops.items():
        df = data[k]
        data[k][k] = df.eval('{} '.format(k) + v)

    return data
