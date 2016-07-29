# -*- coding: utf-8 -*-

"""all procedures for recipes"""

import pandas as pd

# TODO: _translate_header and _translate_column should be combined.
def _translate_header(ingredient, result, **options):

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


def _translate_column(ingredient, result, **options):

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


def _merge(left, right, **options):
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


def _identity(ingredient):
    return ingredient.get_data_copy()


def _filter(ingredient, **options):
    """filter an ingredient based on a set of options and return
    the result as new ingredient
    """
    pass


def _align(ingredient, base, **options):
    pass


def _groupby(ingredient, **options):
    pass


def _run_op(ingredient: Ingredient, **options):
    data = ingredient.get_data()

    ops = options['op']

    for k, v in ops:
        df = data[k]
        data[k] = df.eval('{} ' + v)

    ingredient.data = data
