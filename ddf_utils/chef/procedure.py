# -*- coding: utf-8 -*-

"""all procedures for recipes"""

import pandas as pd
from . ingredient import Ingredient
import time
from typing import List, Union, Dict

import logging


def translate_header(ingredient, *, result=None, **options):

    logging.debug("translate_header: " + ingredient.ingred_id)

    rm = options['dictionary']
    data = ingredient.get_data().copy()

    for k, df in data.items():
        if k in rm.keys():  # if we are renaming concepts
            data[rm[k]] = data[k].rename(columns=rm)
            del(data[k])

        else:  # then we are renaming the index columns
            data[k] = data[k].rename(columns=rm)
            if ingredient.dtype == 'datapoints' or ingredient.dtype == 'concepts':
                for key in rm.keys():
                    if key in ingredient.key:
                        ingredient.key = ingredient.key.replace(key, rm[key])
            else:
                for key in rm.keys():
                    if key in ingredient.key:
                        ingredient.key[ingredient.key.index(key)] = rm[key]

    if not result:
        result = ingredient.ingred_id + '-translated'
    return Ingredient(result, result, ingredient.key, "*", data=data)


def translate_column(ingredient, *, result=None, **options):
    rm = options['dictionary']
    column = options['column']
    di = ingredient.get_data().copy()

    for k, df in di.items():

        df = df.set_index(column)
        di[k] = df.rename(index=rm).reset_index()

    if not result:
        result = ingredient.ingred_id + '-translated'
    return Ingredient(result, result, ingredient.key, "*", data=di)


def merge(*ingredients: List[Ingredient], result=None, **options):
    """the main merge function"""
    # all ingredients should have same dtype and index
    assert len(set([(x.dtype, x.key) for x in ingredients])) == 1

    # get the dtype and index
    dtype = ingredients[0].dtype

    if dtype == 'datapoints':
        index_col = ingredients[0].key_to_list()
    else:
        index_col = ingredients[0].key

    # deep merge is when we check every datapoint for existence
    # if false, overwrite is on the file level. If key-value
    # (e.g. geo,year-population_total) exists, whole file gets overwritten
    # if true, overwrite is on the row level. If values
    # (e.g. afr,2015-population_total) exists, it gets overwritten, if it doesn't it stays
    if 'deep' in options.keys():
        deep = options.pop('deep')
    else:
        deep = False

    # merge data from ingredients one by one.
    res_all = {}

    for i in ingredients:
        res_all = _merge_two(res_all, i.get_data(), index_col, dtype, deep)

    if not result:
        result = 'all_data_merged_'+str(int(time.time()))

    return Ingredient(result, result, index_col, '*', data=res_all)


def _merge_two(left: Dict[str, pd.DataFrame],
               right: Dict[str, pd.DataFrame],
               index_col: Union[List, str],
               dtype: str, deep=False) -> Dict[str, pd.DataFrame]:
    if len(left) == 0:
        return right

    if dtype == 'datapoints':
        if deep:
            for k, df in right.items():
                if k in left.keys():
                    left[k].append(df)
                    left[k] = left[k].drop_duplicates(cols=index_col, take_last=True)
                else:
                    left[k] = df
        else:
            for k, df in right.items():
                left[k] = df

        res_data = left

    elif dtype == 'concepts':

        left_df = pd.concat(left.values())
        right_df = pd.concat(right.values())

        if deep:
            left_df = left_df.merge(right_df, how='outer')
            res_data = {'concept': left_df.drop_duplicates()}
        else:
            res_data = {'concept': right_df.drop_duplicates()}
    else:
        # TODO
        raise NotImplementedError('entity data do not support merging yet.')

    return res_data


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

    if len(no_match) > 0:
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
    # TODO: load the op as ordered dict to speed up. (I can do some common ops at the beginning.)
    ops = options['op']

    # concat all the datapoint dataframe first, and eval the ops
    # TODO: concat() may be expansive. should find a way to improve.
    to_concat = [v.set_index(keys) for v in data.values()]
    df = pd.concat(to_concat, axis=1)

    for k, v in ops.items():
        data[k] = df.eval(v).dropna().reset_index(name=k)

    if not result:
        result = ingredient.ingred_id + '-op'
    return Ingredient(result, ingredient.ddf_id, ingredient.key, '*', data=data)
