# -*- coding: utf-8 -*-

"""all procedures for recipes"""

import pandas as pd
import numpy as np
from . ingredient import Ingredient
import time
from typing import List, Union, Dict, Optional
import re

import logging


def translate_header(ingredient, *, result=None, **options):

    logging.debug("translate_header: " + ingredient.ingred_id)

    rm = options['dictionary']
    data = ingredient.get_data().copy()

    for k in list(data.keys()):
        if k in rm.keys():  # if we need to rename the concept name
            data[rm[k]] = data[k].rename(columns=rm).copy()
            del(data[k])
        else:  # we only rename index/properties columns
            data[k] = data[k].rename(columns=rm)

    # also rename the key
    newkey = ingredient.key
    if ingredient.dtype == 'datapoints' or ingredient.dtype == 'concepts':
        for key in rm.keys():
            if key in ingredient.key:
                newkey = newkey.replace(key, rm[key])
    else:
        for key in rm.keys():
            if key in ingredient.key:
                newkey[ingredient.key.index(key)] = rm[key]

    if not result:
        result = ingredient.ingred_id + '-translated'
    return Ingredient(result, result, newkey, "*", data=data)


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


def copy(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """make copy of ingredient data, with new names"""
    dictionary = options['dictionary']
    data = ingredient.get_data()

    for k, v in dictionary.items():
        if isinstance(v, str):
            data[v] = data[k].rename(columns={k: v}).copy()
        else:
            for n in v:
                data[n] = data[k].rename(columns={k: n}).copy()

    if not result:
        result = ingredient.ingred_id + '_'
    return Ingredient(result, result, ingredient.key, "*", data=data)


def merge(*ingredients: List[Ingredient], result=None, **options):
    """the main merge function"""
    # all ingredients should have same dtype and index

    logging.debug("merge: " + str([i.ingred_id for i in ingredients]))

    # assert that dtype and key are same in all dataframe
    try:
        assert len(set([x.key for x in ingredients])) == 1
        assert len(set([x.dtype for x in ingredients])) == 1
    except (AssertionError, TypeError):
        log1 = "multiple dtype/key detected: \n"
        log2 = "\n".join(["{}: {}, {}".format(x.ingred_id, x.dtype, x.key) for x in ingredients])
        logging.warning(log1+log2)

    # get the dtype and index
    # FIXME: handle the index key better
    dtype = ingredients[0].dtype

    if dtype == 'datapoints':
        index_col = ingredients[0].key_to_list()
        newkey = ','.join(index_col)
    else:
        index_col = ingredients[0].key
        newkey = index_col

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
        result = 'all_data_merged_'+str(int(time.time() * 1000))

    return Ingredient(result, result, newkey, '*', data=res_all)


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
                    left[k] = left[k].drop_duplicates(subset=index_col, keep='last')
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
            res_data = {'concept': left_df.drop_duplicates(subset='concept', keep='last')}
        else:
            res_data = {'concept': right_df.drop_duplicates(subset='concept', keep='last')}
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
            elif np.issubdtype(type(val), np.number):
                queries.append("{} == {}".format(col, val))
            # TODO: support more query methods.
            else:
                raise ValueError("not supported in query: " + str(type(val)))

        query_string = ' and '.join(queries)

        # logging.debug('query sting: ' + query_string)

        df = df.query(query_string).copy()
        df = df.rename(columns={from_name: k})
        # drops all columns with unique contents. and update the key.
        newkey = ingredient.key
        keys = ingredient.key_to_list()
        for c in df.columns:
            if ingredient.dtype == 'datapoints':
                if c in v.keys() and len(df[c].unique()) > 1:
                    logging.debug("column {} have multiple values: {}".format(c, df[c].unique()))
                elif len(df[c].unique()) <= 1:
                    df = df.drop(c, axis=1)
                    if c in keys:
                        keys.remove(c)
                    newkey = ','.join(keys)
            else:
                raise NotImplementedError("filtering concept/entity")

        res[k] = df

    if not result:
        result = ingredient.ingred_id + '-filtered'
    # TODO: the ingredient key need to be dropped too.
    return Ingredient(result, result, newkey, '*', data=res)


def filter_item(ingredient: Ingredient, *, result: Optional[str]=None, **options) -> Ingredient:
    """filter item from the ingredient data dict"""
    data = ingredient.get_data()
    items = options.pop('items')

    try:
        data = dict([(k, data[k]) for k in items])
    except KeyError:
        logging.debug("keys in {}: {}".format(ingredient.ingred_id, str(list(data.keys()))))
        raise

    if not result:
        result = ingredient.ingred_id

    return Ingredient(result, result, ingredient.key, '*', data=data)


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

    if 'drop_not_found' not in options:
        drop_not_found = True
    else:
        drop_not_found = options['drop_not_found']

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

        if drop_not_found:
            # drop those entities not found in the mappings
            df_ = df[df[to_find].isin(mapping.keys())].copy()
        else:
            df_ = df.copy()

        for old, new in mapping.items():
            if not pd.isnull(new):
                df_.loc[df_[to_find] == old, to_replace] = new

        ing_data[k] = df_.dropna(how='any')

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

    try:
        agg = options.pop('aggregate')
    except KeyError:
        logging.warning("no aggregate function found, assuming sum()")
        agg = 'sum'

    for k, df in data.items():
        df = df.groupby(by=by).agg({k: agg})
        newkey = ','.join(df.index.names)
        data[k] = df.reset_index()

    if not result:
        result = ingredient.ingred_id + '-agg'
    return Ingredient(result, result, newkey, '*', data=data)


def accumulate(ingredient: Ingredient, *, result=None, **options) -> Ingredient:

    if ingredient.dtype != 'datapoints':
        raise ValueError("only datapoint support this function!")

    ops = options.pop('op')

    data = ingredient.get_data()
    index = ingredient.key_to_list()

    for k, func in ops.items():
        df = data[k]
        df = df.groupby(by=index).agg('sum')
        assert re.match('[a-z]+', func)  # only lower case chars allowed, for security
        df = eval("df.groupby(level=[0]).{}()".format(func))
        data[k] = df.reset_index()

    if not result:
        result = ingredient.ingred_id + '-accued'

    return Ingredient(result, result, ingredient.key, '*', data=data)


def run_op(ingredient: Ingredient, *, result=None, **options) -> Ingredient:

    assert ingredient.dtype == 'datapoints'

    data = ingredient.get_data()
    keys = ingredient.key_to_list()
    # TODO: load the op as ordered dict to speed up. (e.g I can create shared var at beginning)
    ops = options['op']

    # concat all the datapoint dataframe first, and eval the ops
    # TODO: concat() may be expansive. should find a way to improve.
    to_concat = [v.set_index(keys) for v in data.values()]
    try:
        df = pd.concat(to_concat, axis=1)
    except:
        for i, df in enumerate(to_concat):
            df.to_csv('tmp_'+str(i)+'.csv')
        raise

    for k, v in ops.items():
        data[k] = df.eval(v).dropna().reset_index(name=k)

    if not result:
        result = ingredient.ingred_id + '-op'
    return Ingredient(result, ingredient.ddf_id, ingredient.key, '*', data=data)
