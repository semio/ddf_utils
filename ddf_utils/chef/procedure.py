# -*- coding: utf-8 -*-

"""all procedures for recipes"""

import pandas as pd
import numpy as np
from . ingredient import Ingredient
from . import config
import time
from typing import List, Union, Dict, Optional
import re

import logging


def translate_header(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """Translate column headers

    available options are:
        `dictionary`: a dictionary of oldname -> newname mappings
    """
    logging.info("translate_header: " + ingredient.ingred_id)

    rm = options['dictionary']
    data = ingredient.copy_data()

    for k in list(data.keys()):
        if k in rm.keys():  # if we need to rename the concept name
            data[rm[k]] = data[k].rename(columns=rm).copy()
            del(data[k])
        else:  # we only rename index/properties columns
            data[k] = data[k].rename(columns=rm)

    # also rename the key
    newkey = ingredient.key
    if ingredient.dtype in ['datapoints', 'concepts']:
        for key in rm.keys():
            if key in ingredient.key:
                newkey = newkey.replace(key, rm[key])
    else:
        for key in rm.keys():
            if key in ingredient.key:
                newkey[ingredient.key.index(key)] = rm[key]

    if not result:
        result = ingredient.ingred_id + '-translated'
    return Ingredient(result, ingredient.ddf_id, newkey, "", data=data)


def translate_column(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """Translate column values.

    available options are:
        `dictionary`: a dictionary of oldname -> newname mappings
        `column`: the column to rename
        `base`: if base is provided, transform the columns base on information of base ingredient.
        So that oldname column will be change to values from newname column.

    Note:
        if base and column are provided at same time, it will raise an error.
    """
    logging.info("translate_column: " + ingredient.ingred_id)

    if 'column' in options.keys() and 'base' in options.keys():
        raise ValueError("only accept column or base option, not both")
    try:
        column = options['column']
        rm = options['dictionary']
    except KeyError:
        base_dict = options['base'].get_data()
        assert len(options['dictionary']) == 1
        assert len(base_dict) == 1
        _, base_df = base_dict.popitem()
        k, v = options['dictionary'].popitem()
        column = k
        rm = base_df.set_index(k)[v].to_dict()

    di = ingredient.copy_data()

    for k, df in di.items():

        df = df.set_index(column)
        di[k] = df.rename(index=rm).reset_index()

    if not result:
        result = ingredient.ingred_id + '-translated'
    return Ingredient(result, ingredient.ddf_id, ingredient.key, "", data=di)


def copy(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """make copy of ingredient data, with new names.

    available options:
        `dictionary`: a dictionary of oldname -> newname mappings
    """
    logging.info("copy: " + ingredient.ingred_id)

    dictionary = options['dictionary']
    data = ingredient.copy_data()

    for k, v in dictionary.items():
        if isinstance(v, str):  # value is str, means only make one copy
            data[v] = data[k].rename(columns={k: v}).copy()
        else:  # then it's a list, should make multiple copy
            for n in v:
                data[n] = data[k].rename(columns={k: n}).copy()

    # usually the old ingredient won't be used after creating copy.
    # just reset the data to save memory
    ingredient.reset_data()
    if not result:
        result = ingredient.ingred_id + '_'
    return Ingredient(result, ingredient.ddf_id, ingredient.key, "", data=data)


def merge(*ingredients: List[Ingredient], result=None, **options):
    """the main merge function

    avaliable options:
        deep: if True, then do deep merging. Default is False

    About deep merging:
        deep merge is when we check every datapoint for existence
        if false, overwrite is on the file level. If key-value
        (e.g. geo,year-population_total) exists, whole file gets overwritten
        if true, overwrite is on the row level. If values
        (e.g. afr,2015-population_total) exists, it gets overwritten, if it doesn't it stays
    """
    logging.info("merge: " + str([i.ingred_id for i in ingredients]))

    # assert that dtype and key are same in all dataframe
    try:
        assert len(set([x.key for x in ingredients])) == 1
        assert len(set([x.dtype for x in ingredients])) == 1
    except (AssertionError, TypeError):
        log1 = "multiple dtype/key detected: \n"
        log2 = "\n".join(["{}: {}, {}".format(x.ingred_id, x.dtype, x.key) for x in ingredients])
        logging.warning(log1+log2)
        raise ValueError("can't merge data with multiple dtype/key!")

    # get the dtype and index
    # we have assert dtype and key is unique, so we take it from
    # the first ingredient
    dtype = ingredients[0].dtype

    if dtype == 'datapoints':
        index_col = ingredients[0].key_to_list()
        newkey = ','.join(index_col)
    else:
        index_col = ingredients[0].key
        newkey = index_col

    if 'deep' in options.keys():
        deep = options.pop('deep')
    else:
        deep = False
    if deep:
        logging.info("merge: doing deep merge")
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
    """merge 2 ingredient data."""
    if len(left) == 0:
        return right

    if dtype == 'datapoints':
        if deep:
            for k, df in right.items():
                if k in left.keys():
                    left[k] = left[k].append(df)
                    left[k] = left[k].drop_duplicates(subset=index_col, keep='last')
                    left[k] = left[k].sort_values(by=index_col)
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


def identity(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """return the ingredient as is.

    available options:
        copy: if copy is True, then treat all data as string. Default: False
    """
    if 'copy' in options and options['copy'] is True:
        ingredient.data = ingredient.get_data_str()
    else:
        ingredient.data = ingredient.get_data()

    if result:
        ingredient.ingred_id = result
    return ingredient


def filter_row(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """filter an ingredient based on a set of options and return
    the result as new ingredient.

    Args:
        ingredient: Ingredient object
        result: ingred_id of return ingredient
        options: dict of options

    available options:
        dictionary: test
    """

    logging.info("filter_row: " + ingredient.ingred_id)

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
    return Ingredient(result, ingredient.ingred_id, newkey, '*', data=res)


def filter_item(ingredient: Ingredient, *, result: Optional[str]=None, **options) -> Ingredient:
    """filter item from the ingredient data dict.

    available options:
        items: a list of items to filter from base ingredient
    """
    logging.info("filter_item: " + ingredient.ingred_id)

    data = ingredient.get_data()
    items = options.pop('items')

    try:
        data = dict([(k, data[k]) for k in items])
    except KeyError:
        logging.debug("keys in {}: {}".format(ingredient.ingred_id, str(list(data.keys()))))
        raise

    if not result:
        result = ingredient.ingred_id

    return Ingredient(result, ingredient.ddf_id, ingredient.key, '', data=data)


def align(to_align: Ingredient, base: Ingredient, *, result=None, **options) -> Ingredient:
    """align 2 ingredient by a column.

    This function is like an automatic version of translate_column.
    It firstly creating the mapping dictionary by searching in the base
    ingredient for data in to_align ingredient, and then translate
    according to the mapping file.

    available options:
        `search_cols`: a list of columns of base ingredient, to search for values
        `to_find`: the column of ingredient to_align. The function will search the data
        of this column in search_cols
        `to_replace`: the column of ingredient to replace with new value. can be same
        as to_find or a new column
        `drop_not_found`: drop those entities not found in the base
    """
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
        logging.critical(base.get_data().keys())
        raise NotImplementedError('align to base data with multiple dataframes is not supported yet.')

    logging.info("aligning: {} with {}".format(to_align.ingred_id, base.ingred_id))

    base_data = list(base.get_data().values())[0]
    ing_data = to_align.get_data()

    base_data = base_data.set_index(base.key)

    mapping = {}
    no_match = []

    for k, df in ing_data.items():
        for f in df[to_find].unique():
            if f in mapping:
                continue

            # filtering name in all search_cols
            bools = []
            for sc in search_cols:
                bools.append(base_data[sc] == f)
            mask = bools[0]
            for m in bools[1:]:
                mask = mask | m
            filtered = base_data[mask]

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
                df_.at[df_[to_find] == old, to_replace] = new

        ing_data[k] = df_

    if len(no_match) > 0:
        logging.warning("no match found for: " + str(set(no_match)))

    if not result:
        result = to_align.ingred_id + '-aligned'
    if to_align.dtype == 'datapoints':
        newkey = to_align.key.replace(to_find, to_replace)
        return Ingredient(result, result, newkey, '', data=ing_data)
    else:
        return Ingredient(result, to_align.ddf_id, to_replace, '', data=ing_data)


def groupby(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """group ingredient data by column(s) and run aggregate function

    available options:
        by: the column(s) to group, can be a list or a string
        aggregate: the function to aggregate. Default: sum
    """

    data = ingredient.get_data()
    by = options.pop('by')

    logging.info("groupby: " + ingredient.ingred_id)

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
    return Ingredient(result, ingredient.ddf_id, newkey, '', data=data)


def accumulate(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """run accumulate function on ingredient data.

    available options:
        op: a dictionary of concept_name: function mapping
    """

    logging.info("accumulate: " + ingredient.ingred_id)
    if ingredient.dtype != 'datapoints':
        raise ValueError("only datapoint support this function!")

    ops = options.pop('op')

    data = ingredient.get_data()
    index = ingredient.key_to_list()

    funcs = {
        'aagr': _aagr
    }

    for k, func in ops.items():
        df = data[k]
        df = df.groupby(by=index).agg('sum')
        assert re.match('[a-z_]+', func)  # only lower case chars allowed, for security
        # assuming level0 index is geo
        # because we should run accumulate for each country
        # TODO: https://github.com/semio/ddf_utils/issues/25
        if func in funcs:
            df = df.groupby(level=0, as_index=False).apply(funcs[func])
            df = df.reset_index()
            df = df[index + [k]]
        else:
            df = eval("df.groupby(level=0).{}()".format(func))
            df = df.reset_index()

        data[k] = df

    if not result:
        result = ingredient.ingred_id + '-accued'

    return Ingredient(result, ingredient.ddf_id, ingredient.key, '', data=data)


def _aagr(df: pd.DataFrame, window: int=10):
    """average annual growth rate"""
    pct = df.pct_change()
    return pct.rolling(window).apply(np.mean).dropna()


def run_op(ingredient: Ingredient, *, result=None, **options) -> Ingredient:
    """run math operation on each row of ingredient data.

    available options:
        op: a dictionary of concept_name: function mapping
    """

    assert ingredient.dtype == 'datapoints'
    logging.info("run_op: " + ingredient.ingred_id)

    data = ingredient.get_data()
    keys = ingredient.key_to_list()
    ops = options['op']

    # concat all the datapoint dataframe first, and eval the ops
    to_concat = [v.set_index(keys) for v in data.values()]
    df = pd.concat(to_concat, axis=1)

    for k, v in ops.items():
        res = df.eval(v).dropna()  # type(res) is Series
        res.name = k
        if k not in df.columns:
            df[k] = res
        data[k] = res.reset_index()

    if not result:
        result = ingredient.ingred_id + '-op'
    return Ingredient(result, ingredient.ddf_id, ingredient.key, '', data=data)
