# -*- coding: utf-8 -*-

"""all procedures for recipes"""

import pandas as pd
import numpy as np
from . ingredient import BaseIngredient, Ingredient, ProcedureResult
from .helpers import read_opt, mkfunc
from .. import config
from .. import transformer
import time
from typing import List, Union, Dict, Optional
import re

import logging

logger = logging.getLogger('Chef')


def translate_header(ingredient: BaseIngredient, *, result=None, **options) -> ProcedureResult:
    """Translate column headers

    available options are:
        `dictionary`: a dictionary of oldname -> newname mappings
    """
    logger.info("translate_header: " + ingredient.ingred_id)

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
    return ProcedureResult(result, newkey, data=data)


def translate_column(ingredient: BaseIngredient, *, result=None, **options) -> ProcedureResult:
    """Translate column values.

    available options are:
        `dictionary`: a dictionary of oldname -> newname mappings
        `column`: the column to rename
        `base`: if base is provided, transform the columns base on information of base ingredient.
        So that oldname column will be change to values from newname column.

    Note:
        if base and column are provided at same time, it will raise an error.
    """
    logger.info("translate_column: " + ingredient.ingred_id)

    from ..transformer import translate_column as tc

    di = ingredient.copy_data()

    # reading options
    column = read_opt(options, 'column', required=True)
    target_column = read_opt(options, 'target_column')
    not_found = read_opt(options, 'not_found', default='drop')
    dictionary = read_opt(options, 'dictionary', required=True)

    # find out the type of dictionary.
    if isinstance(dictionary, str):
        dict_type = 'file'
        base_df = None
    else:
        if 'base' in dictionary.keys():
            dict_type = 'dataframe'
            base = dictionary.pop('base')
            base_data = base.get_data()
            if len(base_data) > 1:
                raise ValueError('only support ingredient with 1 item')
            base_df = list(base_data.values())[0]
        else:
            dict_type = 'inline'
            base_df = None

    for k, df in di.items():
        logger.debug("running on: " + k)
        di[k] = tc(df, column, dict_type, dictionary, target_column, base_df, not_found)

    if not result:
        result = ingredient.ingred_id + '-translated'
    return ProcedureResult(result, ingredient.key, data=di)


def copy(ingredient: BaseIngredient, *, result=None, **options) -> ProcedureResult:
    """make copy of ingredient data, with new names.

    available options:
        `dictionary`: a dictionary of oldname -> newname mappings
    """
    logger.info("copy: " + ingredient.ingred_id)

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
    return ProcedureResult(result, ingredient.key, data=data)


def merge(*ingredients: List[BaseIngredient], result=None, **options) -> ProcedureResult:
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
    logger.info("merge: " + str([i.ingred_id for i in ingredients]))

    # assert that dtype and key are same in all dataframe
    try:
        assert len(set([x.key for x in ingredients])) == 1
        assert len(set([x.dtype for x in ingredients])) == 1
    except (AssertionError, TypeError):
        log1 = "multiple dtype/key detected: \n"
        log2 = "\n".join(["{}: {}, {}".format(x.ingred_id, x.dtype, x.key) for x in ingredients])
        logger.warning(log1+log2)
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
        logger.info("merge: doing deep merge")
    # merge data from ingredients one by one.
    res_all = {}

    for i in ingredients:
        res_all = _merge_two(res_all, i.get_data(), index_col, dtype, deep)

    if not result:
        result = 'all_data_merged_'+str(int(time.time() * 1000))

    return ProcedureResult(result, newkey, data=res_all)


def __get_last_item(ser):
    if ser.last_valid_index() is None:
        return np.nan
    else:
        return ser[ser.last_valid_index()]


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
                    left[k] = left[k].append(df, ignore_index=True)
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
            merged = left_df.append(right_df, ignore_index=True)
            res = merged.groupby(index_col).agg(__get_last_item)
            res_data = {'concept': res.reset_index()}
        else:
            res_data = {'concept': right_df.drop_duplicates(subset='concept', keep='last')}
    else:
        # TODO
        raise NotImplementedError('entity data do not support merging yet.')

    return res_data


def identity(ingredient: BaseIngredient, *, result=None, **options) -> BaseIngredient:
    """return the ingredient as is.

    available options:
        copy: if copy is True, then treat all data as string. Default: False
    """
    if 'copy' in options and options['copy'] is True:
        ingredient.data = ingredient.get_data(copy=True)
    else:
        ingredient.data = ingredient.get_data()

    if result:
        ingredient.ingred_id = result
    return ingredient


def filter_row(ingredient: BaseIngredient, *, result=None, **options) -> ProcedureResult:
    """filter an ingredient based on a set of options and return
    the result as new ingredient.

    Args:
        ingredient: Ingredient object
        result: ingred_id of return ingredient
        options: dict of options

    available options:
        dictionary: test
    """

    logger.info("filter_row: " + ingredient.ingred_id)

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
                    logger.debug("column {} have multiple values: {}".format(c, df[c].unique()))
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
    return ProcedureResult(result, newkey, data=res)


def filter_item(ingredient: BaseIngredient, *, result: Optional[str]=None, **options) -> ProcedureResult:
    """filter item from the ingredient data dict.

    available options:
        items: a list of items to filter from base ingredient
    """
    logger.info("filter_item: " + ingredient.ingred_id)

    data = ingredient.get_data()
    items = options.pop('items')

    try:
        data = dict([(k, data[k]) for k in items])
    except KeyError:
        logger.debug("keys in {}: {}".format(ingredient.ingred_id, str(list(data.keys()))))
        raise

    if not result:
        result = ingredient.ingred_id

    return ProcedureResult(result, ingredient.key, data=data)


def align(to_align: BaseIngredient, base: Ingredient, *, result=None, **options) -> ProcedureResult:
    """align 2 ingredient by a column.

    This function is like an automatic version of translate_column.
    It firstly creating the mapping dictionary by searching in the base
    ingredient for data in to_align ingredient, and then translate
    according to the mapping file.

    available options:
        `search_cols`: a list of columns of base ingredient, to search for values
        `to_find`: the column of ingredient to_align. The function will search the data
        of this column in search_cols
        `to_replace`: the column of to_align ingredient to replace with new value.
        can be same as to_find or a new column
        `drop_not_found`: if we should drop those entities not found in the base
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
        logger.critical(base.get_data().keys())
        raise NotImplementedError('align to base data with multiple dataframes is not supported yet.')

    logger.info("aligning: {} with {}".format(to_align.ingred_id, base.ingred_id))

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
                logger.warning("multiple match found: "+f)
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
        logger.warning("no match found for: " + str(set(no_match)))

    if not result:
        result = to_align.ingred_id + '-aligned'
    if to_align.dtype == 'datapoints':
        newkey = to_align.key.replace(to_find, to_replace)
        return ProcedureResult(result, newkey, data=ing_data)
    else:
        return ProcedureResult(result, to_replace, data=ing_data)


def groupby(ingredient: BaseIngredient, *, result, **options) -> ProcedureResult:
    """group ingredient data by column(s) and run aggregate function

    available options:
        groubby: the column(s) to group, can be a list or a string
        aggregate/transform/filter: the function to run. only one of them should be supplied.

    The function block should have below format:

    aggregate:
      column1: funcname1
      column2: funcname2

    or

    aggregate:
      column:
        function: funcname
        param1: foo
        param2: bar

    other columns not mentioned will be dropped.
    """

    logger.info("groupby: " + ingredient.ingred_id)

    data = ingredient.get_data()
    by = options.pop('groupby')

    # only one of aggregate/transform/filter should be in options.
    assert len(list(options.keys())) == 1
    comp_type = list(options.keys())[0]
    assert comp_type in ['aggregate', 'transform', 'filter']

    if comp_type == 'aggregate':  # only aggregate should change the key of ingredient
        if isinstance(by, list):
            newkey = ','.join(by)
        else:
            newkey = by
            by = [by]
        logger.debug("changing the key to: " + str(newkey))
    else:
        newkey = ingredient.key
        by = [by]

    newdata = dict()

    # TODO: support apply function to all items?
    if comp_type == 'aggregate':
        for k, func in options[comp_type].items():
            func = mkfunc(func)
            newdata[k] = data[k].groupby(by=by).agg({k: func}).reset_index()
    if comp_type == 'transform':
        for k, func in options[comp_type].items():
            func = mkfunc(func)
            df = data[k].set_index(ingredient.key_to_list())
            levels = [df.index.names.index(x) for x in by]
            newdata[k] = df.groupby(level=levels)[k].transform(func).reset_index()
    if comp_type == 'filter':
        for k, func in options[comp_type].items():
            func = mkfunc(func)
            df = data[k].set_index(ingredient.key_to_list())
            levels = [df.index.names.index(x) for x in by]
            newdata[k] = df.groupby(level=levels)[k].filter(func).reset_index()

    return ProcedureResult(result, newkey, data=newdata)


def window(ingredient: BaseIngredient, result, **options) -> ProcedureResult:
    """apply functions on a rolling window

    available options:
    window: dictionary
        window definition. options are:
        column: str, column which window is created from
        size: int or 'expanding', if int then rolling window, if expanding then expanding window
        min_periods: int, as in pandas
        center: bool, as in pandas
    aggregate: dictionary
        aggregation functions, format should be
        column: func or column: {function: func, param1: foo, param2: baz, ...}
    """

    logger.info('window: ' + ingredient.ingred_id)

    # reading options
    window = options.pop('window')
    aggregate = options.pop('aggregate')

    column = read_opt(window, 'column', required=True)
    size = read_opt(window, 'size', required=True)
    min_periods = read_opt(window, 'min_periods', default=None)
    center = read_opt(window, 'center', default=False)

    data = ingredient.get_data()
    newdata = dict()

    for k, func in aggregate.items():
        f = mkfunc(func)
        # keys for grouping. in multidimensional data like datapoints, we want create
        # groups before rolling. Just group all key column except the column to aggregate.
        keys = ingredient.key_to_list()
        keys.remove(column)
        df = data[k].set_index(ingredient.key_to_list())
        levels = [df.index.names.index(x) for x in keys]
        if size == 'expanding':
            newdata[k] = (df.groupby(level=levels, group_keys=False)
                          .expanding(on=column, min_periods=min_periods, center=center)
                          .agg(func).reset_index().dropna())
        else:
            # There is a bug when running rolling on with groupby in pandas.
            # see https://github.com/pandas-dev/pandas/issues/13966
            # We will implement this later when we found work around or it's fixed
            # for now, we don't use the `on` parameter in rolling.
            # FIXME: add back the `on` parameter.
            newdata[k] = (df.groupby(level=levels, group_keys=False)
                          .rolling(window=size, min_periods=min_periods, center=center)
                          .agg(func).reset_index().dropna())
    return ProcedureResult(result, ingredient.key, newdata)

def accumulate(ingredient: BaseIngredient, *, result=None, **options) -> ProcedureResult:
    """run accumulate function on ingredient data.

    available options:
        op: a dictionary of concept_name: function mapping
    """

    logger.info("accumulate: " + ingredient.ingred_id)
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

    return ProcedureResult(result, ingredient.key, data=data)


def _aagr(df: pd.DataFrame, window: int=10):
    """average annual growth rate"""
    pct = df.pct_change()
    return pct.rolling(window).apply(np.mean).dropna()


def run_op(ingredient: BaseIngredient, *, result=None, **options) -> ProcedureResult:
    """run math operation on each row of ingredient data.

    available options:
        op: a dictionary of concept_name: function mapping
    """

    assert ingredient.dtype == 'datapoints'
    logger.info("run_op: " + ingredient.ingred_id)

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
    return ProcedureResult(result, ingredient.key, data=data)


def extract_concepts(*ingredients: List[BaseIngredient],
                     result=None, **options) -> ProcedureResult:
    """extract concepts from other ingredients.
    """
    if options:
        base = options['join']['base']
        try:
            join = options['join']['type']
        except KeyError:
            join = 'full_outer'
        concepts = base.get_data()['concepts'].set_index('concept')
    else:
        concepts = pd.DataFrame([], columns=['concept', 'concept_type']).set_index('concept')

    new_concepts = set()

    for i in ingredients:
        data = i.get_data()
        for k, df in data.items():
            # TODO: add logic for concepts/entities ingredients
            new_concepts.add(k)
            if k in concepts.index:
                continue
            if np.issubdtype(df[k].dtype, np.number):
                concepts.ix[k, 'concept_type'] = 'measure'
            else:
                concepts.ix[k, 'concept_type'] = 'string'
    if join == 'ingredients_outer':
        # ingredients_outer join: only keep concepts appears in ingredients
        concepts = concepts.ix[new_concepts]
    if not result:
        result = 'concepts_extracted'
    return ProcedureResult(result, 'concept', data=concepts.reset_index())


def trend_bridge(ingredient: BaseIngredient, result, **options) -> ProcedureResult:
    """run trend bridge on ingredients
    """
    from ..transformer import trend_bridge as tb

    raise NotImplementedError('')
