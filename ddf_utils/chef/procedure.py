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

    Parameters
    ----------
    ingredient : BaseIngredient
        The ingredient to translate
    result : `str`
        The result ingredient id

    Keyword Args
    ------------
    dictionary: dict
        a dictionary of oldname -> newname mappings

    See Also
    --------
    :py:func:`ddf_utils.transformer.translate_header` : Related function in transformer module
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

    Keyword Args
    ------------
    dictionary: dict
        A dictionary of oldname -> newname mappings.
        If 'base' is provided in the dictionary, 'key' and 'value' should also in the dictionary.
        See :py:func:`ddf_utils.transformer.translate_column` for more on how this is handled.
    column: `str`
        the column to be translated
    target_column : `str`, optional
        the target column to store the translated data. If this is not set then the `column` cloumn will be replaced
    not_found : {'drop', 'include', 'error'}, optional
        the behavior when there is values not found in the mapping dictionary, default is 'drop'

    See Also
    --------
    :py:func:`ddf_utils.transformer.translate_column` : related function in transformer module
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
    """make copy of ingredient data columns, with new names.

    A dictionary should be provided, for example:

    .. code-block:: json

        {
            "col1": ["copy1_1", "copy1_2"],
            "col2": "copy2"
        }

    where 'col1' and 'col2' should be existing columns in the input ingredient

    Keyword Args
    ------------
    dictionary: dict
        a dictionary of oldname -> newname mappings

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
    """merge a list of ingredients

    The ingredients will be merged one by one in the order of how they are provided to this function.
    Later ones will overwrite the pervious merged results.

    Parameters
    ----------
    BaseIngredient
        Any numbers of ingredients to be merged

    Keyword Args
    ------------
    deep: `bool`, optional
        if True, then do deep merging. Default is False

    Notes
    -----
    **deep merge** is when we check every datapoint for existence
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
    """get the last vaild item of a Series, or Nan."""
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

    Keyword Args
    ------------
    copy: bool, optional
        if copy is True, then treat all data as string. Default: False
    """
    if 'copy' in options and options['copy'] is True:
        ingredient.data = ingredient.get_data(copy=True)
    else:
        ingredient.data = ingredient.get_data()

    if result:
        ingredient.ingred_id = result + '-identity'
    return ingredient


def filter_row(ingredient: BaseIngredient, *, result=None, **options) -> ProcedureResult:
    """filter an ingredient based on a set of options and return
    the result as new ingredient.

    A dictionary should be provided in options with the following format:

    .. code-block:: yaml

        dictionary:
          new_key_in_new_ingredient:
            from: old_key_in_old_ingredient
            filter_col_1: filter_val_1
            filter_col_2: filter_val_2

    See a detail example in this `github issue <https://github.com/semio/ddf_utils/issues/2#issuecomment-254132615>`_

    Parameters
    ----------
    ingredient: BaseIngredient
    result: `str`

    Keyword Args
    ------------
    dictionary: dict
        The filter description dictionary
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
    """filter items from the ingredient data

    Keyword Args
    ------------
    items: list
        a list of items to filter from base ingredient
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


def groupby(ingredient: BaseIngredient, *, result, **options) -> ProcedureResult:
    """group ingredient data by column(s) and run aggregate function

    Keyword Args
    ------------
    groubby : `str` or `list`
        the column(s) to group, can be a list or a string
    aggregate/transform/filter : `dict`
        the function to run. only one of `aggregate`, `transform` and `filter` should be supplied.

    Examples
    --------

    The function block should have below format:

    .. highlight:: yaml

    ::

      aggregate:
        column1: func_name1
        column2: func_name2

    or

    ::

        aggrgrate:
          column1:
            function: func_name
            param1: foo
            param2: baz

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

    An window object should be provided in options, with following parameters:

    - `column`: str, column which window is created from
    - `size`: int or 'expanding', if int then rolling window, if expanding then expanding window
    - `min_periods`: int, as in pandas
    - `center`: bool, as in pandas

    Keyword Args
    ------------
    window: dict
        window definition.
    aggregate: dictionary
        aggregation functions, format should be
        ``column: func`` or ``column: {function: func, param1: foo, param2: baz, ...}``

    Examples
    --------

    An example of rolling windows:

    .. highlight:: yaml

    ::

        procedure: window
        ingredients:
            - ingredient_to_roll
        result: new_ingredient_id
        options:
          window:
            column: year
            size: 10
            min_periods: 1
            center: false
          aggregate:
            column_to_aggregate: sum

    Notes
    -----
    Any column not mentioned in the `aggregate` block will be dropped in the returned ingredient.
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


def run_op(ingredient: BaseIngredient, *, result=None, **options) -> ProcedureResult:
    """run math operation on each row of ingredient data.

    Keyword Args
    ------------
    op: dict
        a dictionary of concept_name -> function mapping

    Examples
    --------
    .. highlight:: yaml

    for exmaple, if we want to add 2 columns, col_a and col_b, to create an new column, we can write

    ::

        procedure: run_op
        ingredients:
          - ingredient_to_run
        result: new_ingredient_id
        options:
          op:
            new_col_name: "col_a + col_b"
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

    Parameters
    ----------
    ingredients
        any numbers of ingredient that needs to extract concepts from

    Keyword Args
    ------------
    join : dict, optional
        the base ingredient to join

    Examples
    --------

    .. highlight:: yaml

    ::

        - procedure: extract_concepts
          ingredients: ["foo","bar"]
          result: concepts_final
          options:
              join:
                  base: concept_ingredient_id
                  type: full_outer || ingredients_outer # default full_outer

    See Also
    --------
    :py:func:`ddf_utils.transformer.extract_concepts` : related function in transformer
    module
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

    (Not Implemented Yet)

    See Also
    --------
    :py:func:`ddf_utils.transformer.trend_bridge` : related function in transformer module
    """
    from ..transformer import trend_bridge as tb

    raise NotImplementedError('')
