# -*- coding: utf-8 -*-

"""all procedures for recipes"""

import fnmatch
import logging
import time
import warnings
from collections import Mapping, Sequence
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from ddf_utils.chef.cook import Chef

from .dag import DAG
from .exceptions import ProcedureError
from .helpers import debuggable, mkfunc, query, read_opt, create_dsk
from .ingredient import BaseIngredient, ProcedureResult

logger = logging.getLogger('Chef')


@debuggable
def translate_header(chef: Chef, ingredients: List[str], result, dictionary) -> ProcedureResult:
    """Translate column headers

    Procedure format:

    .. code-block:: yaml

       procedure: translate_header
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         dictionary: str or dict  # file name or mappings dictionary

    Parameters
    ----------
    chef : Chef
        The Chef the procedure will run on
    ingredients : list
        A list of ingredient id in the dag to translate
    dictionary : dict
        A dictionary for name mapping
    result : `str`
        The result ingredient id

    See Also
    --------
    :py:func:`ddf_utils.transformer.translate_header` : Related function in transformer module
    """
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    ingredient = ingredients[0]
    logger.info("translate_header: " + ingredient.ingred_id)

    data = ingredient.get_data()
    rm = dictionary

    for k in list(data.keys()):
        df_new = data[k].rename(columns=rm)
        if ingredient.dtype == 'entities':  # also rename the `is--` headers
            rm_ = {}
            for c in df_new.columns:
                if k == c[4:]:
                    rm_[c] = 'is--' + rm[k]
            if len(rm_) > 0:
                df_new = df_new.rename(columns=rm_)
        if k in rm.keys():  # if we need to rename the concept name
            data[rm[k]] = df_new
            del(data[k])
        else:  # we only rename index/properties columns
            data[k] = df_new

    # also rename the key
    newkey = ingredient.key
    if ingredient.dtype == 'datapoints':
        for key in rm.keys():
            if key in ingredient.key:
                newkey = newkey.replace(key, rm[key])
    elif ingredient.dtype == 'entities':
        for key in rm.keys():
            if key == ingredient.key:
                newkey = rm[key]
    else:
        if 'concept' in rm.keys():
            raise ValueError('can translate the primaryKey for concept!')

    if not result:
        result = ingredient.ingred_id + '-translated'
    return ProcedureResult(chef, result, newkey, data=data)


@debuggable
def translate_column(chef: Chef, ingredients: List[str], result, dictionary,
                     column, *, target_column=None, not_found='drop',
                     ambiguity='prompt', ignore_case=False) -> ProcedureResult:
    """Translate column values.

    Procedure format:

    .. code-block:: yaml

       procedure: translate_column
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         column: str  # the column to be translated
         target_column: str  # optional, the target column to store the translated data
         not_found: {'drop', 'include', 'error'}  # optional, the behavior when there is values not
                                                  # found in the mapping dictionary, default is 'drop'
         ambiguity: {'prompt', 'skip', 'error'}  # optional, the behavior when there is ambiguity
                                                 # in the dictionary
         dictionary: str or dict  # file name or mappings dictionary

    If base is provided in dictionary, key and value should also in dictionary.
    In this case chef will generate a mapping dictionary using the base ingredient.
    The dictionary format will be:

    .. code-block:: yaml

       dictionary:
         base: str  # ingredient name
         key: str or list  # the columns to be the keys of the dictionary, can accept a list
         value: str  # the column to be the values of the the dictionary, must be one column

    Parameters
    ----------
    chef : Chef
        The Chef the procedure will run on
    ingredients : list
        A list of ingredient id in the dag to translate

    Keyword Args
    ------------
    dictionary: dict
        A dictionary of oldname -> newname mappings.
        If 'base' is provided in the dictionary, 'key' and 'value' should also in the dictionary.
        See :py:func:`ddf_utils.transformer.translate_column` for more on how this is handled.
    column: `str`
        the column to be translated
    target_column : `str`, optional
        the target column to store the translated data. If this is not set then the `column`
        column will be replaced
    not_found : {'drop', 'include', 'error'}, optional
        the behavior when there is values not found in the mapping dictionary, default is 'drop'
    ambiguity : {'prompt', 'skip', 'error'}, optional
        the behavior when there is ambiguity in the dictionary, default is 'prompt'

    See Also
    --------
    :py:func:`ddf_utils.transformer.translate_column` : related function in transformer module
    """
    from ..transformer import translate_column as tc
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."

    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info("translate_column: " + ingredient.ingred_id)

    di = ingredient.get_data()
    new_data = dict()

    # find out the type of dictionary.
    if isinstance(dictionary, str):
        dict_type = 'file'
        base_df = None
    else:
        if 'base' in dictionary.keys():
            dict_type = 'dataframe'
            base = chef.dag.get_node(dictionary.pop('base')).evaluate()
            base_data = base.get_data()
            if len(base_data) > 1:
                raise ProcedureError('only support ingredient with 1 item')
            base_df = list(base_data.values())[0].copy()
        else:
            dict_type = 'inline'
            base_df = None

    for k, df in di.items():
        logger.debug("running on: " + k)
        new_data[k] = tc(df, column, dict_type, dictionary, target_column, base_df,
                         not_found, ambiguity, ignore_case)

    if not result:
        result = ingredient.ingred_id + '-translated'
    return ProcedureResult(chef, result, ingredient.key, data=new_data)


@debuggable
def merge(chef: Chef, ingredients: List[str], result, deep=False) -> ProcedureResult:
    """merge a list of ingredients

    The ingredients will be merged one by one in the order of how they are provided to this
    function. Later ones will overwrite the previous merged results.

    Procedure format:

    .. code-block:: yaml

       procedure: merge
       ingredients:  # list of ingredient id
         - ingredient_id_1
         - ingredient_id_2
         - ingredient_id_3
         # ...
       result: str  # new ingredient id
       options:
         deep: bool  # use deep merge if true

    Parameters
    ----------
    chef: Chef
        a Chef instance
    ingredients:
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
    # ingredients = [chef.dag.get_node(x).evaluate() for x in ingredients]
    logger.info("merge: " + str([i.ingred_id for i in ingredients]))

    # assert that dtype and key are same in all dataframe
    try:
        for x in ingredients[1:]:
            assert set(x.key_to_list()) == set(ingredients[0].key_to_list())
        assert len(set([x.dtype for x in ingredients])) == 1
    except (AssertionError, TypeError):
        log1 = "multiple dtype/key detected: \n"
        log2 = "\n".join(["{}: {}, {}".format(x.ingred_id, x.dtype, x.key) for x in ingredients])
        logger.warning(log1+log2)
        raise ProcedureError("can't merge data with multiple dtype/key!")

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

    if deep:
        logger.info("merge: doing deep merge")
    # merge data from ingredients one by one.
    res_all = {}

    for i in ingredients:
        res_all = _merge_two(res_all, i.compute(), index_col, dtype, deep)

    if not result:
        result = 'all_data_merged_'+str(int(time.time() * 1000))

    return ProcedureResult(chef, result, newkey, data=create_dsk(res_all))


def __get_last_item(ser):
    """get the last valid item of a Series, or Nan."""
    ser_ = ser.dropna()
    if ser_.last_valid_index() is None:
        return np.nan
    else:
        return ser_[ser_.last_valid_index()]


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
    else:  # entities
        if deep:
            for k, df in right.items():
                if k in left.keys():
                    left[k] = left[k].append(df, ignore_index=True)
                    left[k] = left[k].groupby(index_col).agg(__get_last_item).reset_index()
                else:
                    left[k] = df
        else:
            for k, df in right.items():
                left[k] = df
        res_data = left
        # raise NotImplementedError('entity data do not support merging yet.')

    return res_data


@debuggable
def filter(chef: Chef, ingredients: List[str], result, **options) -> ProcedureResult:
    """filter items and rows just as what `value` and `filter` do in ingredient definition.

    Procedure format:

    .. code-block:: yaml

       - procedure: filter
         ingredients:
             - ingredient_id
         options:
             item:  # just as `value` in ingredient def
                 $in:
                     - concept_1
                     - concept_2
             row:  # just as `filter` in ingredient def
                 $and:
                     geo:
                         $ne: usa
                     year:
                         $gt: 2010

         result: output_ingredient

    for more information, see the :py:class:`ddf_utils.chef.ingredient.Ingredient` class.

    Parameters
    ----------
    chef: Chef
        the Chef instance
    ingredients:
        list of ingredient id in the DAG
    result: `str`

    Keyword Args
    ------------
    item: list or dict, optional
        The item filter
    row: dict, optional
        The row filter
    """

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info("filter_row: " + ingredient.ingred_id)

    data = ingredient.get_data()
    row_filters = read_opt(options, 'row', False, None)
    items = read_opt(options, 'item', False, None)

    res = {}

    if row_filters is None and items is None:
        raise ProcedureError('filter procedure: at least one of `row` and `item` should be set in the options!')

    if items is not None:
        if isinstance(items, Sequence):
            if ingredient.dtype == 'datapoints':
                for i in items:
                    if i in data.keys():
                        res[i] = data[i].copy()
                    else:
                        logger.warning("concept {} not found in ingredient {}".format(i, ingredient.ingred_id))
            else:
                for k, v in data.items():
                    items_ = items.copy()
                    for i in items_:
                        if i not in v.columns:
                            items_.remove(i)
                            logger.warning("concept {} not found in ingredient {}".format(i, ingredient.ingred_id))
                    res[k] = v[items_].copy()
        else:
            assert len(items) == 1
            assert list(items.keys())[0] in ['$in', '$nin']
            selector = list(items.keys())[0]
            item_list = list(items.values())[0]
            if ingredient.dtype == 'datapoints':
                if selector == '$in':
                    for i in item_list:
                        if i in data.keys():
                            res[i] = data[i].copy()
                        else:
                            logger.warning("concept {} not found in ingredient {}".format(i, ingredient.ingred_id))
                else:
                    for k, df in data.items():
                        if k not in item_list:
                            res[k] = data[k].copy()
            else:
                if selector == '$in':
                    items_ = item_list.copy()
                    for k, v in data.items():
                        for i in items_:
                            if i not in v.columns:
                                items_.remove(i)
                                logger.warning("concept {} not found in ingredient {}".format(i, ingredient.ingred_id))
                        res[k] = v[item_list].copy()
                else:
                    for k, v in data.items():
                        for i in item_list:
                            if i not in v.columns:
                                logger.warning("concept {} not found in ingredient {}".format(i, ingredient.ingred_id))
                        keep_cols = list(set(v.columns.values) - set(item_list))
                        res[k] = v[keep_cols].copy()
    else:
        for k, df in data.items():
            res[k] = df.copy()

    if row_filters is not None:
        for k, df in res.items():
            res[k] = query(df, row_filters, available_scopes=df.columns)

    return ProcedureResult(chef, result, ingredient.key, data=res)


@debuggable
def filter_row(chef: Chef, ingredients: List[str], result, **options) -> ProcedureResult:
    """filter an ingredient based on a set of options and return
    the result as new ingredient.

    Procedure format:

    .. code-block:: yaml

       procedure: filter_row
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         filters: dict  # filter definition block

    A dictionary should be provided in options with the following format:

    .. code-block:: yaml

        filters:
            concept_1:
                filter_col_1: filter_val_1
                filter_col_2: filter_val_2

    See a detail example in this `recipe
    <https://github.com/semio/ddf_utils/blob/dev/tests/recipes_pass/test_filter_row.yml>`_

    Parameters
    ----------
    chef: Chef
        the Chef instance
    ingredients:
        list of ingredient id in the DAG
    result: `str`

    Keyword Args
    ------------
    filters: dict
        The filter description dictionary
    """
    warnings.simplefilter('always', DeprecationWarning)
    warnings.warn("filter_row is deprecated, please use filter function instead.", category=DeprecationWarning)
    warnings.simplefilter('default', DeprecationWarning)

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info("filter_row: " + ingredient.ingred_id)

    data = ingredient.get_data()
    filters = read_opt(options, 'filters', True)

    res = {}

    for k, v in filters.items():

        df = data[k].copy()

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
                raise ProcedureError("not supported in query: " + str(type(val)))
        query_string = ' and '.join(queries)
        logger.debug("querying: {}".format(query_string))

        df = df.query(query_string).copy()
        res[k] = df.query(query_string).copy()

    if not result:
        result = ingredient.ingred_id + '-filtered'
    return ProcedureResult(chef, result, ingredient.key, data=res)


@debuggable
def flatten(chef: Chef, ingredients: List[str], result, **options) -> ProcedureResult:
    """flattening some dimensions, create new indicators.

    procedure format:

    .. code-block:: yaml

       procedure: flatten
       ingredients:
           - ingredient_to_run
       options:
           flatten_dimensions:
               - entity_1
               - entity_2
           dictionary:
               "concept_name_wildcard": "new_concept_name_template"
           skip_totals_among_entities:
               - entity_1
               - entity_2

    The ``dictionary`` can have multiple entries, for each entry the concepts that matches the key in wildcard
    matching will be flatten to the value, which should be a template string. The variables for the templates
    will be provided with a dictionary contains ``concept``, and all columns from ``flatten_dimensions`` as keys.

    Parameters
    ----------
    chef : Chef
        the Chef instance
    ingredients : list
        a list of ingredients
    result : `str`
        id of result ingredient
    skip_totals_among_entities : list
        a list of total among entities, which we don't add to new indicator names

    Keyword Args
    ------------
    flatten_dimensions: list
        a list of dimension to be flattened
    dictionary: dict
        the dictionary for old name -> new name mapping
    """
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."

    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    data = ingredient.compute()

    logger.info("flatten: " + ingredient.ingred_id)

    flatten_dimensions = options['flatten_dimensions']
    if not isinstance(flatten_dimensions, list):
        flatten_dimensions = [flatten_dimensions]
    dictionary = options['dictionary']
    skip_totals_among_entities = read_opt(options, 'skip_totals_among_entities')

    newkey = [x for x in ingredient.key_to_list() if x not in flatten_dimensions]
    newkey = ','.join(newkey)

    res = {}
    for from_name_tmpl, new_name_tmpl in dictionary.items():
        dfs = dict([(x, data[x]) for x in fnmatch.filter(data.keys(), from_name_tmpl)])
        for from_name, df in dfs.items():
            groups = df.groupby(flatten_dimensions).groups
            for g, idx in groups.items():
                if not isinstance(g, tuple):
                    g = [g]
                df_ = df.loc[idx].copy()
                tmpl_dict = dict(zip(flatten_dimensions, g))
                tmpl_dict['concept'] = from_name
                new_name = new_name_tmpl.format(**tmpl_dict)
                # remove totals among entities from name
                if skip_totals_among_entities is not None:
                    for e in skip_totals_among_entities:
                        new_name = new_name.replace('_'+e, '')
                    logger.info('new name w/o total among entities is {}'.format(new_name))
                if new_name in res.keys():
                    raise ProcedureError("{} already created! check your name template please.".format(new_name))
                res[new_name] = df_.rename(columns={from_name: new_name}).drop(flatten_dimensions, axis=1)

    return ProcedureResult(chef, result, newkey, data=create_dsk(res))


@debuggable
def filter_item(chef: Chef, ingredients: List[str], result, items: list) -> ProcedureResult:
    """filter items from the ingredient data

    Procedure format:

    .. code-block:: yaml

       procedure: filter_item
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         items: list  # a list of items should be in the result ingredient

    Keyword Args
    ------------
    items: list
        a list of items to filter from base ingredient
    """
    warnings.simplefilter('always', DeprecationWarning)
    warnings.warn("filter_item is deprecated, please use filter function instead.", category=DeprecationWarning)
    warnings.simplefilter('default', DeprecationWarning)

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info("filter_item: " + ingredient.ingred_id)

    data = ingredient.get_data()

    try:
        data = dict([(k, data[k]) for k in items])
    except KeyError as e:
        logger.debug("keys in {}: {}".format(ingredient.ingred_id, str(list(data.keys()))))
        raise ProcedureError(str(e))

    if not result:
        result = ingredient.ingred_id

    return ProcedureResult(chef, result, ingredient.key, data=data)


@debuggable
def groupby(chef: Chef, ingredients: List[str], result, **options) -> ProcedureResult:
    """group ingredient data by column(s) and run aggregate function

    .. highlight:: yaml

    Procedure format:

    ::

       procedure: groupby
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         groupby: str or list  # column(s) to group
         aggregate: dict  # function block
         transform: dict  # function block
         filter: dict  # function block

    The function block should have below format:

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

    wildcard is supported in the column names. So ``aggreagte: {"*": "sum"}`` will run on every indicator in
    the ingredient

    Keyword Args
    ------------
    groubby : `str` or `list`
        the column(s) to group, can be a list or a string
    insert_key : `dict`
        manually insert keys in to result. This is useful when we want to add back the
        aggregated column and set them to one value. For example ``geo: global`` inserts
        the ``geo`` column with all values are "global"
    aggregate
    transform
    filter : `dict`, optinoal
        the function to run. only one of `aggregate`, `transform` and `filter` should be supplied.

    Note
    ----
    - Only one of ``aggregate``, ``transform`` or ``filter`` can be used in one procedure.
    - Any columns not mentioned in groupby or functions are dropped.
    """
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."

    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info("groupby: " + ingredient.ingred_id)
    data = ingredient.compute()
    by = options.pop('groupby')
    if 'insert_key' in options:
        insert_key = options.pop('insert_key')
    else:
        insert_key = dict()

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

    for name_tmpl, func in options[comp_type].items():
        func = mkfunc(func)
        indicator_names = fnmatch.filter(data.keys(), name_tmpl)
        for k in indicator_names:
            if comp_type == 'aggregate':
                newdata[k] = (data[k].groupby(by=by).agg({k: func})
                              .reset_index().dropna())
            if comp_type == 'transform':
                df = data[k].set_index(ingredient.key_to_list())
                levels = [df.index.names.index(x) for x in by]
                newdata[k] = (df.groupby(level=levels)[k].transform(func)
                              .reset_index().dropna())
            if comp_type == 'filter':
                df = data[k].set_index(ingredient.key_to_list())
                levels = [df.index.names.index(x) for x in by]
                newdata[k] = (df.groupby(level=levels)[k].filter(func)
                              .reset_index().dropna())
            for col, val in insert_key.items():
                newdata[k][col] = val
                newkey = newkey+','+col

    newdata = create_dsk(newdata)
    return ProcedureResult(chef, result, newkey, data=newdata)


@debuggable
def window(chef: Chef, ingredients: List[str], result, **options) -> ProcedureResult:
    """apply functions on a rolling window

    .. highlight:: yaml

    Procedure format:

    ::

       procedure: window
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         window:
           column: str  # column which window is created from
           size: int or 'expanding'  # if int then rolling window, if expanding then expanding window
           min_periods: int  # as in pandas
           center: bool  # as in pandas
           aggregate: dict

    Two styles of function block are supported, and they can mix in one procedure:

    ::

       aggregate:
         col1: sum  # run rolling sum to col1
         col2: mean  # run rolling mean to col2
         col3:  # run foo to col3 with param1=baz
       function: foo
       param1: baz

    Keyword Args
    ------------
    window: dict
        window definition, see above for the dictionary format
    aggregate: dict
        aggregation functions

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
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info('window: ' + ingredient.ingred_id)

    # reading options
    window = options.pop('window')
    aggregate = options.pop('aggregate')

    column = read_opt(window, 'column', required=True)
    size = read_opt(window, 'size', required=True)
    min_periods = read_opt(window, 'min_periods', default=0)
    center = read_opt(window, 'center', default=False)

    data = ingredient.compute()
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
                          .agg(f).reset_index().dropna())
        else:
            # There is a bug when running rolling on with groupby in pandas.
            # see https://github.com/pandas-dev/pandas/issues/13966
            # We will implement this later when we found work around or it's fixed
            # for now, we don't use the `on` parameter in rolling.
            # FIXME: add back the `on` parameter.
            newdata[k] = (df.groupby(level=levels, group_keys=False)
                          .rolling(window=size, min_periods=min_periods, center=center)
                          .agg(f).reset_index().dropna())

    newdata = create_dsk(newdata)
    return ProcedureResult(chef, result, ingredient.key, newdata)


@debuggable
def run_op(chef: Chef, ingredients: List[str], result, op) -> ProcedureResult:
    """run math operation on each row of ingredient data.

    Procedure format:

    .. code-block:: yaml

       procedure: filter_item
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         items: list  # a list of items should be in the result ingredient

    Keyword Args
    ------------
    op: dict
        a dictionary of concept_name -> function mapping

    Examples
    --------
    .. highlight:: yaml

    for exmaple, if we want to add 2 columns, col_a and col_b, to create an new column, we can
    write

    ::

        procedure: run_op
        ingredients:
          - ingredient_to_run
        result: new_ingredient_id
        options:
          op:
            new_col_name: "col_a + col_b"
    """

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    assert ingredient.dtype == 'datapoints'
    logger.info("run_op: " + ingredient.ingred_id)

    data = ingredient.compute()
    keys = ingredient.key_to_list()

    # concat all the datapoint dataframe first, and eval the ops
    to_concat = [v.set_index(keys) for v in data.values()]
    df = pd.concat(to_concat, axis=1)

    for k, v in op.items():
        res = df.eval(v).dropna()  # type(res) is Series
        res.name = k
        if k not in df.columns:
            df[k] = res
        data[k] = res.reset_index()

    newdata = create_dsk(data)
    if not result:
        result = ingredient.ingred_id + '-op'
    return ProcedureResult(chef, result, ingredient.key, data=newdata)


@debuggable
def extract_concepts(chef: Chef, ingredients: List[str], result,
                     join=None, overwrite=None, include_keys=False) -> ProcedureResult:
    """extract concepts from other ingredients.

    .. highlight:: yaml

    Procedure format:

    ::

       procedure: extract_concepts
       ingredients:  # list of ingredient id
         - ingredient_id_1
         - ingredient_id_2
       result: str  # new ingredient id
       options:
         join:  # optional
           base: str  # base concept ingredient id
           type: {'full_outer', 'ingredients_outer'}  # default is full_outer
         overwrite:  # overwrite some concept types
           country: entity_set
           year: time
         include_keys: true  # if we should include the primaryKeys concepts

    Parameters
    ----------
    ingredients
        any numbers of ingredient that needs to extract concepts from

    Keyword Args
    ------------
    join : dict, optional
        the base ingredient to join
    overwrite : dict, optional
        overwrite concept types for some concepts
    include_keys : bool, optional
        if we shuld include the primaryKeys of the ingredients, default to false

    See Also
    --------
    :py:func:`ddf_utils.transformer.extract_concepts` : related function in transformer
    module

    Note
    ----
    - all concepts in ingredients in the ``ingredients`` parameter will be extracted
      to a new concept ingredient
    - ``join`` option is optional; if present then the ``base`` will merge with concepts
      from ``ingredients``
    - ``full_outer`` join means get the union of concepts; ``ingredients_outer`` means
      only keep concepts from ``ingredients``

    """

    # ingredients = [chef.dag.get_node(x).evaluate() for x in ingredients]
    logger.info("extract concepts: {}".format([x.ingred_id for x in ingredients]))

    if join:
        base = chef.dag.get_node(join['base']).evaluate()
        try:
            join_type = join['type']
        except KeyError:
            join_type = 'full_outer'
        concepts = base.get_data()['concept'].set_index('concept')
    else:
        concepts = pd.DataFrame([], columns=['concept', 'concept_type']).set_index('concept')
        join_type = 'full_outer'

    new_concepts = set()

    for i in ingredients:
        data = i.get_data()
        pks = i.key_to_list()
        for k, df in data.items():
            if include_keys:
                cols = df.columns
            else:
                cols = [x for x in df.columns if x not in pks]
            for col in cols:
                if col.startswith('is--'):
                    continue
                new_concepts.add(col)
                if col in concepts.index:
                    continue
                if np.issubdtype(df[col].dtype, np.number):
                    concepts.loc[col, 'concept_type'] = 'measure'
                else:
                    concepts.loc[col, 'concept_type'] = 'string'

    if join_type == 'ingredients_outer':
        # ingredients_outer join: only keep concepts appears in ingredients
        concepts = concepts.loc[new_concepts]

    # add name column if there isn't one
    if 'name' not in concepts.columns:
        concepts['name'] = np.nan
    if 'name' not in concepts.index.values:
        concepts.loc['name', 'concept_type'] = 'string'
        concepts.loc['name', 'name'] = 'Name'
    concepts['name'] = concepts['name'].fillna(
        concepts.index.to_series().map(lambda x: str(x).replace('_', ' ').title()))

    # overwrite some of the types
    if overwrite:
        for k, v in overwrite.items():
            concepts.loc[k, 'concept_type'] = v
    if not result:
        result = 'concepts_extracted'
    return ProcedureResult(chef, result, 'concept', data={'concept': concepts.reset_index()})


@debuggable
def trend_bridge(chef: Chef, ingredients: List[str], bridge_start, bridge_end, bridge_length, bridge_on,
                 result, target_column=None) -> ProcedureResult:
    """run trend bridge on ingredients

    .. highlight:: yaml

    Procedure format:

    ::

      procedure: trend_bridge
      ingredients:
        - data_ingredient                 # optional, if not set defaults to empty ingredient
      result: data_bridged
      options:
        bridge_start:
            ingredient: old_data_ingredient # optional, if not set then assume it's the input ingredient
            column: concept_old_data
        bridge_end:
            ingredient: new_data_ingredient # optional, if not set then assume it's the input ingredient
            column: concept_new_data
        bridge_length: 5                  # steps in time. If year, years, if days, days.
        bridge_on: time                   # the index column to build the bridge with
        target_column: concept_in_result  # overwrites if exists. creates if not exists. default to bridge_end.column

    Parameters
    ----------
    chef: Chef
        A Chef instance
    ingredients : list
        The input ingredient. The bridged result will be merged in to this ingredient. If this is
        None, then the only the bridged result will be returned
    bridge_start : dict
        Describe the start of bridge
    bridge_end : dict
        Describe the end of bridge
    bridge_length : int
        The size of bridge
    bridge_on : `str`
        The column to bridge
    result : `str`
        The output ingredient id

    Keyword Args
    ------------
    target_column : `str`, optional
        The column name of the bridge result. default to `bridge_end.column`

    See Also
    --------
    :py:func:`ddf_utils.transformer.trend_bridge` : related function in transformer module
    """
    from ..transformer import trend_bridge as tb

    # check parameters
    if ingredients is None:
        assert 'ingredient' in bridge_start.keys()
        assert 'ingredient' in bridge_end.keys()
        ingredient = None
    else:
        assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
        # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
        ingredient = ingredients[0]

    # get data for start and end
    if 'ingredient' in bridge_start.keys():
        start = chef.dag.get_node(bridge_start['ingredient']).evaluate()
    else:
        start = ingredient
    if 'ingredient' in bridge_end.keys():
        end = chef.dag.get_node(bridge_end['ingredient']).evaluate()
    else:
        end = ingredient

    assert start.dtype == 'datapoints'
    assert end.dtype == 'datapoints'

    logger.info("trend_bridge: {} and {}".format(start.ingred_id, end.ingred_id))

    if target_column is None:
        target_column = bridge_start['column']

    # get the column to group. Because datapoints are multidimensional, but we only
    # bridge them in one column, so we should group other columns.
    assert set(start.key_to_list()) == set(end.key_to_list())

    keys = start.key_to_list()
    keys.remove(bridge_on)

    # start_group = start.get_data()[bridge_start['column']].set_index(bridge_on).groupby(keys)
    # end_group = end.get_data()[bridge_end['column']].set_index(bridge_on).groupby(keys)
    start_group = start.compute()[bridge_start['column']].set_index(bridge_on).groupby(keys)
    end_group = end.compute()[bridge_end['column']].set_index(bridge_on).groupby(keys)

    # calculate trend bridge on each group
    res_grouped = []
    for g, df in start_group:
        gstart = df.copy()
        try:
            gend = end_group.get_group(g).copy()
        except KeyError:  # no new data available for this group
            logger.warning("no data for bridge end: " + g)
            bridged = gstart[bridge_start['column']]
        else:
            bridged = tb(gstart[bridge_start['column']], gend[bridge_end['column']], bridge_length)

        res_grouped.append((g, bridged))

    # combine groups to dataframe
    res = []
    for g, v in res_grouped:
        v.name = target_column
        v = v.reset_index()
        if len(keys) == 1:
            assert isinstance(g, str)
            v[keys[0]] = g
        else:
            assert isinstance(g, list)
            for i, k in enumerate(keys):
                v[k] = g[i]
        res.append(v)
    result_data = pd.concat(res, ignore_index=True)

    if ingredient is not None:
        merged = _merge_two(ingredient.compute(), {target_column: result_data},
                            start.key_to_list(), 'datapoints', deep=True)
        return ProcedureResult(chef, result, start.key, create_dsk(merged))
    else:
        return ProcedureResult(chef, result, start.key,
                               create_dsk({target_column: result_data}))


@debuggable
def merge_entity(chef: Chef, ingredients: List[str], dictionary,
                 target_column, result, merged='drop'):
    """merge entities"""
    from ..transformer import merge_keys

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]

    data = ingredient.compute()

    res_data = dict()
    for k, df in data.items():
        res_data[k] = merge_keys(df.set_index(ingredient.key_to_list()),
                                 dictionary, target_column=target_column, merged=merged).reset_index()

    return ProcedureResult(chef, result, ingredient.key, create_dsk(res_data))


@debuggable
def split_entity(chef: Chef, ingredients: List[str], dictionary,
                 target_column, result, splitted='drop'):
    """split entities"""
    from ..transformer import split_keys

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]

    data = ingredient.compute()

    res_data = dict()
    for k, df in data.items():
        res_data[k] = split_keys(df.set_index(ingredient.key_to_list()),
                                 target_column, dictionary, splitted).reset_index()

    return ProcedureResult(chef, result, ingredient.key, create_dsk(res_data))
