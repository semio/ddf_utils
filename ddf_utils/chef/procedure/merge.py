# -*- coding: utf-8 -*-

"""merge procedure for recipes"""

import logging
import time

from typing import List, Dict, Union

import numpy as np
import pandas as pd
import dask.dataframe as dd

from .. exceptions import ProcedureError
from .. helpers import debuggable
from .. model.ingredient import Ingredient, get_ingredient_class
from .. model.chef import Chef


logger = logging.getLogger('merge')


@debuggable
def merge(chef: Chef, ingredients: List[Ingredient], result, deep=False) -> Ingredient:
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
    logger.info("merge: " + str([i.id for i in ingredients]))

    # assert that dtype and key are same in all dataframe
    try:
        for x in ingredients[1:]:
            assert set(x.key) == set(ingredients[0].key)
        assert len(set([x.dtype for x in ingredients])) == 1
    except (AssertionError, TypeError):
        log1 = "multiple dtype/key detected: \n"
        log2 = "\n".join(["{}: {}, {}".format(x.id, x.dtype, x.key) for x in ingredients])
        logger.warning(log1 + log2)
        raise ProcedureError("can't merge data with multiple dtype/key!")

    # get the dtype and index
    # we have assert dtype and key is unique, so we take it from
    # the first ingredient
    dtype = ingredients[0].dtype

    if dtype == 'datapoints':
        index_col = ingredients[0].key
        newkey = ','.join(index_col)
    else:
        index_col = ingredients[0].key
        newkey = index_col

    if deep:
        logger.info("merge: doing deep merge")
    # merge data from ingredients one by one.
    res_all = {}

    for i in ingredients:
        res_all = _merge_two(res_all, i.get_data(), index_col, dtype, deep)

    if not result:
        result = 'all_data_merged_' + str(int(time.time() * 1000))

    return get_ingredient_class(dtype).from_procedure_result(result, newkey, data_computed=res_all)


def __get_last_item(ser):
    """get the last valid item of a Series, or Nan."""
    ser_ = ser.dropna()
    if ser_.last_valid_index() is None:
        return np.nan
    else:
        return ser_.values[-1]


def _merge_two(left: Dict[str, Union[pd.DataFrame, dd.DataFrame]],
               right: Dict[str, Union[pd.DataFrame, dd.DataFrame]],
               index_col: Union[List, str],
               dtype: str, deep=False) -> Dict[str, pd.DataFrame]:
    """merge 2 ingredient data."""
    if len(left) == 0:
        return right

    res_data = {}

    # for datapoints we use dask to help performance.
    if dtype == 'datapoints':
        res_data = dict([(k, v) for k, v in left.items()])
        if deep:
            for k, df in right.items():
                if k in left.keys():
                    columns = left[k].columns.values
                    # res_data[k] = left[k].append(df[columns], interleave_partitions=True)
                    res_data[k] = dd.concat([left[k], df[columns]], axis=0, interleave_partitions=True)
                    res_data[k] = res_data[k].drop_duplicates(subset=index_col, keep='last')
                    # res_data[k] = res_data[k].sort_values(by=index_col)
                else:
                    res_data[k] = df
        else:
            for k, df in right.items():
                res_data[k] = df

    # for concepts/entities, we don't need to use dask.
    elif dtype == 'concepts':

        left_df = pd.concat([x for x in left.values()], sort=False)
        right_df = pd.concat([x for x in right.values()], sort=False)

        if deep:
            merged = left_df.append(right_df, sort=False)
            res = merged.groupby(by=index_col).agg(__get_last_item)
            res_data = {'concept': res.reset_index()}
        else:
            res_data = {'concept':
                        right_df.drop_duplicates(subset='concept',
                                                 keep='last')}
        res_data = res_data

    else:  # entities
        if deep:
            for k, df in right.items():
                if k in left.keys():
                    left[k] = left[k].append(df, ignore_index=True, sort=False)
                    left[k] = left[k].groupby(index_col).agg(__get_last_item).reset_index()
                else:
                    left[k] = df
        else:
            for k, df in right.items():
                left[k] = df
        res_data = left

    return res_data
