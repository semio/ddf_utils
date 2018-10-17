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

from .. dag import DAG
from .. exceptions import ProcedureError
from .. helpers import debuggable, mkfunc, query, read_opt, create_dsk, build_dictionary
from .. ingredient import BaseIngredient, ProcedureResult

logger = logging.getLogger('Chef')


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
    from ... transformer import trend_bridge as tb

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
        from . merge import _merge_two
        merged = _merge_two(ingredient.compute(), {target_column: result_data},
                            start.key_to_list(), 'datapoints', deep=True)
        return ProcedureResult(chef, result, start.key, create_dsk(merged))
    else:
        return ProcedureResult(chef, result, start.key,
                               create_dsk({target_column: result_data}))
