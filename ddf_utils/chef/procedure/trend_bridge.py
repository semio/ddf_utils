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
            column:
              - concept_old_data
        bridge_end:
            ingredient: new_data_ingredient # optional, if not set then assume it's the input ingredient
            column:
              - concept_new_data
        bridge_length: 5                  # steps in time. If year, years, if days, days.
        bridge_on: time                   # the index column to build the bridge with
        target_column:
              - concept_in_result  # overwrites if exists. creates if not exists. default to bridge_end.column

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
    target_column : `list`, optional
        The column name of the bridge result. default to `bridge_end.column`

    See Also
    --------
    :py:func:`ddf_utils.transformer.trend_bridge` : related function in transformer module
    """
    from ... transformer import trend_bridge as tb

    # check parameters
    if ingredients is None or ingredients == []:
        assert 'ingredient' in bridge_start.keys()
        assert 'ingredient' in bridge_end.keys()
        ingredient = None
    else:
        assert len(ingredients) <= 1, "procedure only support 1 ingredient for now."
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

    assert len(bridge_end['column']) == len(bridge_start['column']),\
        "columns length in bridge_start and bridge_end should be the same!"

    logger.info("trend_bridge: {} and {}".format(start.ingred_id, end.ingred_id))

    if target_column is None:
        target_column = bridge_end['column']

    # get the column to group. Because datapoints are multidimensional, but we only
    # bridge them in one column, so we should group other columns.
    try:
        assert set(start.key_to_list()) == set(end.key_to_list())
    except AssertionError:
        logger.critical("start and end have different keys! {} and {}".format(start.key, end.key))
        raise

    keys = start.key_to_list()
    keys.remove(bridge_on)

    # calculate for each column
    new_data = dict()

    start_computed = start.compute()
    end_computed = end.compute()

    for c1, c2, c3 in zip(bridge_start['column'], bridge_end['column'], target_column):
        logger.info("bridge_start: {}, bridge_end: {}, target_column: {}".format(c1, c2, c3))
        start_group = start_computed[c1].set_index(bridge_on).groupby(keys)
        end_group = end_computed[c2].set_index(bridge_on).groupby(keys)

        # get all groups
        g1 = list(start_group.groups.keys())
        g2 = list(end_group.groups.keys())
        all_groups = g1.copy()
        for g in g2:
            if g not in all_groups:
                all_groups.append(g)

        # calculate trend bridge on each group
        res_grouped = []
        for g in all_groups:
            if g not in g1:
                logger.warning("no data for bridge start: " + str(g))
                bridged = end_group.get_group(g)[c2].copy()
            elif g not in g2:
                logger.warning("no data for bridge end: " + str(g))
                bridged = start_group.get_group(g)[c1].copy()
            else:
                gstart = start_group.get_group(g)[c1].copy()
                gend = end_group.get_group(g)[c2].copy()
                bridged = tb(gstart, gend, bridge_length)

            res_grouped.append((g, bridged))

        # combine groups to dataframe
        res = []
        for g, v in res_grouped:
            v.name = c3
            v = v.reset_index()
            if len(keys) == 1:
                assert isinstance(g, str)
                v[keys[0]] = g
            else:
                assert isinstance(g, tuple)
                for i, k in enumerate(keys):
                    v[k] = g[i]
            res.append(v)
        new_data[c3] = pd.concat(res, ignore_index=True)

    # merge in to ingredient in `ingredients` parameter if needed
    if ingredient is not None:
        from . merge import _merge_two
        merged = _merge_two(ingredient.compute(), new_data,
                            start.key_to_list(), 'datapoints', deep=False)
        return ProcedureResult(chef, result, start.key, create_dsk(merged))
    else:
        return ProcedureResult(chef, result, start.key,
                               create_dsk(new_data))