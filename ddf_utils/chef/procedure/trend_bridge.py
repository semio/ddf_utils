# -*- coding: utf-8 -*-

"""all procedures for recipes"""

import logging
from typing import List

import pandas as pd

from .. helpers import debuggable
from .. model.ingredient import DataPointIngredient
from .. model.chef import Chef

logger = logging.getLogger('trend_bridge')


@debuggable
def trend_bridge(chef: Chef, ingredients: List[DataPointIngredient], bridge_start, bridge_end,
                 bridge_length, bridge_on, result, target_column=None) -> DataPointIngredient:
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

    logger.info("trend_bridge: {} and {}".format(start.id, end.id))

    if target_column is None:
        target_column = bridge_end['column']

    # get the column to group. Because datapoints are multidimensional, but we only
    # bridge them in one column, so we should group other columns.
    try:
        assert set(start.key) == set(end.key)
    except AssertionError:
        logger.critical("start and end have different keys! {} and {}".format(start.key, end.key))
        raise

    keys = start.key.copy()
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
        # NOTE: currently pandas (0.24) always emits all values from a category when doing groupby, regardless whether
        # the value is actually in the column.
        g1 = list(start_group.groups.keys())
        g2 = list(end_group.groups.keys())
        all_groups = g1.copy()
        for g in g2:
            if g not in all_groups:
                all_groups.append(g)

        # calculate trend bridge on each group
        res_grouped = []
        for g in all_groups:
            try:
                gstart = start_group.get_group(g)[c1].copy()
            except KeyError:
                logger.warning("no data for bridge start: " + str(g))
                gstart = None
            try:
                gend = end_group.get_group(g)[c2].copy()
            except KeyError:
                logger.warning("no data for bridge end: " + str(g))
                gend = None

            if gstart is None and gend is None:
                continue
            if gstart is None:
                bridged = gend
            elif gend is None:
                bridged = gstart
            else:
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
                            start.key, 'datapoints', deep=False)
        return DataPointIngredient.from_procedure_result(result, start.key, merged)
    else:
        return DataPointIngredient.from_procedure_result(result, start.key, new_data)
