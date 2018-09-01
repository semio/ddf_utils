# -*- coding: utf-8 -*-

"""filter procedure for recipes"""

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

    if items is not None and len(items) != 0:
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
        elif isinstance(items, dict) and len(items) == 1:
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
            raise ValueError("item filter not supported: " + str(items))
    else:
        for k, df in data.items():
            res[k] = df.copy()

    if row_filters is not None:
        for k, df in res.items():
            res[k] = query(df, row_filters, available_scopes=df.columns)

    return ProcedureResult(chef, result, ingredient.key, data=res)
