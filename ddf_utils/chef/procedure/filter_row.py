# -*- coding: utf-8 -*-

"""filter_row procedure for recipes

This function is deprecated.
"""

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
