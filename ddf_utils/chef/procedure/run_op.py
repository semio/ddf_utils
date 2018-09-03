# -*- coding: utf-8 -*-

"""run_op procedure for recipes"""

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
def run_op(chef: Chef, ingredients: List[BaseIngredient], result, op) -> ProcedureResult:
    """run math operation on each row of ingredient data.

    Procedure format:

    .. code-block:: yaml

       procedure: run_op
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         op: dict  # a dictionary describing calculation for each columns.

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
    to_concat = [v for v in data.values()]

    # preserve dtypes
    dtypes_ = to_concat[0].dtypes[keys]
    dtypes = dict([k, v.name] for k, v in dtypes_.items())

    if len(to_concat) == 1:
        df = to_concat[0]
    else:
        df = pd.merge(to_concat[0], to_concat[1], on=keys, how='outer')
        for _df in to_concat[2:]:
            df = pd.merge(df, _df, on=keys, how='outer')
        # reset dtypes
        for c in keys:
            df[c] = df[c].astype(dtypes[c])
    df = df.set_index(keys)

    for k, v in op.items():
        res = df.eval(v).dropna()  # type(res) is Series
        res.name = k
        if k not in df.columns:
            df[k] = res
        data[k] = res.reset_index()

    newdata = create_dsk(data)
    if not result:
        result = ingredient.ingred_id + '-op'
    return ProcedureResult(chef, result, ingredient.key, data=create_dsk(newdata))
