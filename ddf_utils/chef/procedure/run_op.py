# -*- coding: utf-8 -*-

"""run_op procedure for recipes"""

import logging
from typing import List

import pandas as pd

from .. helpers import debuggable
from .. model.ingredient import DataPointIngredient
from .. model.chef import Chef


logger = logging.getLogger('run_op')


@debuggable
def run_op(chef: Chef, ingredients: List[DataPointIngredient], result, op) -> DataPointIngredient:
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
    logger.info("run_op: " + ingredient.id)

    # FIXME: avoid using compute
    data = ingredient.compute()
    keys = ingredient.key

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

    if not result:
        result = ingredient.id + '-op'
    return DataPointIngredient.from_procedure_result(result, ingredient.key, data_computed=data)
