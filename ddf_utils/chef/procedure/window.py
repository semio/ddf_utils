# -*- coding: utf-8 -*-

"""window procedure for recipes"""

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
