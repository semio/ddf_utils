# -*- coding: utf-8 -*-

"""window procedure for recipes"""

import logging
from typing import List

import pandas as pd
from .. helpers import debuggable, read_opt, mkfunc
from .. model.ingredient import DataPointIngredient
from .. model.chef import Chef


logger = logging.getLogger('window')


@debuggable
def window(chef: Chef, ingredients: List[DataPointIngredient], result, **options) -> DataPointIngredient:
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
    logger.info('window: ' + ingredient.id)

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
        keys = ingredient.key.copy()

        df = data[k].copy()

        # always sort before rolling
        df = df.sort_values(keys)

        # then remove the rolling column from primary keys, group by remaining keys
        keys.remove(column)

        if size == 'expanding':
            res = []
            groups = df.groupby(by=keys, sort=False)
            for _, df_g in groups:
                res.append(df_g.set_index(ingredient.key)
                           .expanding(min_periods=min_periods, center=center).agg({k: f}))
            newdata[k] = pd.concat(res, sort=False).reset_index()
        else:
            newdata[k] = (df.groupby(by=keys, sort=False)
                          .rolling(on=column, window=size, min_periods=min_periods, center=center)
                          .agg({k: f}).reset_index(ingredient.key).dropna())


    return DataPointIngredient.from_procedure_result(result, ingredient.key, newdata)
