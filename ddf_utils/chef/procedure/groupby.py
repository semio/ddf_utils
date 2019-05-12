# -*- coding: utf-8 -*-

"""groupby procedure for recipes"""

import fnmatch
import logging

from typing import List

from .. helpers import debuggable, mkfunc
from .. model.ingredient import DataPointIngredient
from .. model.chef import Chef


logger = logging.getLogger('groupby')


@debuggable
def groupby(chef: Chef, ingredients: List[DataPointIngredient], result, **options) -> DataPointIngredient:
    """group ingredient data by column(s) and run aggregate function

    .. highlight:: yaml

    Procedure format:

    ::

       procedure: groupby
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         groupby: str or list  # column(s) to group
         aggregate: dict  # function block
         transform: dict  # function block
         filter: dict  # function block

    The function block should have below format:

    ::

      aggregate:
        column1: func_name1
        column2: func_name2

    or

    ::

        aggrgrate:
          column1:
            function: func_name
            param1: foo
            param2: baz

    wildcard is supported in the column names. So ``aggreagte: {"*": "sum"}`` will run on every indicator in
    the ingredient

    Keyword Args
    ------------
    groupby : `str` or `list`
        the column(s) to group, can be a list or a string
    insert_key : `dict`
        manually insert keys in to result. This is useful when we want to add back the
        aggregated column and set them to one value. For example ``geo: global`` inserts
        the ``geo`` column with all values are "global"
    aggregate
    transform
    filter : `dict`, optinoal
        the function to run. only one of `aggregate`, `transform` and `filter` should be supplied.

    Note
    ----
    - Only one of ``aggregate``, ``transform`` or ``filter`` can be used in one procedure.
    - Any columns not mentioned in groupby or functions are dropped.
    """
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."

    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info("groupby: " + ingredient.id)
    data = ingredient.get_data()
    by = options.pop('groupby')
    if 'insert_key' in options:
        insert_key = options.pop('insert_key')
    else:
        insert_key = dict()

    # only one of aggregate/transform/filter should be in options.
    assert len(list(options.keys())) == 1
    comp_type = list(options.keys())[0]
    assert comp_type in ['aggregate', 'transform', 'filter']

    if comp_type == 'aggregate':  # only aggregate should change the key of ingredient
        if isinstance(by, list):
            newkey = ','.join(by)
        else:
            newkey = by
            by = [by]
        logger.debug("changing the key to: " + str(newkey))
    else:
        newkey = ingredient.key
        by = [by]

    newdata = dict()

    for name_tmpl, func in options[comp_type].items():
        func = mkfunc(func)
        indicator_names = fnmatch.filter(data.keys(), name_tmpl)
        for k in indicator_names:
            df = data[k].compute()
            if comp_type == 'aggregate':
                newdata[k] = (df.groupby(by=by).agg({k: func})
                              .reset_index().dropna())
            if comp_type == 'transform':
                df = df.set_index(ingredient.key)
                levels = [df.index.names.index(x) for x in by]
                newdata[k] = (df.groupby(level=levels)[k].transform(func)
                              .reset_index().dropna())
            if comp_type == 'filter':
                df = df.set_index(ingredient.key)
                levels = [df.index.names.index(x) for x in by]
                newdata[k] = (df.groupby(level=levels)[k].filter(func)
                              .reset_index().dropna())
            for col, val in insert_key.items():
                newdata[k][col] = val
                newkey = newkey + ',' + col

    return DataPointIngredient.from_procedure_result(result, newkey, data_computed=newdata)
