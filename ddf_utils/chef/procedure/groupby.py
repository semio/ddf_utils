# -*- coding: utf-8 -*-

"""groupby procedure for recipes"""

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
def groupby(chef: Chef, ingredients: List[str], result, **options) -> ProcedureResult:
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
    logger.info("groupby: " + ingredient.ingred_id)
    data = ingredient.compute()
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
            if comp_type == 'aggregate':
                newdata[k] = (data[k].groupby(by=by).agg({k: func})
                              .reset_index().dropna())
            if comp_type == 'transform':
                df = data[k].set_index(ingredient.key_to_list())
                levels = [df.index.names.index(x) for x in by]
                newdata[k] = (df.groupby(level=levels)[k].transform(func)
                              .reset_index().dropna())
            if comp_type == 'filter':
                df = data[k].set_index(ingredient.key_to_list())
                levels = [df.index.names.index(x) for x in by]
                newdata[k] = (df.groupby(level=levels)[k].filter(func)
                              .reset_index().dropna())
            for col, val in insert_key.items():
                newdata[k][col] = val
                newkey = newkey+','+col

    newdata = create_dsk(newdata)
    return ProcedureResult(chef, result, newkey, data=newdata)
