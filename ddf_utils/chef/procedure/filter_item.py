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
def filter_item(chef: Chef, ingredients: List[str], result, items: list) -> ProcedureResult:
    """filter items from the ingredient data

    Procedure format:

    .. code-block:: yaml

       procedure: filter_item
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         items: list  # a list of items should be in the result ingredient

    Keyword Args
    ------------
    items: list
        a list of items to filter from base ingredient
    """
    warnings.simplefilter('always', DeprecationWarning)
    warnings.warn("filter_item is deprecated, please use filter function instead.", category=DeprecationWarning)
    warnings.simplefilter('default', DeprecationWarning)

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info("filter_item: " + ingredient.ingred_id)

    data = ingredient.get_data()

    try:
        data = dict([(k, data[k]) for k in items])
    except KeyError as e:
        logger.debug("keys in {}: {}".format(ingredient.ingred_id, str(list(data.keys()))))
        raise ProcedureError(str(e))

    if not result:
        result = ingredient.ingred_id

    return ProcedureResult(chef, result, ingredient.key, data=data)
