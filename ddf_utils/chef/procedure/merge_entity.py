# -*- coding: utf-8 -*-

"""merge_entity procedure for recipes"""

import fnmatch
import logging
import time
import warnings
from collections import Mapping, Sequence
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from .. exceptions import ProcedureError
from .. helpers import debuggable, mkfunc, query, read_opt, create_dsk, build_dictionary
from .. model.ingredient import *
from .. model.chef import Chef


logger = logging.getLogger('merge_entity')


@debuggable
def merge_entity(chef: Chef, ingredients: List[EntityIngredient], dictionary,
                 target_column, result, merged='drop') -> EntityIngredient:
    """merge entities"""
    from ... transformer import merge_keys

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]

    data = ingredient.get_data()

    res_data = dict()
    for k, df in data.items():
        res_data[k] = merge_keys(df.set_index(ingredient.key),
                                 dictionary, target_column=target_column, merged=merged).reset_index()

    return EntityIngredient.from_procedure_result(result, ingredient.key, res_data)
