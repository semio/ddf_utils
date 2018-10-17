# -*- coding: utf-8 -*-

"""split_entity procedure for recipes"""

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
def split_entity(chef: Chef, ingredients: List[str], dictionary,
                 target_column, result, splitted='drop'):
    """split entities"""
    from ... transformer import split_keys

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]

    data = ingredient.compute()

    res_data = dict()
    for k, df in data.items():
        res_data[k] = split_keys(df.set_index(ingredient.key_to_list()),
                                 target_column, dictionary, splitted).reset_index()

    return ProcedureResult(chef, result, ingredient.key, create_dsk(res_data))
