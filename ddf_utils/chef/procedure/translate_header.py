# -*- coding: utf-8 -*-

"""translate_header procedures for recipes"""

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
def translate_header(chef: Chef, ingredients: List[str],
                     result, dictionary, duplicated='error') -> ProcedureResult:
    """Translate column headers

    Procedure format:

    .. code-block:: yaml

       procedure: translate_header
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         dictionary: str or dict  # file name or mappings dictionary

    Parameters
    ----------
    chef : Chef
        The Chef the procedure will run on
    ingredients : list
        A list of ingredient id in the dag to translate
    dictionary : dict or `str`
        A dictionary for name mapping, or filepath to the dictionary
    duplicated : `str`
       What to do when there are duplicated columns after renaming. Avaliable options
       are `error`, `replace`
    result : `str`
        The result ingredient id

    See Also
    --------
    :py:func:`ddf_utils.transformer.translate_header` : Related function in transformer module
    """
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    ingredient = ingredients[0]
    logger.info("translate_header: " + ingredient.ingred_id)

    data = ingredient.get_data()
    new_data = dict()
    rm = build_dictionary(chef.dag, dictionary)

    for k in list(data.keys()):
        # df_new = data[k].rename(columns=rm)
        df_new = data[k].copy()
        for old_name, new_name in rm.items():
            if new_name in df_new.columns:
                if duplicated == 'error':
                    raise ValueError(
                        'can not rename column {} to {} '
                        'because {} already exists!'.format(old_name, new_name, new_name))
                elif duplicated == 'replace':
                    df_new[new_name] = df_new[old_name]
                    df_new = df_new.drop(old_name, axis=1)
                else:
                    raise ValueError('unknown option to `duplicated`: {}'.format(duplicated))
            elif old_name in df_new.columns:
                df_new[new_name] = df_new[old_name]
                df_new = df_new.drop(old_name, axis=1)
            else:
                continue
        if ingredient.dtype == 'entities':  # also rename the `is--` headers
            rm_ = {}
            for c in df_new.columns:
                if k == c[4:]:
                    rm_[c] = 'is--' + rm[k]
            if len(rm_) > 0:
                df_new = df_new.rename(columns=rm_)
        if k in rm.keys():  # if we need to rename the concept name
            new_data[rm[k]] = df_new
            # del(data[k])
        else:  # we only rename index/properties columns
            new_data[k] = df_new

    # also rename the key
    newkey = ingredient.key
    if ingredient.dtype == 'datapoints':
        for key in rm.keys():
            if key in ingredient.key:
                newkey = newkey.replace(key, rm[key])
    elif ingredient.dtype == 'entities':
        for key in rm.keys():
            if key == ingredient.key:
                newkey = rm[key]
    else:
        if 'concept' in rm.keys():
            raise ValueError('can translate the primaryKey for concept!')

    if not result:
        result = ingredient.ingred_id + '-translated'
    return ProcedureResult(chef, result, newkey, data=new_data)
