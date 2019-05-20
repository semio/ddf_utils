# -*- coding: utf-8 -*-

"""translate_header procedures for recipes"""

import logging
from typing import List

from .. helpers import debuggable, build_dictionary
from .. model.ingredient import Ingredient, get_ingredient_class
from .. exceptions import ProcedureError
from .. model.chef import Chef


logger = logging.getLogger('translate_header')


def _translate_header_datapoints(ingredient, dictionary, duplicated):
    """Translating Datapoints.

    1. rename the columns in dataframe
    2. check duplication in the origin data.keys()
    3. rename the key in data.keys()
    """
    data = ingredient.get_data()
    new_data = dict()

    for k in list(data.keys()):
        df_new = data[k].rename(columns=dictionary)

        if k not in dictionary.keys():  # no need to translate keys in `data`
            new_data[k] = df_new
            continue

        new_name = dictionary[k]
        if new_name in data.keys():  # duplicated
            if duplicated == 'error':
                raise ValueError(
                    'can not rename indicator {} to {} '
                    'because {} already exists!'.format(k, new_name, new_name))
            else:
                # replace
                new_data[new_name] = df_new
        else:
            new_data[new_name] = df_new

    return new_data


def _translate_header_entities_concepts(ingredient, dictionary, duplicated):
    """Translating entities or concepts

    Entities and concepts only have one dataframe inside the ingredient, so renaming will
    happen in the dataframe columns.

    Note that it's not allowed to translate reversed keyword `concept`/`concept_type`
    """
    data = ingredient.get_data()
    new_data = dict()

    if 'concept' in dictionary and ingredient.dtype == 'concepts':
        raise ValueError('cannot translate the primaryKey for concept!')

    for k in list(data.keys()):
        df_new = data[k].copy()
        for old_name, new_name in dictionary.items():
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
                if c.startswith('is--'):
                    old_name_is_header = c[4:]
                    if old_name_is_header in dictionary:
                        rm_[c] = 'is--' + dictionary[old_name_is_header]
            if len(rm_) > 0:
                df_new = df_new.rename(columns=rm_)
        if k in dictionary.keys():  # if we need to rename the concept name
            new_data[dictionary[k]] = df_new
            # del(data[k])
        else:  # we only rename index/properties columns
            new_data[k] = df_new

    return new_data


@debuggable
def translate_header(chef: Chef, ingredients: List[Ingredient],
                     result, dictionary, duplicated='error') -> Ingredient:
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
    logger.info("translate_header: " + ingredient.id)

    d = build_dictionary(chef, dictionary)

    if ingredient.dtype == 'datapoints':
        new_data = _translate_header_datapoints(ingredient, d, duplicated)
    elif ingredient.dtype in ['entities', 'concepts']:
        new_data = _translate_header_entities_concepts(ingredient, d, duplicated)
    else:
        raise ProcedureError("translate header do not support this data type: {}".format(ingredient.dtype))

    # also rename the key
    if ingredient.dtype == 'datapoints':
        newkey = ingredient.key.copy()
        for key in d.keys():
            if key in ingredient.key:
                newkey[newkey.index(key)] = d[key]
    elif ingredient.dtype == 'entities':
        newkey = ingredient.key
        for key in d.keys():
            if key == ingredient.key:
                newkey = d[key]
    else:  # concepts, no need to change.
        newkey = ingredient.key
    if not result:
        result = ingredient.id + '-translated'
    return get_ingredient_class(ingredient.dtype).from_procedure_result(result, newkey, data_computed=new_data)
