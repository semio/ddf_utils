# -*- coding: utf-8 -*-

"""split_entity procedure for recipes"""

import logging
from typing import List

from .. helpers import debuggable
from .. model.ingredient import DataPointIngredient
from .. model.chef import Chef


logger = logging.getLogger('split_entity')


@debuggable
def split_entity(chef: Chef, ingredients: List[DataPointIngredient], dictionary,
                 target_column, result, splitted='drop') -> DataPointIngredient:
    """split entities"""
    from ... transformer import split_keys

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]

    data = ingredient.compute()

    res_data = dict()
    for k, df in data.items():
        res_data[k] = split_keys(df.set_index(ingredient.key),
                                 target_column, dictionary, splitted).reset_index()

    return DataPointIngredient.from_procedure_result(result, ingredient.key, res_data)
