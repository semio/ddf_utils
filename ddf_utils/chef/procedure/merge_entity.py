# -*- coding: utf-8 -*-

"""merge_entity procedure for recipes"""

import logging
from typing import List

from .. helpers import debuggable
from .. model.ingredient import DataPointIngredient
from .. model.chef import Chef


logger = logging.getLogger('merge_entity')


@debuggable
def merge_entity(chef: Chef, ingredients: List[DataPointIngredient], dictionary,
                 target_column, result, merged='drop') -> DataPointIngredient:
    """merge entities"""
    from ... transformer import merge_keys

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]

    data = ingredient.compute()

    res_data = dict()
    for k, df in data.items():
        res_data[k] = merge_keys(df.set_index(ingredient.key),
                                 dictionary, target_column=target_column, merged=merged).reset_index()

    return DataPointIngredient.from_procedure_result(result, ingredient.key, res_data)
