# -*- coding: utf-8 -*-

"""translate_column procedures for recipes"""

import logging
from typing import List

from .. helpers import debuggable, build_dictionary, read_opt
from .. model.ingredient import Ingredient, get_ingredient_class
from .. model.chef import Chef

from ddf_utils.str import to_concept_id

logger = logging.getLogger('translate_column')


@debuggable
def translate_column(chef: Chef, ingredients: List[Ingredient], result, dictionary,
                     column, *, target_column=None, not_found='drop',
                     ambiguity='prompt', ignore_case=False, value_modifier=None) -> Ingredient:
    """Translate column values.

    Procedure format:

    .. code-block:: yaml

       procedure: translate_column
       ingredients:  # list of ingredient id
         - ingredient_id
       result: str  # new ingredient id
       options:
         column: str  # the column to be translated
         target_column: str  # optional, the target column to store the translated data
         not_found: {'drop', 'include', 'error'}  # optional, the behavior when there is values not
                                                  # found in the mapping dictionary, default is 'drop'
         ambiguity: {'prompt', 'skip', 'error'}  # optional, the behavior when there is ambiguity
                                                 # in the dictionary
         dictionary: str or dict  # file name or mappings dictionary

    If base is provided in dictionary, key and value should also in dictionary.
    In this case chef will generate a mapping dictionary using the base ingredient.
    The dictionary format will be:

    .. code-block:: yaml

       dictionary:
         base: str  # ingredient name
         key: str or list  # the columns to be the keys of the dictionary, can accept a list
         value: str  # the column to be the values of the the dictionary, must be one column

    Parameters
    ----------
    chef : Chef
        The Chef the procedure will run on
    ingredients : list
        A list of ingredient id in the dag to translate

    Keyword Args
    ------------
    dictionary: dict
        A dictionary of oldname -> newname mappings.
        If 'base' is provided in the dictionary, 'key' and 'value' should also in the dictionary.
        See :py:func:`ddf_utils.transformer.translate_column` for more on how this is handled.
    column: `str`
        the column to be translated
    target_column : `str`, optional
        the target column to store the translated data. If this is not set then the `column`
        column will be replaced
    not_found : {'drop', 'include', 'error'}, optional
        the behavior when there is values not found in the mapping dictionary, default is 'drop'
    ambiguity : {'prompt', 'skip', 'error'}, optional
        the behavior when there is ambiguity in the dictionary, default is 'prompt'
    value_modifier : `str`, optional
        a function to modify new column values, default is None

    See Also
    --------
    :py:func:`ddf_utils.transformer.translate_column` : related function in transformer module
    """
    from ... transformer import translate_column as tc
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."

    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info("translate_column: " + ingredient.id)

    if target_column is None:
        target_column = column

    di = ingredient.get_data()
    new_data = dict()

    # modifier
    value_modifier = read_opt(dictionary, 'value_modifier', default=None, method='pop')
    if value_modifier == 'to_concept_id':
        modifier = to_concept_id
    else:
        # TODO: accept more modifiers
        if value_modifier is not None:
            logger.warning("for now only `to_concept_id` is accepted")
        modifier = None

    # build the dictionary
    dictionary_ = build_dictionary(chef, dictionary, ignore_case, value_modifier=modifier)
    dict_type = 'inline'
    base_df = None

    # modifier

    for k, df in di.items():
        logger.debug("running on: " + k)
        if df.dtypes[column].name == "category":
            categorical_col = True
        else:
            categorical_col = False

        if categorical_col:
            df[column] = df[column].astype('str')
        new_data[k] = tc(df, column, dict_type, dictionary_, target_column, base_df,
                         not_found, ambiguity, ignore_case)
        # TODO: astype() returns unknown category, but it's better to be known.
        if categorical_col:
            new_data[k][target_column] = new_data[k][target_column].astype("category")

    if not result:
        result = ingredient.id + '-translated'
    return get_ingredient_class(ingredient.dtype).from_procedure_result(
        result, ingredient.key, data_computed=new_data)
