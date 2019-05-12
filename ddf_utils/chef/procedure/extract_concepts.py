# -*- coding: utf-8 -*-

"""extract_concepts procedure for recipes"""

import logging
import numpy as np
import pandas as pd

from typing import List

from .. helpers import debuggable
from .. model.ingredient import Ingredient, ConceptIngredient
from .. model.chef import Chef


logger = logging.getLogger('extract_concepts')


@debuggable
def extract_concepts(chef: Chef, ingredients: List[Ingredient], result,
                     join=None, overwrite=None, include_keys=False) -> ConceptIngredient:
    """extract concepts from other ingredients.

    .. highlight:: yaml

    Procedure format:

    ::

       procedure: extract_concepts
       ingredients:  # list of ingredient id
         - ingredient_id_1
         - ingredient_id_2
       result: str  # new ingredient id
       options:
         join:  # optional
           base: str  # base concept ingredient id
           type: {'full_outer', 'ingredients_outer'}  # default is full_outer
         overwrite:  # overwrite some concept types
           country: entity_set
           year: time
         include_keys: true  # if we should include the primaryKeys concepts

    Parameters
    ----------
    ingredients
        any numbers of ingredient that needs to extract concepts from

    Keyword Args
    ------------
    join : dict, optional
        the base ingredient to join
    overwrite : dict, optional
        overwrite concept types for some concepts
    include_keys : bool, optional
        if we shuld include the primaryKeys of the ingredients, default to false

    See Also
    --------
    :py:func:`ddf_utils.transformer.extract_concepts` : related function in transformer
    module

    Note
    ----
    - all concepts in ingredients in the ``ingredients`` parameter will be extracted
      to a new concept ingredient
    - ``join`` option is optional; if present then the ``base`` will merge with concepts
      from ``ingredients``
    - ``full_outer`` join means get the union of concepts; ``ingredients_outer`` means
      only keep concepts from ``ingredients``

    """

    # ingredients = [chef.dag.get_node(x).evaluate() for x in ingredients]
    logger.info("extract concepts: {}".format([x.id for x in ingredients]))

    if join:
        base = chef.dag.get_node(join['base']).evaluate()
        try:
            join_type = join['type']
        except KeyError:
            join_type = 'full_outer'
        concepts = base.get_data()['concept'].set_index('concept')
    else:
        concepts = pd.DataFrame([], columns=['concept', 'concept_type']).set_index('concept')
        join_type = 'full_outer'

    new_concepts = set()

    for i in ingredients:
        data = i.get_data()
        if i.dtype in ['concepts', 'entities']:
            pks = [i.key]
        else:
            pks = i.key

        for k, df in data.items():
            if include_keys:
                cols = df.columns
            else:
                cols = [x for x in df.columns if x not in pks]

            cat_cols = df.select_dtypes(include=['category']).columns
            for col in cols:
                if col.startswith('is--'):
                    continue
                new_concepts.add(col)
                if col in concepts.index:
                    continue
                # if df.dtypes[col] == 'category':  # doesn't work
                if col in cat_cols:
                    concepts.loc[col, 'concept_type'] = 'string'
                else:
                    concepts.loc[col, 'concept_type'] = 'measure'

    if join_type == 'ingredients_outer':
        # ingredients_outer join: only keep concepts appears in ingredients
        concepts = concepts.loc[new_concepts]

    # add name column if there isn't one
    if 'name' not in concepts.columns:
        concepts['name'] = np.nan
    if 'name' not in concepts.index.values:
        concepts.loc['name', 'concept_type'] = 'string'
        concepts.loc['name', 'name'] = 'Name'
    concepts['name'] = concepts['name'].fillna(
        concepts.index.to_series().map(lambda x: str(x).replace('_', ' ').title()))

    # overwrite some of the types
    if overwrite:
        for k, v in overwrite.items():
            concepts.loc[k, 'concept_type'] = v
    if not result:
        result = 'concepts_extracted'
    return ConceptIngredient.from_procedure_result(result, 'concept',
                                                   data_computed={'concept': concepts.reset_index()})
