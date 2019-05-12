# -*- coding: utf-8 -*-

"""flatten procedure for recipes"""

import fnmatch
import logging

from typing import List

from .. helpers import debuggable, read_opt
from .. model.ingredient import DataPointIngredient
from .. model.chef import Chef


logger = logging.getLogger('flatten')


@debuggable
def flatten(chef: Chef, ingredients: List[DataPointIngredient], result, **options) -> DataPointIngredient:
    """flattening some dimensions, create new indicators.

    procedure format:

    .. code-block:: yaml

       procedure: flatten
       ingredients:
           - ingredient_to_run
       options:
           flatten_dimensions:
               - entity_1
               - entity_2
           dictionary:
               "concept_name_wildcard": "new_concept_name_template"
           skip_totals_among_entities:
               - entity_1
               - entity_2

    The ``dictionary`` can have multiple entries, for each entry the concepts that matches the key in wildcard
    matching will be flatten to the value, which should be a template string. The variables for the templates
    will be provided with a dictionary contains ``concept``, and all columns from ``flatten_dimensions`` as keys.

    Parameters
    ----------
    chef : Chef
        the Chef instance
    ingredients : list
        a list of ingredients
    result : `str`
        id of result ingredient
    skip_totals_among_entities : list
        a list of total among entities, which we don't add to new indicator names

    Keyword Args
    ------------
    flatten_dimensions: list
        a list of dimension to be flattened
    dictionary: dict
        the dictionary for old name -> new name mapping
    """
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."

    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    data = ingredient.get_data()

    logger.info("flatten: " + ingredient.id)

    flatten_dimensions = read_opt(options, 'flatten_dimensions', required=True)
    if not isinstance(flatten_dimensions, list):
        flatten_dimensions = [flatten_dimensions]
    dictionary = read_opt(options, 'dictionary', required=True)
    skip_totals_among_entities = read_opt(options, 'skip_totals_among_entities')

    newkey = [x for x in ingredient.key if x not in flatten_dimensions]
    newkey = ','.join(newkey)

    res = {}
    for from_name_tmpl, new_name_tmpl in dictionary.items():
        dfs = dict([(x, data[x]) for x in fnmatch.filter(data.keys(), from_name_tmpl)])
        for from_name, df_ in dfs.items():
            df = df_.compute()
            grouper = df.groupby(flatten_dimensions)
            for g, _ in grouper.groups.items():
                # logger.warn(g)
                # FIXME: There is an issue for pandas grouper for categorical data
                # where it will return all categories even if it's already filtered
                # it's WIP and refer to pull request #20583 for pandas.
                try:
                    df_ = grouper.get_group(g)
                except KeyError:
                    continue
                if df_.empty:
                    continue
                if not isinstance(g, tuple):
                    g = [g]
                tmpl_dict = dict(zip(flatten_dimensions, g))
                tmpl_dict['concept'] = from_name
                new_name = new_name_tmpl.format(**tmpl_dict)
                # remove totals among entities from name
                if skip_totals_among_entities is not None:
                    for e in skip_totals_among_entities:
                        new_name = new_name.replace('_' + e, '')
                    logger.info('new name w/o total among entities is {}'.format(new_name))
                if new_name in res.keys():
                    # raise ProcedureError("{} already created! check your name template please.".format(new_name))
                    logger.warning("{} already exists! It will be overwritten.".format(new_name))
                res[new_name] = df_.rename(columns={from_name: new_name}).drop(flatten_dimensions, axis=1)

    return DataPointIngredient.from_procedure_result(result, newkey, data_computed=res)
