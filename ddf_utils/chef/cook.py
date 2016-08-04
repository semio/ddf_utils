# -*- coding: utf-8 -*-
"""recipe cooking"""

import os
import pandas as pd
import json
import yaml
import re

from . ingredient import *
from . procedure import *

import logging


# functions for reading/running recipe
def build_recipe(recipe_file):
    """build a complete recipe file if there are includes in
    recipe file, if no includes found than return the file as is.
    """
    if re.match('.*\.json', recipe_file):
        recipe = json.load(open(recipe_file))
    else:
        recipe = yaml.load(open(recipe_file))

    if 'include' not in recipe.keys():
        return recipe
    else:
        base_dir = os.path.dirname(recipe_file)
        recipe_dir = recipe['config']['recipes_dir']

        sub_recipes = []
        for i in recipe['include']:
            path = os.path.join(base_dir, recipe_dir, i)
            sub_recipes.append(build_recipe(path))

        for rcp in sub_recipes:
            if 'ingredients' in recipe.keys():
                ingredients = [*recipe['ingredients'], *rcp['ingredients']]
                # drop duplicated ingredients.
                # TODO: it's assumed that ingredients with same id have same key/value.
                # need to confirm if it is true.
                recipe['ingredients'] = list({v['id']:v for v in ingredients}.values())
            else:
                recipe['ingredients'] = rcp['ingredients']

            for p in ['datapoints', 'entities', 'concepts']:
                if p not in rcp['cooking'].keys():
                    continue

                if 'cooking' in recipe.keys():
                    if p in recipe['cooking'].keys():
                        recipe['cooking'][p] = [*recipe['cooking'][p], *rcp['cooking'][p]]
                    else:
                        recipe['cooking'][p] = rcp['cooking'][p]
                else:
                    recipe['cooking'] = {}
                    recipe['cooking'][p] = rcp['cooking'][p]

        return recipe


def run_recipe(recipe_file):
    """run the recipe."""
    recipe = build_recipe(recipe_file)

    # load ingredients
    ings = [Ingredient.from_dict(i) for i in recipe['ingredients']]
    ings_dict = dict([[i.ingred_id, i] for i in ings])

    # cooking
    funcs = {
        'translate_column': translate_column,
        'translate_header': translate_header,
        'identity': identity,
        'merge': merge,
        'run_op': run_op,
        'filter': filter_row,
        'align': align,
        'filter_item': filter_item,
        'groupby': groupby
    }

    res = {}

    for k, pceds in recipe['cooking'].items():

        print("running "+k)

        for p in pceds:
            func = p['procedure']

            if func not in funcs.keys():
                raise NotImplementedError("Not supported: " + func)

            ingredient = [ings_dict[i] for i in p['ingredients']]

            if 'result' in p.keys():
                result = p['result']
                if 'options' in p.keys():
                    options = p['options']
                    ings_dict[result] = funcs[func](*ingredient, result=result, **options)
                else:
                    ings_dict[result] = funcs[func](*ingredient, result=result)
            else:
                if 'options' in p.keys():
                    options = p['options']
                    out = funcs[func](*ingredient, **options)
                else:
                    out = funcs[func](*ingredient)

        res[k] = out.get_data()

    return res


def dish_to_csv(dishes, outpath):
    for t, dish in dishes.items():

        # get the key for datapoint
        if t == 'datapoints':
            name_tmp = list(dish.keys())[0]
            df_tmp = dish[name_tmp]
            by = df_tmp.columns.drop(name_tmp)
        else:
            by = None

        if isinstance(dish, dict):
            for k, df in dish.items():
                if re.match('ddf--.*.csv', k):
                    path = os.path.join(outpath, k)
                else:
                    if by is not None:
                        path = os.path.join(outpath, 'ddf--{}--{}--by--{}.csv'.format(t, k, '--'.join(by)))
                    elif k == 'concept':
                        path = os.path.join(outpath, 'ddf--{}.csv'.format(t))
                    else:
                        path = os.path.join(outpath, 'ddf--{}--{}.csv'.format(t, k))

                if t == 'datapoints':
                    df.to_csv(path, index=False, float_format='%.2f')
                else:
                    df.to_csv(path, index=False)
        else:
            path = os.path.join(outpath, 'ddf--{}.csv'.format(t))
            dish.to_csv(path, index=False)
