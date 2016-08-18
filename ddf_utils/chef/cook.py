# -*- coding: utf-8 -*-
"""recipe cooking"""

import os
import pandas as pd
import json
import yaml
import re

from . ingredient import *
from . import config
from . procedure import *

import logging


# functions for reading/running recipe
def build_recipe(recipe_file, to_disk=False):
    """build a complete recipe file if there are includes in
    recipe file, if no includes found than return the file as is.
    """
    if re.match('.*\.json', recipe_file):
        recipe = json.load(open(recipe_file))
    else:
        recipe = yaml.load(open(recipe_file))

    # expand all files in the options
    if 'config' in recipe.keys() and 'dictionary_dir' in recipe['config'].keys():
        dict_dir = recipe['config']['dictionary_dir']
        for p in ['concepts', 'datapoints', 'entities']:
            if p not in recipe['cooking'].keys():
                continue
            for i, procedure in enumerate(recipe['cooking'][p]):
                try:
                    opt_dict = procedure['options']['dictionary']
                except KeyError:
                    continue
                if isinstance(opt_dict, str):
                    # FIXME: sometimes if dictionary_dir is not define, no error will raise
                    path = os.path.join(dict_dir, opt_dict)
                    recipe['cooking'][p][i]['options']['dictionary'] = json.load(open(path, 'r'))

    if 'include' not in recipe.keys():
        return recipe
    else:
        base_dir = os.path.dirname(recipe_file)
        recipe_dir = recipe['config']['recipes_dir']

        sub_recipes = []
        for i in recipe['include']:
            # FIXME: add support to expand user home and env vars
            if os.path.isabs(recipe_dir):
                path = os.path.join(recipe_dir, i)
            else:
                path = os.path.join(base_dir, recipe_dir, i)
            sub_recipes.append(build_recipe(path))

        for rcp in sub_recipes:
            if 'ingredients' in recipe.keys():
                ingredients = [*recipe['ingredients'], *rcp['ingredients']]
                # drop duplicated ingredients.
                rcp_dict_tmp = {}
                for v in ingredients:
                    if v['id'] not in rcp_dict_tmp.keys():
                        rcp_dict_tmp[v['id']] = v
                    else:
                        # raise error when ingredients with same ID have different contents.
                        if v != rcp_dict_tmp[v['id']]:
                            raise ValueError("Different content with same ingredient id detected: " + v['id'])
                recipe['ingredients'] = list(rcp_dict_tmp.values())
            else:
                recipe['ingredients'] = rcp['ingredients']

            if 'cooking' not in rcp.keys():
                continue

            for p in ['datapoints', 'entities', 'concepts']:
                if p not in rcp['cooking'].keys():
                    continue

                if 'cooking' in recipe.keys():
                    # currently if dictionary option is a file name it won't change
                    # to actual dictionary in that file.
                    # TODO: build all dictionary options into the recipe.
                    if p in recipe['cooking'].keys():
                        # NOTE: the included cooking procedures should be placed in front of
                        # the origin ones.
                        recipe['cooking'][p] = [*rcp['cooking'][p], *recipe['cooking'][p]]
                    else:
                        recipe['cooking'][p] = rcp['cooking'][p]
                else:
                    recipe['cooking'] = {}
                    recipe['cooking'][p] = rcp['cooking'][p]

                # TODO: drop duplicated procedures
                # if ingredients, procedure name are same, then
                # we assume the procedures are same one
                # recipe['cooking'][p] = list(
                #     unique_everseen([[proc['procedure'], proc['ingredients']]
                #                      for proc in recipe['cooking'][p]])
                # )
        if to_disk:
            yaml.dump(recipe, open('recipe.yaml', 'w'))

        return recipe


def update_recipe_last_update(recipe, outdir):
    pass


def run_recipe(recipe):
    """run the recipe.

    returns a dictionary. keys are `concepts`, `entities` and `datapoints`,
    and values are ingredients return by the procedures
    """

    config.SEARCH_PATH = recipe['config']['ddf_dir']

    logging.debug('path for searching: ' + str(config.SEARCH_PATH))

    # load ingredients
    ings = [Ingredient.from_dict(i) for i in recipe['ingredients']]
    ings_dict = dict([[i.ingred_id, i] for i in ings])

    # cooking
    # TODO: move this dict outside
    funcs = {
        'translate_column': translate_column,
        'translate_header': translate_header,
        'identity': identity,
        'merge': merge,
        'run_op': run_op,
        'filter_row': filter_row,
        'align': align,
        'filter_item': filter_item,
        'groupby': groupby,
        'accumulate': accumulate,
        'copy': copy
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
                    out = funcs[func](*ingredient, result=result, **options)
                else:
                    out = funcs[func](*ingredient, result=result)
            else:
                if 'options' in p.keys():
                    options = p['options']
                    out = funcs[func](*ingredient, **options)
                else:
                    out = funcs[func](*ingredient)
                result = out.ingred_id

            if result in ings_dict.keys():
                logging.warning("overwriting existing ingredient: " + result)

            ings_dict[result] = out

        res[k] = out  # use the last output Ingredient object as final result.

    return res


def dish_to_csv(dishes, outpath):
    for t, dish in dishes.items():

        all_data = dish.get_data()

        # get the key for datapoint
        if t == 'datapoints':
            # assuming all datapoints have same by values, so only check one item.
            name_tmp = list(all_data.keys())[0]
            df_tmp = all_data[name_tmp]
            by = df_tmp.columns.drop(name_tmp)
        else:
            by = None

        if isinstance(all_data, dict):
            for k, df in all_data.items():
                if re.match('ddf--.*.csv', k):
                    path = os.path.join(outpath, k)
                else:
                    if by is not None:
                        path = os.path.join(outpath, 'ddf--{}--{}--by--{}.csv'.format(t, k, '--'.join(by)))
                    elif k == 'concept':
                        path = os.path.join(outpath, 'ddf--{}.csv'.format(t))
                    else:  # entities
                        domain = dish.key[0]
                        if k == domain:
                            path = os.path.join(outpath, 'ddf--{}--{}.csv'.format(t, k))
                        else:
                            path = os.path.join(outpath, 'ddf--{}--{}--{}.csv'.format(t, domain, k))

                if t == 'datapoints':
                    df.to_csv(path, index=False, float_format='%.15g')
                else:
                    df.to_csv(path, index=False)
        else:
            path = os.path.join(outpath, 'ddf--{}.csv'.format(t))
            all_data.to_csv(path, index=False)
