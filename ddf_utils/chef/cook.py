# -*- coding: utf-8 -*-
"""recipe cooking"""

import json
import yaml
import re
import os
from orderedattrdict import AttrDict
from orderedattrdict.yamlutils import AttrDictYAMLLoader

from . ingredient import Ingredient
from . dag import DAG, IngredientNode, ProcedureNode
from . helpers import read_opt
from .. import config
from . exceptions import ChefRuntimeError

import logging

logger = logging.getLogger('Chef')


def _loadfile(f):
    """load json/yaml file, into AttrDict"""
    if re.match('.*\.json', f):
        res = json.load(open(f), object_pairs_hook=AttrDict)
    else:
        res = yaml.load(open(f), Loader=AttrDictYAMLLoader)

    return res


# functions for reading/running recipe
def build_recipe(recipe_file, to_disk=False, **kwargs):
    """build a complete recipe object.

    This function will check each part of recipe, convert string (the ingredient ids,
    dictionaries file names) into actual objects.

    If there are includes in recipe file, this function will run recurivly.
    If no includes found then return the parsed object as is.

    Parameters
    ----------
    recipe_file : `str`
        path to recipe file

    Keyword Args
    ------------
    to_disk : bool
        if true, save the parsed reslut to a yaml file in working dir

    Other Parameters
    ----------------
    ddf_dir : `str`
        path to search for DDF datasets, will overwrite the contfig in recipe

    """
    recipe = _loadfile(recipe_file)

    # the base dir of recipe file. for building paths for dictionary_dir and
    # sub recipe paths.
    base_dir = os.path.dirname(recipe_file)

    # the dictionary dir to retrieve translation dictionaries
    try:
        dict_dir = recipe['config']['dictionary_dir']
        config.DICT_PATH = dict_dir
    except KeyError:
        dict_dir = config.DICT_PATH

    # expand all files in the options
    if 'cooking' in recipe.keys():
        for p in ['concepts', 'datapoints', 'entities']:
            if p not in recipe['cooking'].keys():
                continue
            for i, procedure in enumerate(recipe['cooking'][p]):
                try:
                    opt_dict = procedure['options']['dictionary']
                except KeyError:
                    continue
                if isinstance(opt_dict, str):
                    # if the option dict is str, then it should be a filename
                    if dict_dir is None:
                        raise ChefRuntimeError("dictionary_dir not found in config!")
                    if os.path.isabs(dict_dir):
                        path = os.path.join(dict_dir, opt_dict)
                    else:
                        path = os.path.join(base_dir, dict_dir, opt_dict)

                    recipe['cooking'][p][i]['options']['dictionary'] = _loadfile(path)

    # setting ddf search path if option is provided
    if 'ddf_dir' in kwargs.keys():
        if 'config' not in recipe.keys():
            recipe['config'] = AttrDict()
        recipe.config.ddf_dir = kwargs['ddf_dir']
        config.DDF_SEARCH_PATH = kwargs['ddf_dir']

    if 'include' not in recipe.keys():
        return recipe
    else:  # append sub-recipe entities into main recipe
        recipe_dir = recipe['config']['recipes_dir']

        sub_recipes = []
        for i in recipe['include']:
            # TODO: maybe add support to expand user home and env vars
            if os.path.isabs(recipe_dir):
                path = os.path.join(recipe_dir, i)
            else:
                path = os.path.join(base_dir, recipe_dir, i)
            sub_recipes.append(build_recipe(path))

        for rcp in sub_recipes:
            # appending ingredients
            if 'ingredients' in recipe.keys():
                # ingredients = [*recipe['ingredients'], *rcp['ingredients']]
                # ^ not supportted by Python < 3.5
                ingredients = []
                if 'ingredients' in recipe.keys():
                    [ingredients.append(ing) for ing in recipe['ingredients']]
                if 'ingredients' in rcp.keys():
                    [ingredients.append(ing) for ing in rcp['ingredients']]
                # drop duplicated ingredients.
                rcp_dict_tmp = {}
                for v in ingredients:
                    if v['id'] not in rcp_dict_tmp.keys():
                        rcp_dict_tmp[v['id']] = v
                    else:
                        # raise error when ingredients with same ID have different contents.
                        if v != rcp_dict_tmp[v['id']]:
                            raise ChefRuntimeError(
                                "Different content with same ingredient id detected: " + v['id'])
                recipe['ingredients'] = list(rcp_dict_tmp.values())
            else:
                recipe['ingredients'] = rcp['ingredients']

            # appending cooking procedures
            if 'cooking' not in rcp.keys():
                continue
            for p in ['datapoints', 'entities', 'concepts']:
                if p not in rcp['cooking'].keys():
                    continue
                if 'cooking' in recipe.keys():
                    if p in recipe['cooking'].keys():
                        # NOTE: the included cooking procedures should be placed in front of
                        # the origin ones.
                        # recipe['cooking'][p] = [*rcp['cooking'][p], *recipe['cooking'][p]]
                        # ^ not supportted by Python < 3.5
                        new_procs = []
                        [new_procs.append(proc) for proc in rcp['cooking'][p]]
                        [new_procs.append(proc) for proc in recipe['cooking'][p]
                         if proc not in new_procs]
                        recipe['cooking'][p] = new_procs
                    else:
                        recipe['cooking'][p] = rcp['cooking'][p]
                else:
                    recipe['cooking'] = {}
                    recipe['cooking'][p] = rcp['cooking'][p]

        return recipe


def update_recipe_last_update(recipe, outdir):
    pass


def check_dataset_availability(recipe):
    """check availability of all datasets required by the recipe.

    raise error if some dataset not available. otherwise do nothing.
    """
    try:
        ddf_dir = recipe.config.ddf_dir
    except (KeyError, AttributeError):
        ddf_dir = config.DDF_SEARCH_PATH

    datasets = set()
    for ingred in recipe.ingredients:
        datasets.add(ingred.dataset)

    not_exists = []
    for d in datasets:
        if not os.path.exists(os.path.join(ddf_dir, d)):
            not_exists.append(d)

    if len(not_exists) > 0:
        logging.critical("not enough datasets! please checkout following datasets:\n{}\n"
                         .format('\n'.join(not_exists)))
        raise ChefRuntimeError('not enough datasets')
    return


def build_dag(recipe):
    """build a DAG model for the recipe.

    For more detail for DAG model, see :py:mod:`ddf_utils.chef.dag`.
    """

    def add_dependency(dag, upstream_id, downstream):
        if not dag.has_node(upstream_id):
            upstream = ProcedureNode(upstream_id, None, dag)
            dag.add_node(upstream)
        else:
            upstream = dag.get_node(ing)
        dag.add_dependency(upstream.node_id, downstream.node_id)

    dag = DAG()
    ingredients = [Ingredient.from_dict(x) for x in recipe.ingredients]
    # ingredients_names = [x.ingred_id for x in ingredients]
    serving = []

    # adding ingredient nodes
    for i in ingredients:
        dag.add_node(IngredientNode(i.ingred_id, i, dag))

    for k, d in recipe.cooking.items():
        for proc in d:
            if proc['procedure'] == 'serve':
                [serving.append(x) for x in proc.ingredients]
                continue
            try:
                node = ProcedureNode(proc.result, proc, dag)
            except KeyError:
                logger.critical('Please set the result id for procedure: ' + proc['procedure'])
                raise
            dag.add_node(node)
            # add nodes from base ingredients
            for ing in proc.ingredients:
                add_dependency(dag, ing, node)
            # also add nodes from options
            # for now, if a key in option named base or ingredient, it will be treat as ingredients
            # TODO: see if there is better way to do
            if 'options' in proc.keys():
                options = proc['options']
                for ingredient_key in ['base', 'ingredient']:
                    if ingredient_key in options.keys():
                        add_dependency(dag, options[ingredient_key], node)
                    for opt, val in options.items():
                        if isinstance(val, AttrDict) and ingredient_key in val.keys():
                            add_dependency(dag, options[opt][ingredient_key], node)
            # detect cycles in recipe after adding all related nodes
            node.detect_downstream_cycle()
    # check if all serving ingredients are available
    for i in serving:
        if not dag.has_node(i):
            raise ChefRuntimeError('Ingredient not found: ' + i)
    if 'serving' in recipe.keys():
        if len(serving) > 0:
            raise ChefRuntimeError('can not have serve procedure and serving section at same time!')
        for i in recipe['serving']:
            if not dag.has_node(i['id']):
                raise ChefRuntimeError('Ingredient not found: ' + i['id'])
    # display the tree
    # dag.tree_view()
    return dag


def get_dishes(recipe):
    """get all dishes in the recipe"""

    if 'serving' in recipe:
        return recipe['serving']

    dishes = list()
    for _, procs in recipe['cooking'].items():
        serve_proc_exists = False
        for p in procs:
            if p['procedure'] == 'serve':
                serve_proc_exists = True
                for i in p['ingredients']:
                    try:
                        dishes.append({'id': i, 'options': p['options']})
                    except KeyError:
                        dishes.append({'id': i, 'options': dict()})
        if not serve_proc_exists:
            logger.warning('no serve procedure found, serving the last result: ' + p['result'])
            dishes.append({'id': p['result'], 'options': dict()})

    return dishes


def run_recipe(recipe, serve=False, outpath=None):
    """run the recipe.

    returns a dictionary. keys are `concepts`, `entities` and `datapoints`,
    and values are ingredients defined in the `serve` procedures or `serving` section.
    for example:

    .. code-block:: python

        {
            "concepts": [{"ingredient": DataFrame1, "options": None}]
            "datapoints": [
                {
                    "ingredient": DataFrame2,
                    "options": {"digits": 5}
                },
                {
                    "ingredient": DataFrame3,
                    "options": {"digits": 1}
                },
            ]
        }

    """
    try:
        config.DDF_SEARCH_PATH = recipe['config']['ddf_dir']
    except KeyError:
        if not config.DDF_SEARCH_PATH:
            raise ChefRuntimeError("no ddf_dir configured, please check your recipe")
    logging.info('path for searching DDF: ' + str(config.DDF_SEARCH_PATH))

    # make a copy recipe, don't change the origin recipe
    from copy import deepcopy
    recipe_ = deepcopy(recipe)

    # check all datasets availability
    check_dataset_availability(recipe_)

    # create DAG of recipe
    dag = build_dag(recipe_)

    # check all ingredients availability
    for root in dag.roots:
        root.detect_missing_dependency()

    # now run the recipe
    dishes = get_dishes(recipe_)

    results = list()
    for dish in dishes:
        dish_result = dag.get_node(dish['id']).evaluate()
        results.append(dish_result)
        if serve:
            if 'options' in dish:
                dish_result.serve(outpath, **dish['options'])
            else:
                dish_result.serve(outpath)

    return results
