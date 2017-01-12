# -*- coding: utf-8 -*-
"""recipe cooking"""

import json
import yaml
import re
import os
from orderedattrdict import AttrDict
from orderedattrdict.yamlutils import AttrDictYAMLLoader

from . ingredient import BaseIngredient, Ingredient, ProcedureResult
from . dag import DAG, IngredientNode, ProcedureNode
from . helpers import read_opt
from .. import config
from . exceptions import *

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
                # ingredients = [*recipe['ingredients'], *rcp['ingredients']]  # not supportted by Python < 3.5
                ingredients = []
                [ingredients.append(ing) for ing in recipe['ingredients']]
                [ingredients.append(ing) for ing in rcp['ingredients']]
                # drop duplicated ingredients.
                rcp_dict_tmp = {}
                for v in ingredients:
                    if v['id'] not in rcp_dict_tmp.keys():
                        rcp_dict_tmp[v['id']] = v
                    else:
                        # raise error when ingredients with same ID have different contents.
                        if v != rcp_dict_tmp[v['id']]:
                            raise ChefRuntimeError("Different content with same ingredient id detected: " + v['id'])
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
                        # recipe['cooking'][p] = [*rcp['cooking'][p], *recipe['cooking'][p]]  # not supportted by Python < 3.5
                        new_procs = []
                        [new_procs.append(proc) for proc in rcp['cooking'][p]]
                        [new_procs.append(proc) for proc in recipe['cooking'][p] if proc not in new_procs]
                        recipe['cooking'][p] = new_procs
                    else:
                        recipe['cooking'][p] = rcp['cooking'][p]
                else:
                    recipe['cooking'] = {}
                    recipe['cooking'][p] = rcp['cooking'][p]

        if to_disk:
            yaml.dump(recipe, open('recipe_dump.yaml', 'w'))

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
        logging.critical("not enough datasets! please checkout following datasets:\n{}\n"\
                            .format('\n'.join(not_exists)))
        raise ChefRuntimeError('not enough datasets')
    return


def build_dag(recipe):
    """build a DAG model for the recipe.

    For more detail for DAG model, see :py:mod:`ddf_utils.chef.dag`.
    """

    def add_dependency(dag, upstream_id, downstream):
        if not dag.has_task(upstream_id):
            upstream = ProcedureNode(upstream_id, None, dag)
            dag.add_task(upstream)
        else:
            upstream = dag.get_task(ing)
        dag.add_dependency(upstream.node_id, downstream.node_id)

    dag = DAG()
    ingredients = [Ingredient.from_dict(x) for x in recipe.ingredients]
    ingredients_names = [x.ingred_id for x in ingredients]
    serving = []

    # adding ingredient nodes
    for i in ingredients:
        dag.add_task(IngredientNode(i.ingred_id, i, dag))

    for k, d in recipe.cooking.items():
        for proc in d:
            if proc['procedure'] == 'serve':
                [serving.append(x) for x in proc.ingredients]
                continue
            try:
                task = ProcedureNode(proc.result, proc, dag)
            except KeyError:
                logger.critical('Please set the result id for procedure: ' + proc['procedure'])
                raise
            dag.add_task(task)
            # add nodes from base ingredients
            for ing in proc.ingredients:
                add_dependency(dag, ing, task)
            # also add nodes from options
            # for now, if a key in option named base or ingredient, it will be treat as ingredients
            # TODO: see if there is better way to do
            if 'options' in proc.keys():
                options = proc['options']
                for ingredient_key in ['base', 'ingredient']:
                    if ingredient_key in options.keys():
                        add_dependency(dag, options[ingredient_key], task)
                    for opt, val in options.items():
                        if isinstance(val, AttrDict) and ingredient_key in val.keys():
                            add_dependency(dag, options[opt][ingredient_key], task)
            # detect cycles in recipe after adding all related nodes
            task.detect_downstream_cycle()
    # check if all serving ingredients are available
    for i in serving:
        if not dag.has_task(i):
            raise ChefRuntimeError('Ingredient not found: ' + i)
    if 'serving' in recipe.keys():
        if len(serving) > 0:
            raise ChefRuntimeError('can not have serve procedure and serving section at same time!')
        for i in recipe['serving']:
            if not dag.has_task(i['id']):
                raise ChefRuntimeError('Ingredient not found: ' + i['id'])
    # display the tree
    # dag.tree_view()
    return dag


def run_recipe(recipe):
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

    # check all datasets availability
    check_dataset_availability(recipe)

    # create DAG of recipe
    dag = build_dag(recipe)

    # now run the recipe
    dishes = {}

    for k, pceds in recipe['cooking'].items():

        print("running "+k)
        dishes[k] = list()

        for p in pceds:
            func = p['procedure']
            if func == 'serve':
                ingredients = [dag.get_task(x).evaluate() for x in p['ingredients']]
                opts = read_opt(p, 'options', default=dict())
                [dishes[k].append({'ingredient': i, 'options': opts}) for i in ingredients]
                continue
            out = dag.get_task(p['result']).evaluate()
        # if there is no seving procedures/section, use the last output Ingredient object as final result.
        if len(dishes[k]) == 0 and 'serving' not in recipe.keys():
            logger.warning('serving last procedure output for {}: {}'.format(k, out.ingred_id))
            dishes[k].append({'ingredient': out, 'options': dict()})
    # update dishes when there is serving section
    if 'serving' in recipe.keys():
        for i in recipe['serving']:
            opts = read_opt(i, 'options', default=dict())
            ing = dag.get_task(i['id']).evaluate()
            if ing.dtype in dishes.keys():
                dishes[ing.dtype].append({'ingredient': ing, 'options': opts})
            else:
                dishes[ing.dtype] = [{'ingredient': ing, 'options': opts}]
    return dishes


def dish_to_csv(dishes, outpath):
    """save the recipe output to disk"""
    for t, ds in dishes.items():
        for dish in ds:
            dish['ingredient'].serve(outpath, **dish['options'])
