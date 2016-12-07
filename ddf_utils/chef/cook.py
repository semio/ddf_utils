# -*- coding: utf-8 -*-
"""recipe cooking"""

import json
import yaml
from orderedattrdict import AttrDict
from orderedattrdict.yamlutils import AttrDictYAMLLoader

from . ingredient import *
from . dag import DAG, IngredientNode, ProcedureNode
from .. import config
from . procedure import *
from .. str import format_float_digits

import logging


def _loadfile(f):
    """load json/yaml file, into AttrDict"""
    if re.match('.*\.json', f):
        res = json.load(open(f), object_pairs_hook=AttrDict)
    else:
        res = yaml.load(open(f), Loader=AttrDictYAMLLoader)

    return res


# functions for reading/running recipe
def build_recipe(recipe_file, to_disk=False):
    """build a complete recipe file if there are includes in
    recipe file, if no includes found than return the file as is.
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
                        raise KeyError("dictionary_dir not found in config!")
                    if os.path.isabs(dict_dir):
                        path = os.path.join(dict_dir, opt_dict)
                    else:
                        path = os.path.join(base_dir, dict_dir, opt_dict)

                    recipe['cooking'][p][i]['options']['dictionary'] = _loadfile(path)

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
                            raise ValueError("Different content with same ingredient id detected: " + v['id'])
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
    ddf_dir = recipe.config.ddf_dir

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
        raise ValueError('not enough datasets')
    return


def build_dag(recipe):

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
            if 'options' in proc.keys():
                options = proc['options']
                if 'base' in options.keys():
                    add_dependency(dag, options['base'], task)
                for opt in options.keys():
                    if isinstance(opt, dict) and 'base' in opt.keys():
                        add_dependency(dag, options[opt]['base'], task)
            # detect cycles in recipe after adding all related nodes
            task.detect_downstream_cycle()
    # check if all serving ingredients are available
    for i in serving:
        if not dag.has_task(i):
            raise ValueError('Ingredient not found: ' + i)
    if 'serving' in recipe.keys():
        for i in recipe['serving']:
            if not dag.has_task(i):
                raise ValueError('Ingredient not found: ' + i)
    # display the tree
    # dag.tree_view()
    return dag


def run_recipe(recipe):
    """run the recipe.

    returns a dictionary. keys are `concepts`, `entities` and `datapoints`,
    and values are ingredients return by the procedures
    """
    try:
        config.DDF_SEARCH_PATH = recipe['config']['ddf_dir']
    except KeyError:
        if not config.DDF_SEARCH_PATH:
            raise ValueError("no ddf_dir configured, please check your recipe")
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
                [dishes[k].append(i) for i in ingredients]
                continue
            out = dag.get_task(p['result']).evaluate()
        # if there is no seving procedures/section, use the last output Ingredient object as final result.
        if len(dishes[k]) == 0 and 'serving' not in recipe.keys():
            logger.warning('serving last procedure output for {}: {}'.format(k, out.ingred_id))
            dishes[k].append(out)
    # update dishes when there is serving section
    if 'serving' in recipe.keys():
        for i in recipe['serving']:
            ing = dag.get_task(i).evaluate()
            if ing.dtype in dishes.keys():
                dishes[ing.dtype].append(ing)
            else:
                dishes[ing.dtype] = [ing]
    return dishes


def dish_to_csv(dishes, outpath):
    for t, ds in dishes.items():
        for dish in ds:
            all_data = dish.get_data()
            if isinstance(all_data, dict):
                for k, df in all_data.items():
                    # change boolean into string
                    for i, v in df.dtypes.iteritems():
                        if v == 'bool':
                            df[i] = df[i].map(lambda x: str(x).upper())
                    if t == 'datapoints':
                        by = dish.key_to_list()
                        path = os.path.join(outpath, 'ddf--{}--{}--by--{}.csv'.format(t, k, '--'.join(by)))
                    elif t == 'concepts':
                        path = os.path.join(outpath, 'ddf--{}.csv'.format(t))
                    elif t == 'entities':
                        domain = dish.key[0]
                        if k == domain:
                            path = os.path.join(outpath, 'ddf--{}--{}.csv'.format(t, k))
                        else:
                            path = os.path.join(outpath, 'ddf--{}--{}--{}.csv'.format(t, domain, k))
                    else:
                        raise ValueError('Not a correct collection: ' + t)

                    if t == 'datapoints':
                        df = df.set_index(by)
                        if not np.issubdtype(df[k].dtype, np.number):
                            try:
                                df[k] = df[k].astype(float)
                                # TODO: make floating precision an option
                                df[k] = df[k].map(lambda x: format_float_digits(x, 5))
                            except ValueError:
                                logging.warning("data not numeric: " + k)
                        else:
                            df[k] = df[k].map(lambda x: format_float_digits(x, 5))
                        df[[k]].to_csv(path, encoding='utf8')
                    else:
                        df.to_csv(path, index=False, encoding='utf8')
            else:
                path = os.path.join(outpath, 'ddf--{}.csv'.format(t))
                all_data.to_csv(path, index=False, encoding='utf8')
