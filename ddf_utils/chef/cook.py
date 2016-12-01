# -*- coding: utf-8 -*-
"""recipe cooking"""

import json
import yaml
from orderedattrdict import AttrDict
from orderedattrdict.yamlutils import AttrDictYAMLLoader

from . ingredient import *
from .. import config
from . procedure import *
from .. str import format_float_digits

import logging

# supported procedures, import from procedure.py
supported_procs = {
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
    'copy': copy,
    'add_concepts': add_concepts
}


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
                        [new_procs.append(proc) for proc in recipe['cooking'][p]]
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

    check_dataset_availability(recipe)

    # load ingredients
    ings = [Ingredient.from_dict(i) for i in recipe['ingredients']]
    ings_dict = dict([[i.ingred_id, i] for i in ings])

    # cooking
    funcs = supported_procs
    dishes = {}

    for k, pceds in recipe['cooking'].items():

        print("running "+k)
        dishes[k] = list()

        for p in pceds:
            func = p['procedure']

            if func not in funcs.keys() and func != 'serve':
                raise NotImplementedError("Not supported: " + func)

            ingredient = [ings_dict[i] for i in p['ingredients']]

            if func == 'serve':
                for i in ingredient:
                    dishes[k].append(i)
                continue

            # change the 'base'/'source_ingredients' option to actual ingredient
            # TODO: find better way to handle the ingredients in options
            if 'options' in p.keys():
                options = p['options']
                if 'base' in options.keys():
                    options['base'] = ings_dict[options['base']]
                if 'source_ingredients' in options.keys():
                    options['source_ingredients'] = [ings_dict[x] for x in options['source_ingredients']]

            if 'result' in p.keys():
                result = p['result']
                if 'options' in p.keys():
                    out = funcs[func](*ingredient, result=result, **options)
                else:
                    out = funcs[func](*ingredient, result=result)
            else:
                if 'options' in p.keys():
                    out = funcs[func](*ingredient, **options)
                else:
                    out = funcs[func](*ingredient)
                result = out.ingred_id

            if result in ings_dict.keys():
                logging.warning("overwriting existing ingredient: " + result)
            ings_dict[result] = out

        # if there is no seving procedures, use the last output Ingredient object as final result.
        if len(dishes[k]) == 0:
            logger.warning('serving last procedure output for {}: {}'.format(k, out.ingred_id))
            dishes[k].append(out)
    return dishes


def dish_to_csv(dishes, outpath):
    for t, ds in dishes.items():
        for dish in ds:
            all_data = dish.get_data()
            if isinstance(all_data, dict):
                for k, df in all_data.items():
                    if re.match('ddf--.*.csv', k):
                        path = os.path.join(outpath, k)
                    else:
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
