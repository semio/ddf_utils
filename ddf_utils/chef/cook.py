# -*- coding: utf-8 -*-
"""recipe cooking"""

import json
import logging
import os
import re
import sys
from collections import Mapping
from copy import deepcopy
from time import time

from dask import delayed

import ruamel.yaml as yaml
from graphviz import Digraph

from ddf_utils.chef.dag import DAG, IngredientNode, ProcedureNode
from ddf_utils.chef.exceptions import ChefRuntimeError
from ddf_utils.chef.helpers import get_procedure
from ddf_utils.chef.ingredient import Ingredient

logger = logging.getLogger('Chef')


def _loadfile(f):
    """load json/yaml file, into AttrDict"""
    if re.match('.*\.json', f):
        res = json.load(open(f))
    else:
        res = yaml.load(open(f), Loader=yaml.Loader)

    return res


class Chef:
    """the chef api"""

    def __init__(self, dag=None, metadata=None, config=None, cooking=None, serving=None, recipe=None):
        if dag is None:
            self.dag = DAG()
        else:
            self.dag = dag
        if metadata is None:
            self.metadata = dict()
        else:
            self.metadata = metadata
        if config is None:
            self._config = dict()
        else:
            self._config = config
        if cooking is None:
            self.cooking = {'concepts': list(), 'datapoints': list(), 'entities': list()}
        else:
            self.cooking = cooking
        if serving is None:
            self._serving = list()
        else:
            self._serving = serving

        self._recipe = recipe
        self.ddf_object_cache = {}

    @property
    def config(self):
        if 'ddf_dir' not in self._config.keys():
            logger.warning('ddf_dir not configured, assuming current directory')
            self._config['ddf_dir'] = os.path.abspath('./')
        return self._config

    @property
    def serving(self):
        if len(self._serving) == 0:
            for k, v in self.cooking.items():
                if len(v) > 0:
                    self._serving.append({'id': v[-1]['result']})
        return self._serving

    @serving.setter
    def serving(self, serving):
        self._serving = serving

    @classmethod
    def from_recipe(cls, recipe_file, **config):
        recipe = Chef._build_recipe(recipe_file)
        chef = cls()

        if 'info' in recipe.keys():
            chef.add_metadata(**recipe['info'])
        if 'config' in recipe.keys():
            chef.add_config(**recipe['config'])
        chef.add_config(**config)

        assert 'ingredients' in recipe.keys(), "recipe must have ingredients section"
        assert 'cooking' in recipe.keys(), "recipe must have cooking section!"

        for ingred in recipe['ingredients']:
            chef.add_ingredient(**ingred)

        for k, ps in recipe['cooking'].items():
            for p in ps:
                result = p.get('result', None)
                options = p.get('options', None)
                chef.add_procedure(k, p['procedure'], p['ingredients'], result, options)

        chef.serving = Chef._get_dishes(recipe)
        return chef

    @property
    def ingredients(self):
        return [x.evaluate() for x in self.dag.nodes if isinstance(x, IngredientNode)]

    def copy(self):
        return Chef(dag=self.dag.copy(), metadata=deepcopy(self.metadata),
                    config=deepcopy(self._config), cooking=deepcopy(self.cooking),
                    serving=deepcopy(self._serving), recipe=deepcopy(self._recipe))

    def validate(self):
        """validate if the chef is good to run.

        The following will be tested:

        1. check if datasets required by ingredients are available
        2. check if procedures are available
        3. check if the DAG is valid. i.e no dependency cycle, no missing dependency.
        """
        # 1. check dataset availability
        ddf_dir = self.config['ddf_dir']
        datasets = set()
        for ingred in self.ingredients:
            if ingred.ddf_id:
                datasets.add(ingred.ddf_id)
        not_exists = []
        for d in datasets:
            if not os.path.exists(os.path.join(ddf_dir, d)):
                not_exists.append(d)
        if len(not_exists) > 0:
            logger.critical("not enough datasets! please checkout following datasets:\n{}\n"
                            .format('\n'.join(not_exists)))
            raise ChefRuntimeError('not enough datasets')

        # 2. check procedure availability
        for k, ps in self.cooking.items():
            for p in ps:
                try:
                    get_procedure(p['procedure'], self.config.get('procedure_dir'))
                except (AttributeError, ImportError):
                    logger.warning("{} is not a valid procedure, please double check "
                                   "or register new procedure".format(p['procedure']))
                    raise ChefRuntimeError('procedures not ready')

        # 3. check if the DAG is valid
        for ing in self.serving:
            self.dag.get_node(ing['id']).detect_downstream_cycle()
            self.dag.get_node(ing['id']).detect_missing_dependency()

    def add_config(self, **config):
        """add configs, all keyword args will be added/replace existing in config dictionary"""
        for k, v in config.items():
            self._config[k] = v
        return self

    def add_metadata(self, **metadata):
        """add metadata, all keyword args will be added/replace existing in metadata dictionary"""
        for k, v in metadata.items():
            self.metadata[k] = v
        return self

    def add_ingredient(self, **kwargs):
        """add a new ingredient in DAG.

        keyword arguments will send as a dictionary to the ``dictionary`` keyword of
        :py:meth:`ddf_utils.chef.ingredient.Ingredient.from_dict` method. Check ``from_dict()``
        doc for available keywords
        """
        ingredient = Ingredient.from_dict(chef=self, dictionary=kwargs)
        self.dag.add_node(IngredientNode(ingredient.ingred_id, ingredient, self))
        return self

    def add_procedure(self, collection, procedure, ingredients, result=None, options=None):

        if procedure == 'serve':
            [self._serving.append({'id': x,
                                   'options': options}) for x in ingredients]
            return self

        # check if procedure is supported
        try:
            get_procedure(procedure, self.config.get('procedure_dir', None))
        except (AttributeError, ImportError):
            logging.warning("{} is not a valid procedure, please double check "
                            "or register new procedure".format(procedure))

        assert result is not None, "result is mandatory for {}".format(procedure)

        def add_dependency(dag, upstream_id, downstream):
            if not dag.has_node(upstream_id):
                upstream = ProcedureNode(upstream_id, None, self)
                dag.add_node(upstream)
            else:
                upstream = dag.get_node(upstream_id)
            dag.add_dependency(upstream.node_id, downstream.node_id)

        # create inline ingredients and change definition to actual id
        for idx, ing in enumerate(ingredients):
            if isinstance(ing, dict):
                ingredient = Ingredient.from_dict(chef=self, dictionary=ing)
                self.dag.add_node(IngredientNode(ingredient.ingred_id, ingredient, self))
                ingredients[idx] = ingredient.ingred_id

        if options is None:
            pdict = {'procedure': procedure, 'ingredients': ingredients, 'result': result}
        else:
            pdict = {'procedure': procedure, 'ingredients': ingredients, 'options': options, 'result': result}
        pnode = ProcedureNode(result, pdict, self)
        self.dag.add_node(pnode)
        self.cooking[collection].append(pnode.procedure)

        # adding dependencies
        for ing in ingredients:
            add_dependency(self.dag, ing, pnode)

        if options is not None:
            for ingredient_key in ['base', 'ingredient']:
                if ingredient_key in options.keys():
                    add_dependency(self.dag, options[ingredient_key], pnode)
                for opt, val in options.items():
                    if isinstance(val, Mapping) and ingredient_key in val.keys():
                        add_dependency(self.dag, options[opt][ingredient_key], pnode)
        return self

    def add_dish(self, ingredients, options=None):
        existing_dish = [x['id'] for x in self._serving]
        for ing in ingredients:
            if ing in existing_dish:
                logger.warning('dish already exist: {}, skipping...'.format(ing))
            self._serving.append({'id': ing, 'options': options})

    @staticmethod
    def register_procedure(func):
        from ddf_utils.chef import procedure as pc
        assert callable(func)
        setattr(pc, func.__name__, func)

    def run(self, serve=False, outpath=None):
        self.validate()

        results = [self.dag.get_node(x['id']).evaluate() for x in self.serving]

        if serve:
            if not outpath:
                outpath = self.config.get('out_dir', './')

            @delayed
            def _serve_all(dishes):
                for dish in dishes:
                    dish_result = self.dag.get_node(dish['id']).evaluate()

                    if 'options' in dish and dish['options'] is not None:
                        dish_result.serve(outpath, **dish['options'])
                    else:
                        dish_result.serve(outpath)

            _serve_all(self.serving).compute()

        return results

    def to_recipe(self, fp=None):
        if fp is None:
            fp = sys.stdout
        recipe = dict()
        recipe['info'] = self.metadata
        recipe['config'] = self.config
        recipe['ingredients'] = list()
        recipe['cooking'] = dict()
        recipe['serving'] = self.serving

        for ingredient in self.ingredients:
            info = {'id': ingredient.ingred_id,
                    'dataset': ingredient.ddf_id,
                    'key': ingredient.key,
                    'values': ingredient.values,
                    }
            if ingredient.row_filter is not None:
                info['row_filter'] = ingredient.row_filter
            recipe['ingredients'].append(info)

        for k, v in self.cooking.items():
            if len(v) > 0:
                recipe['cooking'][k] = v

        yaml.round_trip_dump(recipe, fp)

    def to_graph(self, node=None):

        def process_node(g_, k_, recursive=False):
            pc = self.dag.get_node(k_).procedure['procedure']
            node_name = pc + str(time())
            g_.node(node_name, label=pc, shape='box')
            g_.edge(node_name, k_)

            for ing in self.dag.get_node(k_).get_direct_relatives(upstream=True):
                g_.edge(ing.node_id, node_name)
                if recursive:
                    process_node(g_, ing.node_id)

        g = Digraph()
        g.attr(rankdir='TB', fontsize='8')

        if node is None:
            # define all nodes
            to_serve = [x['id'] for x in self.serving]
            for k, v in self.dag.node_dict.items():
                if k in to_serve:
                    g.node(k, color='red')
                elif isinstance(self.dag.get_node(k), IngredientNode):
                    g.node(k, color='blue')
                else:
                    g.node(k)
                if isinstance(self.dag.get_node(k), ProcedureNode):
                    process_node(g, k)
        else:
            assert node in self.dag.node_dict.keys()
            g.node(node)
            process_node(g, node, recursive=True)

        return g

    @staticmethod
    def _build_recipe(recipe_file, to_disk=False, **kwargs):
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
        if 'config' not in recipe.keys():
            dict_dir = None
            external_csv_dir = base_dir
            recipe_dir = base_dir
        else:
            dict_dir = recipe['config'].get('dictionary_dir', None)
            external_csv_dir = recipe['config'].get('external_csv_dir', base_dir)
            recipe_dir = recipe['config'].get('recipes_dir', base_dir)

        def external_csv_abs_path(ing):
            """change the csv file in `data` to full path"""
            if isinstance(ing, dict) and 'data' in ing.keys() and isinstance(ing['data'], str):
                if not os.path.isabs(ing['data']):
                    ing['data'] = os.path.join(external_csv_dir, ing['data'])
            return ing

        # expand all files in the options
        if 'cooking' in recipe.keys():
            for p in ['concepts', 'datapoints', 'entities']:
                if p not in recipe['cooking'].keys():
                    continue
                for i, procedure in enumerate(recipe['cooking'][p]):

                    procedure['ingredients'] = [external_csv_abs_path(ing) for ing in procedure['ingredients']]
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

        if 'ingredients' in recipe.keys():
            recipe['ingredients'] = [external_csv_abs_path(ing) for ing in recipe['ingredients']]

        if 'include' not in recipe.keys():
            return recipe
        else:  # append sub-recipe entities into main recipe
            sub_recipes = []
            for i in recipe['include']:
                if os.path.isabs(recipe_dir):
                    path = os.path.join(recipe_dir, i)
                else:
                    path = os.path.expanduser(os.path.join(base_dir, recipe_dir, i))
                sub_recipes.append(Chef._build_recipe(path))

            for rcp in sub_recipes:
                # appending ingredients
                if 'ingredients' in recipe.keys():
                    # ingredients = [*recipe['ingredients'], *rcp['ingredients']]
                    # ^ not supportted by Python < 3.5
                    ingredients = []
                    if 'ingredients' in recipe.keys():
                        [ingredients.append(ing) for ing in recipe['ingredients']]
                    if 'ingredients' in rcp.keys():
                        [ingredients.append(external_csv_abs_path(ing)) for ing in rcp['ingredients']]
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

    @staticmethod
    def _get_dishes(recipe):
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
                logger.warning('no serve procedure found, will serve the last result: ' + p['result'])
                dishes.append({'id': p['result'], 'options': dict()})

        return dishes
