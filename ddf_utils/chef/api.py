"""APIs for chef"""

# TODO: design and add apis.

import os
import sys
import logging
from ddf_utils.chef.dag import DAG, ProcedureNode, IngredientNode
from ddf_utils.chef.ingredient import Ingredient
from ddf_utils import config as cfg
from ddf_utils.chef.cook import build_recipe, build_dag, get_dishes
from ddf_utils.chef import procedure as pc
import ruamel.yaml as yaml
from collections import Mapping
from graphviz import Digraph


class Chef:
    """the chef api"""

    def __init__(self, dag=None, metadata=None, config=None, cooking=None, serving=None):
        if dag is None:
            self.dag = DAG()
        else:
            self.dag = dag
        if metadata is None:
            self.metadata = dict()
        else:
            self.metadata = metadata
        if config is None:
            self.config = dict()
        else:
            self.config = config
        if cooking is None:
            self.cooking = {'concepts': list(), 'datapoints': list(), 'entities': list()}
        else:
            self.cooking = cooking
        if serving is None:
            self.serving = list()
        else:
            self.serving = serving

    def add_config(self, **config):
        for k, v in config.items():
            self.config[k] = v
            if k == 'ddf_dir':
                cfg.DDF_SEARCH_PATH = v
        return self

    def add_metadata(self, **metadata):
        for k, v in metadata.items():
            self.metadata[k] = v
        return self

    def add_ingredient(self, **kwargs):
        ingredient = Ingredient.from_dict(kwargs)
        self.dag.add_node(IngredientNode(ingredient.ingred_id, ingredient, self.dag))
        return self

    def add_procedure(self, collection, procedure, ingredients, result=None, options=None):

        if procedure == 'serve':
            [self.serving.append({'id': x,
                                  'options': procedure.get('options', None)}) for x in ingredients]
            return self

        # check if procedure is supported
        try:
            getattr(pc, procedure)
        except AttributeError:
            logging.warning("{} is not a valid procedure, please double check "
                            "or register new procedure".format(procedure))

        def add_dependency(dag, upstream_id, downstream):
            if not dag.has_node(upstream_id):
                upstream = ProcedureNode(upstream_id, None, dag)
                dag.add_node(upstream)
            else:
                upstream = dag.get_node(ing)
            dag.add_dependency(upstream.node_id, downstream.node_id)

        if options is None:
            pdict = {'procedure': procedure, 'ingredients': ingredients, 'result': result}
        else:
            pdict = {'procedure': procedure, 'ingredients': ingredients, 'options': options, 'result': result}
        pnode = ProcedureNode(result, pdict, self.dag)
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

    @staticmethod
    def register_procedure(func):
        assert callable(func)
        setattr(pc, func.__name__, func)

    @property
    def ingredients(self):
        return [x.evaluate() for x in self.dag.nodes if isinstance(x, IngredientNode)]

    def run(self, serve=False):
        results = list()
        if len(self.serving) == 0:
            for k, v in self.cooking.items():
                if len(v) > 0:
                    self.serving.append({'id': v[-1]['result']})

        for dish in self.serving:
            dish_result = self.dag.get_node(dish['id']).evaluate()
            results.append(dish_result)
            if serve:
                outpath = self.config.get('out_dir', './')
                if 'options' in dish:
                    dish_result.serve(outpath, **dish['options'])
                else:
                    dish_result.serve(outpath)
        return results

    @classmethod
    def from_recipe(cls, recipe_file):
        recipe = build_recipe(recipe_file)

        dag = build_dag(recipe)
        metadata = recipe['info']
        if 'config' in recipe.keys():
            config = recipe['config']
        else:
            config = None
        cooking = dict()
        for k, v in recipe['cooking'].items():
            cooking[k] = v
        serving = get_dishes(recipe)

        return cls(dag=dag, metadata=metadata, config=config, cooking=cooking, serving=serving)

    def to_recipe(self, fp=None):
        if fp is None:
            fp = sys.stdout
        recipe = dict()
        recipe['info'] = self.metadata
        recipe['config'] = self.config
        recipe['ingredients'] = list()
        recipe['cooking'] = dict()

        for ingredient in self.ingredients:
            info = {'id': ingredient.ingred_id,
                    'dataset': ingredient.ddf.ddf_id,
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

    def to_graph(self, g=None, node=None):
        if not g:
            g = Digraph()

        if node is not None:
            g.node(node.node_id)
            r = node.get_direct_relatives(upstream=True)
            if len(r) > 0:
                for n in set(r):
                    g.edge(n.node_id, node.node_id)
                    self.to_graph(g, n)
        else:
            for k, v in self.dag.node_dict.items():
                g.node(k)
                r = v.get_direct_relatives()
                if len(r) > 0:
                    for n in set(r):
                        g.node(n.node_id)
                        g.edge(k, n.node_id)
        return g


