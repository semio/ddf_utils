# -*- coding: utf-8 -*-
"""recipe cooking"""

import json
import yaml
import re
import os
from orderedattrdict import AttrDict
from orderedattrdict.yamlutils import AttrDictYAMLLoader

from . helpers import read_opt, mkfunc
from functools import reduce
from .. import config
from . exceptions import ChefRuntimeError
from ..model.ddf import Dataset
from . import procedure

import logging

logger = logging.getLogger('Chef')


class Pipeline:
    def __init__(self, name, procedures, chef=None, result=None):
        self.name = name
        self.procedures = procedures
        self._result = result
        self.chef = chef

    @property
    def is_cooked(self):
        if self._result is not None:
            return True
        else:
            return False

    @property
    def result(self):
        if self._result is not None:
            return self._result
        else:
            self.cook()
            return self._result

    def cook(self):
        def get_proc_func(proc):
            func = getattr(procedure, proc['procedure'])
            return func

        def run_procedure(proc):
            func = get_proc_func(proc)
            return func(self.chef, **proc['options'])

        for proc in self.procedures:
            self._result = run_procedure(proc)

        return self._result


class Chef:
    def __init__(self, info=None, cfg=None, ingredients=None, cooking=None, serving=None):
        assert 'main' in cooking.keys(), 'the cooking pipeline "main" must exists'

        self.info = info
        self.config = cfg
        self._ingredients = ingredients
        self.cooking = cooking
        self.serving = serving
        self._ingredients_bag = None

    @classmethod
    def from_recipe(cls, recipe):
        info = recipe['info']
        cfg = recipe['config']
        ingredients = recipe['ingredients']
        cooking = recipe['cooking']
        serving = recipe['serving']

        return cls(info, cfg, ingredients, cooking, serving)

    @property
    def ingredients_bag(self):
        if self._ingredients_bag is not None:
            return self._ingredients_bag
        bag = dict()
        for ing in self._ingredients:
            bag[ing['id']] = Dataset.from_ddfcsv(ing['path'])
        for name, ppl in self.cooking.items():
            bag[name] = Pipeline(name, ppl, self)
        self._ingredients_bag = bag
        return bag

    def get_ingredient(self, name):
        ing = self.ingredients_bag[name]
        if isinstance(ing, Pipeline):
            return ing.result
        else:
            return ing

    def cook(self):
        result = self.get_ingredient('main').compute()
        result.serve(self.serving)


def _loadfile(f):
    """load json/yaml file, into AttrDict"""
    if re.match('.*\.json', f):
        res = json.load(open(f), object_pairs_hook=AttrDict)
    else:
        res = yaml.load(open(f), Loader=AttrDictYAMLLoader)

    return res


