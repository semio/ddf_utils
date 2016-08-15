# -*- coding: utf-8 -*-

import ddf_utils.chef as chef
from ddf_utils.chef.ingredient import Ingredient

import yaml
import time


chef.config.SEARCH_PATH = '/Users/semio/src/work/Gapminder/'

recipe = yaml.load(open('./recipes/recipe_cme.yaml'))

ing = Ingredient.from_dict(recipe['ingredients'][0])

l = ing.get_last_update()

t = time.ctime(l.last_update.values[0])

print(t)
