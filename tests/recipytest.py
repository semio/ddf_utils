# -*- coding: utf-8 -*-

from ddf_utils.recipe import *
import ddf_utils.recipe as ddf

import os
import pandas as pd
import json
import yaml
import numpy as np

ddf.SEARCH_PATH = '/Users/semio/src/work/Gapminder/'
ddf.DICT_PATH = '/Users/semio/src/work/Gapminder/ddf--gapminder--systema_globalis/etl/translation_dictionaries'

# recipe = yaml.load(open('/Users/semio/src/work/Gapminder/ddf--gapminder--systema_globalis/etl/recipe.yaml'))
recipe = '/Users/semio/src/work/Gapminder/ddf--gapminder--systema_globalis/etl/recipe.yaml'

# ings = [Ingredient.from_dict(i) for i in recipe['ingredients']]
# ings_dict = dict([[i.ingred_id, i] for i in ings])

# print(ings_dict['gw-entities'].filter_index_file_name())

dishes = run_recipe(recipe)

dish_to_csv(dishes, os.path.join('/Users/semio/src/work/Gapminder/', 'ddf--gapminder--systema_globalis/etl/output'))

