# -*- coding: utf-8 -*-

# from ddf_utils.chef import *
import ddf_utils.chef as chef

import os
import shutil
# import pandas as pd
# import json
# import yaml
# import numpy as np


import logging
logging.basicConfig(level=logging.DEBUG)

# chef.SEARCH_PATH = '/Users/semio/src/work/Gapminder/'
# chef.DICT_PATH = '/Users/semio/src/work/Gapminder/ddf--gapminder--systema_globalis/etl/translation_dictionaries'

recipe_file = 'recipes/test.yaml'
outdir = 'tmp/'

if not os.path.exists(outdir):
    os.mkdir(outdir)
else:
    for f in os.listdir(outdir):
        path = os.path.join(outdir, f)
        if os.path.isdir(path):
            pass
        else:
            os.remove(path)


recipe =  chef.build_recipe(recipe_file, to_disk=True)
res = chef.run_recipe(recipe)
chef.dish_to_csv(res, outdir)
