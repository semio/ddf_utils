# -*- coding: utf-8 -*-

# from ddf_utils.chef import *
import ddf_utils.chef as chef

import os
import shutil
# import pandas as pd
# import json
# import yaml
# import numpy as np


chef.SEARCH_PATH = '/Users/semio/src/work/Gapminder/'
chef.DICT_PATH = '/Users/semio/src/work/Gapminder/ddf--gapminder--systema_globalis/etl/translation_dictionaries'

recipe = 'test.yaml'
outdir = '../tmp'

if not os.path.exists(outdir):
    os.mkdir(outdir)
else:
    for f in os.listdir(outdir):
        path = os.path.join(outdir, f)
        shutil.rmtree(path)

res = chef.run_recipe(recipe)
chef.dish_to_csv(res, outdir)
