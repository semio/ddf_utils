# -*- coding: utf-8 -*-

import os

from ddf_utils.chef.api import Chef

recipe_file = '../recipes/etl.yml'

if __name__ == '__main__':

    try:
        d = os.environ['DATASETS_DIR']
        chef = Chef.from_recipe(recipe_file, ddf_dir=d)
    except KeyError:
        chef = Chef.from_recipe(recipe_file)

    chef.run(serve=True, outpath='../../')
