#!/usr/bin/env python3

"""
command line utility for creating ddf from recipe

usage:

before running the script, environment variables should be set

$ export DDF_PATH='path_to_search_ddf'
$ export DICT_PATH='path_to_search_translation_dict'

Then you can run the script with:

$ runrecipe.py -i path_to_recipe -o outdir

"""

import ddf_utils.chef as ddfrecipe
from ddf_utils.index import create_index_file
import sys
import getopt

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s -%(levelname)s %(message)s',
                    datefmt="%H:%M:%S"
                    )

if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], 'i:o:u', ['recipe=', 'outdir=', 'update'])

    update = False
    for o, v in opts:
        if o == '-i':
            recipe_file = v
        if o == '-o':
            outdir = v
        if o == '-u':
            update = True

    print('running recipe...')
    recipe = ddfrecipe.build_recipe(recipe_file)
    if update:
        pass
    res = ddfrecipe.run_recipe(recipe)
    print('saving result to disk...')
    ddfrecipe.dish_to_csv(res, outdir)
    print('creating index file...')
    create_index_file(outdir)
