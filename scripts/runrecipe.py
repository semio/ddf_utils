#!/usr/bin/env python3

"""
command line utility for creating ddf from recipe

usage:

before running the script, enviornment variables should be set

$ export DDF_PATH='path_to_search_ddf'
$ export DICT_PATH='path_to_search_translation_dict'

Then you can run the script with:

$ runrecipe.py -i path_to_recipe -o outdir

"""

import ddf_utils.recipe as ddfrecipe
from ddf_utils.index import create_index_file
import os
import sys
import getopt


if __name__ == '__main__':
    ddf_path = os.environ.get('DDF_PATH')
    dict_path = os.environ.get('DICT_PATH')

    opts, args = getopt.getopt(sys.argv[1:], 'i:o:', ['recipe=', 'outdir='])

    ddfrecipe.SEARCH_PATH = ddf_path
    ddfrecipe.DICT_PATH = dict_path

    for o, v in opts:
        if o == '-i':
            recipe = v
        if o == '-o':
            outdir = v

    print('running recipe...')
    res = ddfrecipe.run_recipe(recipe)
    print('saving result to disk...')
    ddfrecipe.dish_to_csv(res, outdir)
    print('creating index file...')
    create_index_file(outdir)
