# -*- coding: utf-8 -*-

# from ddf_utils.chef import *
import ddf_utils.chef as chef

import os
import tempfile
import shutil
import logging
import common
import pytest
import glob

all_test_recipes = glob.glob('recipes/test_*')

@pytest.fixture(scope='session',
                params=all_test_recipes)
def recipe_file(request):
    return request.param


def test_run_recipe(recipe_file, to_disk=False):
    print('running test: ' + recipe_file)
    recipe = chef.build_recipe(recipe_file)
    if to_disk:
        outdir = tempfile.mkdtemp()
        print('tmpdir: ' + outdir)
        chef.run_recipe(recipe, True, outdir)
    else:
        _ = chef.run_recipe(recipe)
    assert 1
