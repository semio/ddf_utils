# -*- coding: utf-8 -*-

# from ddf_utils.chef import *
import ddf_utils.chef as chef

import os
import tempfile
import shutil
import logging
import pytest

@pytest.fixture(scope='session')
def recipe_file():
    fn = 'recipes/test.yaml'
    return fn


def test_build_recipe(recipe_file):
    recipe = chef.build_recipe(recipe_file)
    # res = chef.run_recipe(recipe)
    # chef.dish_to_csv(res, outdir)
    assert 1


def test_run_recipe(recipe_file):
    outdir = tempfile.mkdtemp()
    print('tmpdir: ' + outdir)
    recipe = chef.build_recipe(recipe_file)
    res = chef.run_recipe(recipe)
    assert 1
