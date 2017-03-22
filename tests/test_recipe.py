# -*- coding: utf-8 -*-

import os
import tempfile
import shutil
import logging
import common
import pytest
import glob

# from ddf_utils.chef import *
import ddf_utils.chef as chef
from ddf_utils.chef.exceptions import ChefRuntimeError


test_recipes_pass = glob.glob('recipes_pass/test_*')
test_recipes_fail = glob.glob('recipes_fail/test_*')


@pytest.fixture(scope='session',
                params=test_recipes_pass)
def recipe_file_pass(request):
    return request.param


@pytest.fixture(scope='session',
                params=test_recipes_fail)
def recipe_file_fail(request):
    return request.param


def test_run_recipe_pass(recipe_file_pass, to_disk=False):
    print('running test: ' + recipe_file_pass)
    recipe = chef.build_recipe(recipe_file_pass)
    if to_disk:
        outdir = tempfile.mkdtemp()
        print('tmpdir: ' + outdir)
    else:
        outdir = None

    chef.run_recipe(recipe, to_disk, outdir)


def test_run_recipe_fail(recipe_file_fail, to_disk=False):
    print('running test: ' + recipe_file_fail)
    recipe = chef.build_recipe(recipe_file_fail)
    if to_disk:
        outdir = tempfile.mkdtemp()
        print('tmpdir: ' + outdir)
    else:
        outdir = None
    try:
        chef.run_recipe(recipe, to_disk, outdir)
    except ChefRuntimeError:
        return
    else:
        raise Exception('test should fail')
