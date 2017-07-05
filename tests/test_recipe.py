# -*- coding: utf-8 -*-

import os
import sys
import tempfile
import shutil
import logging
import common
import pytest
import glob

# from ddf_utils.chef import *
from ddf_utils.chef.api import Chef
from ddf_utils.chef.exceptions import ChefRuntimeError, ProcedureError


wd = os.path.dirname(__file__)
test_recipes_pass = glob.glob(os.path.join(wd, 'recipes_pass/test_*'))
test_recipes_fail = glob.glob(os.path.join(wd, 'recipes_fail/test_*'))


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
    chef = Chef.from_recipe(recipe_file_pass,
                            ddf_dir=os.path.join(wd, 'datasets'),
                            procedure_dir=os.path.join(wd, 'recipes_pass/procedures'))
    if to_disk:
        outdir = tempfile.mkdtemp()
        print('tmpdir: ' + outdir)
    else:
        outdir = None

    chef.run(to_disk, outdir)


def test_run_recipe_fail(recipe_file_fail, to_disk=False):
    print('running test: ' + recipe_file_fail)
    chef = Chef.from_recipe(recipe_file_fail,
                            ddf_dir=os.path.join(wd, 'datasets'))
    if to_disk:
        outdir = tempfile.mkdtemp()
        print('tmpdir: ' + outdir)
    else:
        outdir = None
    try:
        chef.run(to_disk, outdir)
    except (ChefRuntimeError, ProcedureError):
        return
    else:
        raise Exception('test should fail')
