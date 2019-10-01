# -*- coding: utf-8 -*-

"""In this test collection, we include tests for functions that
need input from user.
"""

import os
import tempfile
from ddf_utils.cli import ddf

import click
from click.testing import CliRunner
import pandas as pd

base_path = os.path.dirname(__file__)


def test_translate_column():

    @click.command()
    def test():
        from ddf_utils.transformer import _translate_column_df as tc
        df = pd.DataFrame([['congo', 'Congo']], columns=['country', 'name'])
        base_df = pd.DataFrame([['cod', 'Congo', 'Democratic Republic of the Congo'],
                                ['cog', 'Congo', 'Republic of the Congo']],
                               columns=['geo', 'abbr1', 'abbr2'])
        di = {'key': ['abbr1', 'abbr2'], 'value': 'geo'}

        res = tc(df, 'name', 'geo', di, base_df, 'drop', 'prompt', False)
        # click.echo(res)
        click.echo(res['geo'].values[0])

    runner = CliRunner()
    result = runner.invoke(test, input='1')
    assert not result.exception
    assert result.output.split('\n')[-2] == 'cod'
    result = runner.invoke(test, input='2')
    assert not result.exception
    assert result.output.split('\n')[-2] == 'cog'


def test_ddf_cli_1():
    runner = CliRunner()
    # base command
    result = runner.invoke(ddf)
    click.echo(result.output)


def test_ddf_cli_2():
    runner = CliRunner()
    # build recipe
    result = runner.invoke(ddf, args=['build_recipe',
                                      os.path.join(base_path, 'chef/recipes/test_flatten.yml')])
    click.echo(result.output)
    assert result.exit_code == 0

    # run recipe
    tmpdir = tempfile.mkdtemp()
    result = runner.invoke(ddf, args=['run_recipe',
                                      '--recipe',
                                      os.path.join(base_path, 'chef/recipes/test_cli.yaml'),
                                      '--outdir', tmpdir,
                                      '--ddf_dir',
                                      os.path.join(base_path, 'chef/datasets/')])
    click.echo(result.output)
    assert result.exit_code == 0

    # create_datapackage
    result = runner.invoke(ddf, args=['create_datapackage', '--update', tmpdir])
    assert result.exit_code == 0

    result = runner.invoke(ddf, args=['create_datapackage', tmpdir])
    assert result.exit_code == 0

    result = runner.invoke(ddf, args=['create_datapackage', '-p', tmpdir])
    assert result.exit_code == 0

    # etl_type
    result = runner.invoke(ddf, args=['etl_type', '-d',
                                      os.path.join(base_path, 'chef/datasets/ddf--gapminder--co2_emission/', 'etl/scripts')])
    assert result.exit_code == 0

    # cleanup
    result = runner.invoke(ddf, args=['cleanup', 'ddf', tmpdir])
    assert result.exit_code == 0


def test_ddf_cli_3():
    runner = CliRunner()
    # diff
    result = runner.invoke(ddf, args=['diff',
                                      '-k', 'country',
                                      '-i', 'rmse',
                                      '-i', 'nrmse',
                                      '-i', 'rval',
                                      '-i', 'avg_pct_chg',
                                      '-i', 'max_pct_chg',
                                      '-i', 'new_datapoints',
                                      '-i', 'dropped_datapoints',
                                      os.path.join(base_path, 'chef/datasets/ddf--gapminder--co2_emission'),
                                      os.path.join(base_path, 'chef/datasets/ddf--gapminder--co2_emission_2')])

    assert result.exit_code == 0
