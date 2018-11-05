# -*- coding: utf-8 -*-

"""In this test collection, we include tests for functions that
need input from user.
"""

import ddf_utils.chef as chef

import click
from click.testing import CliRunner
import pandas as pd


# def test_prompt_select():

#     @click.command()
#     def test():
#         options = ['foo', 'baz']
#         res = chef.helpers.prompt_select(options, 'testing prompt')
#         click.echo(str(res))

#     runner = CliRunner()
#     result = runner.invoke(test, input='1')
#     assert result.output.split('\n')[-2] == 'foo'
#     result = runner.invoke(test, input='2')
#     assert result.output.split('\n')[-2] == 'baz'
#     result = runner.invoke(test, input='n')
#     assert result.output.split('\n')[-2] == '-1'
#     result = runner.invoke(test, input='q')
#     assert result.exit_code == 130


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


def test_ddf_cli():
    import os
    import tempfile
    from ddf_utils.cli import ddf

    base_path = os.path.dirname(__file__)

    runner = CliRunner()

    result = runner.invoke(ddf)
    click.echo(result.output)

    result = runner.invoke(ddf, args=['build_recipe',
                                      os.path.join(base_path, 'chef/recipes/test_flatten.yml')])
    click.echo(result.output)
    assert result.exit_code == 0

    tmpdir = tempfile.mkdtemp()
    result = runner.invoke(ddf, args=['run_recipe',
                                      '--recipe',
                                      os.path.join(base_path, 'chef/recipes/test_cli.yaml'),
                                      '--outdir', tmpdir,
                                      '--ddf_dir',
                                      os.path.join(base_path, 'chef/datasets/')])
    click.echo(result.output)
    assert result.exit_code == 0
