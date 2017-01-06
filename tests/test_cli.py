# -*- coding: utf-8 -*-

"""In this test collection, we include tests for functions that
need input from user.
"""

import ddf_utils.chef as chef

import common
import click
from click.testing import CliRunner
import pytest
import pandas as pd


def test_prompt_select():

    @click.command()
    def test():
        options = ['foo', 'baz']
        res = chef.helpers.prompt_select(options, 'testing prompt')
        click.echo(str(res))

    runner = CliRunner()
    result = runner.invoke(test, input='1')
    assert result.output.split('\n')[-2] == 'foo'
    result = runner.invoke(test, input='2')
    assert result.output.split('\n')[-2] == 'baz'
    result = runner.invoke(test, input='n')
    assert result.output.split('\n')[-2] == '-1'
    result = runner.invoke(test, input='q')
    assert result.exit_code == 130


def test_generate_mapping_dict():

    @click.command()
    def test():
        from ddf_utils.transformer import _generate_mapping_dict2 as f
        df = pd.DataFrame([['congo', 'Congo']], columns=['country', 'name'])
        base_df = pd.DataFrame([['cod', 'Congo'], ['cog', 'Congo']],
                               columns=['geo', 'abbr'])
        di = {'key': ['abbr'], 'value': 'geo'}

        res = f(df, 'name', di, base_df, 'drop')
        # click.echo(res)
        click.echo(res['Congo'])

    runner = CliRunner()
    result = runner.invoke(test, input='1')
    assert not result.exception
    assert result.output.split('\n')[-2] == 'cod'
    result = runner.invoke(test, input='2')
    assert not result.exception
    assert result.output.split('\n')[-2] == 'cog'
