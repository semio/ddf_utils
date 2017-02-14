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


def test_translate_column():

    @click.command()
    def test():
        from ddf_utils.transformer import _translate_column_df as tc
        df = pd.DataFrame([['congo', 'Congo']], columns=['country', 'name'])
        base_df = pd.DataFrame([['cod', 'Congo', 'Democratic Republic of the Congo'],
                                ['cog', 'Congo', 'Republic of the Congo']],
                               columns=['geo', 'abbr1', 'abbr2'])
        di = {'key': ['abbr1', 'abbr2'], 'value': 'geo'}

        res = tc(df, 'name', 'geo', di, base_df, 'drop', 'prompt')
        # click.echo(res)
        click.echo(res['geo'].values[0])

    runner = CliRunner()
    result = runner.invoke(test, input='1')
    assert not result.exception
    assert result.output.split('\n')[-2] == 'cod'
    result = runner.invoke(test, input='2')
    assert not result.exception
    assert result.output.split('\n')[-2] == 'cog'
