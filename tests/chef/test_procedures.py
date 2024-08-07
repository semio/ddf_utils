# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
import shutil
import tempfile
import pytest
from ddf_utils.chef.api import Chef
from ddf_utils.chef.exceptions import IngredientError, ProcedureError, ChefRuntimeError


wd = os.path.dirname(__file__)


def chef_fn(fn):
    return Chef.from_recipe(os.path.join(wd, 'recipes', fn),
                            ddf_dir=os.path.join(wd, 'datasets'),
                            procedure_dir=os.path.join(wd, 'procedures'))


def test_debug_option():
    chef = chef_fn('test_debug_option.yaml')
    chef.run()
    assert os.path.exists('./_debug/dps_key_translated')
    assert os.path.exists('./_debug/res')

    # cleanup
    shutil.rmtree('./_debug/')


def test_include():
    chef = chef_fn('test_include_main.yml')
    chef.run()


def test_include_fail():
    with pytest.raises(ChefRuntimeError):
        chef = chef_fn('test_include_fail_main.yaml')
        chef.run()


def test_extract_concepts():
    chef = chef_fn('test_extract_concepts.yaml')
    res = chef.run()
    res = res[0].get_data()['concept']

    assert 'geo' in res.concept.values
    assert 'year' in res.concept.values
    assert res.set_index('concept').loc['year', 'concept_type'] == 'time'


def test_filter():
    chef = chef_fn('test_filter.yaml')
    res = chef.run()
    res_conc = list(filter(lambda x: True if x.dtype == 'concepts' else False, res))[0]
    res_ent = list(filter(lambda x: True if x.dtype == 'entities' else False, res))[0]
    res_dps = list(filter(lambda x: True if x.dtype == 'datapoints' else False, res))[0]

    assert 'imr_lower' in res_conc.get_data()['concept']['concept'].values

    country = res_ent.get_data()['country']
    dps = res_dps.compute()

    assert set(dps.keys()) == {'imr_upper', 'imr_lower'}
    for dp in dps.values():
        # assert dp.year.dtype == np.int64
        assert np.all(dp.year > "2000")
        assert set(dp.country.unique()) == {'usa', 'swe'}

    assert set(country.columns) == {'country', 'countryname'}
    assert set(country.country.values) == {'usa', 'swe'}


def test_flatten():
    chef = chef_fn('test_flatten.yml')
    res = chef.run()

    for r in res:
        print(r.compute().keys())

    assert set(res[0].compute().keys()) == {
        'agriculture_thousands_f', 'agriculture_thousands_m',
        'agriculture_percentage_f', 'agriculture_percentage_m',
        'agriculture_thousands', 'agriculture_percentage'}

    # assert res[0].compute()['agriculture_percentage_m'].dtypes['year'] == np.int64


def test_groupby():
    chef = chef_fn('test_groupby.yaml')
    chef.run()

    dp1 = chef.dag.get_node('grouped-datapoints-1').evaluate().compute()
    dp2 = chef.dag.get_node('grouped-datapoints-2').evaluate().compute()

    assert len(dp1.keys()) == 1
    assert len(dp2.keys()) == 1
    assert set(dp1['agriculture_percentage'].columns) == {'country', 'year', 'agriculture_percentage'}
    assert set(dp2['agriculture_thousands'].columns) == {'country', 'year', 'agriculture_thousands'}
    # assert dp1['agriculture_percentage'].dtypes['year'] == np.int16
    # assert dp2['agriculture_thousands'].dtypes['year'] == np.int16


def test_custom_procedure():
    chef = chef_fn('test_import_procedure.yml')
    chef.run()


def test_ingredients():
    for i in range(1, 4):
        chef = chef_fn('test_ingredients_{}.yaml'.format(i))
        chef.run()


def test_translate_column():
    chef = chef_fn('test_translate_column.yaml')
    chef.run()

    res = chef.dag.get_node('bp-datapoints-aligned').evaluate().compute()


def test_translate_header():
    chef = chef_fn('test_translate_header.yaml')
    res = chef.run()

    indicators = ['infant_mortality_median', 'imr_lower']
    data = res[0].compute()

    assert set(data.keys()) == set(indicators)
    for i in indicators:
        assert set(data[i].columns) == set(['geo', 'time', i])
        # assert data[i].dtypes['time'] == np.int64

    data = res[1].get_data()
    assert 'city' in data.keys()
    assert 'city' in data['city'].columns
    assert 'is--city' in data['city'].columns


def test_translate_header_fail():
    chef = chef_fn('test_translate_header_fail_1.yaml')
    with pytest.raises(ValueError):
        chef.run()

    chef = chef_fn('test_translate_header_fail_2.yaml')
    with pytest.raises(ValueError):
        chef.run()

    chef = chef_fn('test_translate_header_fail_3.yaml')
    with pytest.raises(ValueError):
        chef.run()


def test_trend_bridge():
    chef = chef_fn('test_trend_bridge.yml')
    chef.run()

    res = chef.dag.get_node('res-1').evaluate().compute()
    # assert res['imr_lower'].dtypes['year'] == np.int64


def test_window():
    chef = chef_fn('test_window.yaml')
    chef.run()

    dp1 = chef.dag.get_node('rolling_datapoints_1').evaluate().compute()
    dp2 = chef.dag.get_node('rolling_datapoints_2').evaluate().compute()
    dp3 = chef.dag.get_node('rolling_datapoints_3').evaluate().compute()
    dp4 = chef.dag.get_node('rolling_datapoints_4').evaluate().compute()

    assert dp1['value']['value'].tolist() == [1, 1, 1, 1, 1, 1, 1, 1.5, 2, 3, 4, 5]
    assert dp2['value']['value'].tolist() == [1, 2, 3, 4, 5, 6, 1, 3, 6, 10, 15, 21]
    assert dp3['value']['value'].tolist() == [1, 1, 1, 1, 1, 1, 1, 1.5, 2, 3, 4, 5]
    assert dp4['value']['value'].tolist() == [1, 2, 3, 4, 5, 6, 1, 3, 6, 10, 15, 21]

def test_serving():
    chef1 = chef_fn('test_serve_procedure.yaml')
    res = chef1.run()
    assert len(res) == 3

    chef2 = chef_fn('test_serving_section.yaml')
    tmpdir = tempfile.mkdtemp()
    res = chef2.run(serve=True, outpath=tmpdir)
    assert len(res) == 3
    assert os.path.exists(os.path.join(tmpdir, 'test_serving'))


def test_merge():
    chef = chef_fn('test_merge.yaml')
    res = chef.run()

    data = res[0].compute()
    indicators = ['imr_lower', 'imr_median', 'imr_upper',
                  'biofuels_production_kboed', 'biofuels_production_ktoe']
    assert set(data.keys()) == set(indicators)
    # assert data['imr_median'].dtypes['year'] == np.int64

    imr_lower = data['imr_lower'].set_index(['geo', 'year'])
    assert imr_lower.loc[('afg', "1961"), 'imr_lower'] == 2055


def test_merge_2():
    chef = chef_fn('test_merge_2.yaml')
    res = chef.run()

    data = res[0].get_data()
    data = data['concept'].set_index('concept')
    assert data.loc['col1', 'col1'] == 'testing1'
    assert data.loc['col2', 'col2'] == 'testing2'
    assert data.loc['col1', 'col2'] == 'bar'
    assert data.loc['col2', 'col1'] is np.nan

def test_merge_3():
    chef = chef_fn('test_merge_3.yaml')
    res = chef.run()

    data = res[0].compute()
    data = data['indicator'].set_index(['country', 'year'])
    assert data.loc[('chn', 2000), 'indicator'] == 1
    assert data.loc[('chn', 2001), 'indicator'] == 2
    assert data.loc[('chn', 2002), 'indicator'] == 3
    assert data.loc[('chn', 2003), 'indicator'] == 3
    assert data.loc[('chn', 2004), 'indicator'] == 3


def test_merge_fail():
    chef = chef_fn('test_merge_fail.yaml')
    with pytest.raises(ProcedureError):
        chef.run()


def test_dag_fail():
    chef = chef_fn('test_dag_fail.yaml')
    with pytest.raises(ChefRuntimeError):
        chef.run()


def test_run_op():
    chef = chef_fn('test_run_op.yaml')
    chef.run()

    res = chef.dag.get_node('res').evaluate().compute()
    # assert res['mean_imr'].dtypes['year'] == np.int16


def test_import_procedure_fail():
    chef = chef_fn('test_import_procedure.yml')
    chef.add_procedure('datapoints', 'nonexists', ['result'], result='error-ing')
    try:
        chef.run()
    except ChefRuntimeError:
        pass


def test_ops():
    from ddf_utils.chef.ops import gt, lt, between, aagr

    x = np.ones(1000)
    assert gt(x, 0)
    assert gt(x, 1, include_eq=True)
    assert not gt(x, 2)

    assert lt(x, 2)
    assert lt(x, 1, include_eq=True)
    assert not lt(x, 0)

    assert between(x, 0, 2)

    assert np.all(aagr(pd.DataFrame(x)) == 0)
