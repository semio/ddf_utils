# -*- coding: utf-8 -*-

import os
import numpy as np
import shutil
import tempfile
from ddf_utils.chef.api import Chef
from ddf_utils.chef.ingredient import Ingredient
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
    res_ent = list(filter(lambda x: True if x.dtype == 'entities' else False, res))[0]
    res_dps = list(filter(lambda x: True if x.dtype == 'datapoints' else False, res))[0]

    country = res_ent.get_data()['country']
    dps = res_dps.get_data()

    assert set(dps.keys()) == {'imr_upper', 'imr_lower'}
    for dp in dps.values():
        assert np.all(dp.year > 2000)
        assert set(dp.country.unique()) == {'usa', 'swe'}

    assert set(country.columns) == {'country', 'countryname'}
    assert set(country.country.values) == {'usa', 'swe'}


def test_flatten():
    chef = chef_fn('test_flatten.yml')
    res = chef.run()

    for r in res:
        print(r.get_data().keys())

    assert set(res[0].get_data().keys()) == {
        'agriculture_thousands_f', 'agriculture_thousands_m',
        'agriculture_thousands_mf', 'agriculture_percentage_f',
        'agriculture_percentage_m', 'agriculture_percentage_mf'}


def test_groupby():
    chef = chef_fn('test_groupby.yaml')
    chef.run()

    dp1 = chef.dag.get_node('grouped-datapoints-1').evaluate().get_data()
    dp2 = chef.dag.get_node('grouped-datapoints-2').evaluate().get_data()

    assert len(dp1.keys()) == 1
    assert len(dp2.keys()) == 1
    assert set(dp1['agriculture_percentage'].columns) == set(['country', 'year',
                                                              'agriculture_percentage'])
    assert set(dp2['agriculture_thousands'].columns) == set(['country', 'year',
                                                             'agriculture_thousands'])


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


def test_translate_header():
    chef = chef_fn('test_translate_header.yaml')
    res = chef.run()

    indicators = ['infant_mortality_upper', 'infant_mortality_median', 'infant_mortality_lower']
    data = res[0].get_data()

    assert set(data.keys()) == set(indicators)
    for i in indicators:
        assert set(data[i].columns) == set(['geo', 'time', i])


def test_trend_bridge():
    chef = chef_fn('test_trend_bridge.yml')
    chef.run()


def test_window():
    chef = chef_fn('test_window.yaml')
    chef.run()
    # TODO: check result?


def test_serving():
    chef1 = chef_fn('test_serve_procedure.yaml')
    res = chef1.run()
    assert len(res) == 2

    chef2 = chef_fn('test_serving_section.yaml')
    tmpdir = tempfile.mkdtemp()
    res = chef2.run(serve=True, outpath=tmpdir)
    assert len(res) == 2
    assert os.path.exists(os.path.join(tmpdir, 'test_serving'))


def test_merge():
    chef = chef_fn('test_merge.yaml')
    res = chef.run()

    data = res[0].get_data()
    indicators = ['imr_lower', 'imr_median', 'imr_upper',
                  'biofuels_production_kboed', 'biofuels_production_ktoe']
    assert set(data.keys()) == set(indicators)


def test_run_op():
    chef = chef_fn('test_run_op.yaml')
    chef.run()


def test_import_procedure_fail():
    chef = chef_fn('test_import_procedure.yml')
    chef.add_procedure('datapoints', 'nonexists', ['result'], result='error-ing')
    try:
        chef.run()
    except ChefRuntimeError:
        pass
    except:
        raise


def test_deprecated():
    chef1 = chef_fn('test_filter_row.yml')
    chef1.run()

    chef2 = chef_fn('test_filter_item.yaml')
    chef2.run()