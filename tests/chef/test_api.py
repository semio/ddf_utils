"""testing chef api"""


import os

import tempfile

from ddf_utils.chef.api import Chef, ingredient_from_dict, run_recipe
from ddf_utils.chef.exceptions import ChefRuntimeError, IngredientError

wd = os.path.dirname(__file__)


def test_chef_api_call():
    from ddf_utils.chef.model.dag import DAG
    from ddf_utils.chef.model.ingredient import DataPointIngredient
    # create empty chef
    dag = DAG()
    Chef(dag=dag, metadata={}, config={}, cooking={}, serving=[])

    # create chef and add config
    chef = Chef()

    (chef.add_config(ddf_dir=os.path.join(wd, 'datasets'))
         .add_metadata(id='test_dataset',
                       base=['ddf--bp--energy'])
         .add_ingredient(id='bp-datapoints', dataset='ddf--bp--energy', key='geo, year', value='*')
         .add_procedure(collection='datapoints',
                        procedure='translate_header',
                        ingredients=['bp-datapoints'],
                        result='bp-datapoints-translate',
                        options={'dictionary': {'geo': 'country'}}))

    def multiply_1000(chef, ingredients, result, **options):
        # ingredients = [chef.dag.get_node(x) for x in ingredients]
        ingredient = ingredients[0]

        new_data = dict()
        for k, df in ingredient.get_data().items():
            df_ = df.copy()
            df_[k] = df_[k] * 1000
            new_data[k] = df_

        return DataPointIngredient.from_procedure_result(result, ingredient.key, new_data)

    chef.register_procedure(multiply_1000)
    chef.add_procedure(collection='datapoints',
                       procedure='multiply_1000',
                       ingredients=['bp-datapoints-translate'],
                       result='res')

    chef.serving
    chef.add_dish(['bp-datapoints-translate'], options={})
    chef.to_graph()
    chef.to_graph(node='res')
    chef.to_recipe()
    chef.dag.tree_view()
    chef.validate()
    res = chef.run()

    assert 1


def test_run_recipe():
    tmpdir = tempfile.mkdtemp()
    run_recipe(os.path.join(wd, 'recipes/test_cli.yaml'), os.path.join(wd, 'datasets/'), tmpdir)


def test_chef_load_recipe():
    recipe_file = os.path.join(wd, 'recipes/test_flatten.yml')
    chef = Chef.from_recipe(recipe_file)
    try:
        chef.validate()
    except ChefRuntimeError:
        pass
    assert 1


def test_ingredients():
    chef = Chef()
    chef = chef.add_config(ddf_dir=os.path.join(wd, 'datasets'))

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$in': ['*lower']
        }
    }, **chef.config)
    assert set(list(i.get_data().keys())) == set(['imr_lower'])

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'concept',
        'value': {
            '$in': ['concept', 'name', 'concept_type']
        }
    }, **chef.config)
    assert set(i.get_data()['concept'].columns) == set(['concept', 'name', 'concept_type'])

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'concept',
        'value': ['concept', 'name', 'concept_type']
    }, **chef.config)
    assert set(i.get_data()['concept'].columns) == set(['concept', 'name', 'concept_type'])

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country',
        'value': {
            '$nin': ['iso_code']
        }
    }, **chef.config)
    assert 'iso_code' not in i.get_data()['country'].columns

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country',
        'value': ['country', 'iso_code']
    }, **chef.config)
    assert 'iso_code' in i.get_data()['country'].columns

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$nin': ['*lower']
        }
    }, **chef.config)
    assert set(list(i.get_data().keys())) == set(['imr_upper', 'imr_median'])

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$nin': ['imr_lower', 'imr_upper']
        }
    }, **chef.config)
    assert set(list(i.get_data().keys())) == set(['imr_median'])

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$in': ['imr_lower', 'imr_upper']
        }
    }, **chef.config)
    assert set(list(i.get_data().keys())) == set(['imr_lower', 'imr_upper'])

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$in': ['imr_lower', 'lsdf']
        }
    }, **chef.config)
    assert set(list(i.get_data().keys())) == set(['imr_lower'])

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$nin': ['imr_*']
        }
    }, **chef.config)
    try:
        i.get_data()
    except IngredientError:
        pass

    i = ingredient_from_dict(dictionary={
        'id': 'ddf--dummy',
        'dataset': 'ddf--gapminder--dummy_companies',
        'key': 'synonym, region'
    }, **chef.config)
    assert set(list(i.get_data().keys())) == set(['region'])

    i = ingredient_from_dict(dictionary={
        'id': 'geo_entity_domain',
        'dataset': 'ddf--gapminder--geo_entity_domain',
        'key': 'country',
        'value': {
            '$in': ['country', 'name']
        }
    }, **chef.config)
