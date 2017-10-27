"""testing chef api"""


import os
import ruamel.yaml as yaml
from ddf_utils.chef.api import Chef
from ddf_utils.chef.ingredient import Ingredient
from ddf_utils.chef.exceptions import IngredientError

wd = os.path.dirname(__file__)


def test_chef_api_call():
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

    from ddf_utils.chef.ingredient import ProcedureResult

    def multiply_1000(chef, ingredients, result, **options):
        ingredients = [chef.dag.get_node(x) for x in ingredients]
        ingredient = ingredients[0].evaluate()

        new_data = dict()
        for k, df in ingredient.get_data().items():
            df_ = df.copy()
            df_[k] = df_[k] * 1000
            new_data[k] = df_

        return ProcedureResult(chef, result, ingredient.key, new_data)

    chef.register_procedure(multiply_1000)
    chef.add_procedure(collection='datapoints',
                       procedure='multiply_1000',
                       ingredients=['bp-datapoints-translate'],
                       result='res')

    g = chef.to_graph()
    r = chef.to_recipe()
    chef.validate()
    res = chef.run()

    assert 1


def test_chef_load_recipe():
    recipe_file = os.path.join(wd, 'recipes/test_flatten.yml')
    chef = Chef.from_recipe(recipe_file, ddf_dir=os.path.join(wd, 'datasets'))
    res = chef.run()
    assert 1


def test_ingredients():
    chef = Chef()
    chef = chef.add_config(ddf_dir=os.path.join(wd, 'datasets'))

    i = Ingredient.from_dict(chef, {
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$in': ['*lower']
        }
    })
    assert set(list(i.get_data().keys())) == set(['imr_lower'])

    i = Ingredient.from_dict(chef, {
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$nin': ['*lower']
        }
    })
    assert set(list(i.get_data().keys())) == set(['imr_upper', 'imr_median'])

    i = Ingredient.from_dict(chef, {
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$nin': ['imr_lower', 'imr_upper']
        }
    })
    assert set(list(i.get_data().keys())) == set(['imr_median'])

    i = Ingredient.from_dict(chef, {
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$in': ['imr_lower', 'imr_upper']
        }
    })
    assert set(list(i.get_data().keys())) == set(['imr_lower', 'imr_upper'])

    i = Ingredient.from_dict(chef, {
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$in': ['imr_lower', 'lsdf']
        }
    })
    assert set(list(i.get_data().keys())) == set(['imr_lower'])

    i = Ingredient.from_dict(chef, {
        'id': 'ddf--cme',
        'dataset': 'ddf--cme',
        'key': 'country, year',
        'value': {
            '$nin': ['imr_*']
        }
    })
    try:
        i.get_data()
    except IngredientError:
        pass
