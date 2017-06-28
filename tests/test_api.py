"""testing chef api"""


import os
import ruamel.yaml as yaml
from ddf_utils.chef.api import Chef

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

    def multiply_1000(dag, ingredients, result, **options):
        ingredients = [dag.get_node(x) for x in ingredients]
        ingredient = ingredients[0].evaluate()

        new_data = dict()
        for k, df in ingredient.get_data().items():
            df_ = df.copy()
            df_[k] = df_[k] * 1000
            new_data[k] = df_

        return ProcedureResult(result, ingredient.key, new_data)

    chef.register_procedure(multiply_1000)
    chef.add_procedure(collection='datapoints',
                       procedure='multiply_1000',
                       ingredients=['bp-datapoints-translate'],
                       result='res')

    g = chef.to_graph()
    r = chef.to_recipe()
    res = chef.run()

    assert 1


def test_chef_load_recipe():
    recipe_file = os.path.join(wd, 'recipes_pass/test_flatten.yml')
    chef = Chef.from_recipe(recipe_file)
    res = chef.run()
    assert 1
