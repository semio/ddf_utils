# same recipe as example.hy, impletment in python.

from datetime import datetime

from ddf_utils.chef.api import Chef

chef = Chef()

chef.add_metadata(id='population-age-group-dataset',
                  last_update=str(datetime.today()))

chef.add_config(ddf_dir='pato/to/dataset/dir')

chef.add_ingredient(id='population-gy-age-datapoints',
                    dataset='ddf--unpon--wpp_population',
                    key='country_code, year, age',
                    value=['population'])

collection = 'datapoints'
# define groups and names
groups = [[str(x) for x in range(0, 5)],
          [str(x) for x in range(5, 10)],
          [str(x) for x in range(10, 20)]]
names = ['population_0_4', 'population_5_9', 'population_10_19']
ingredient_source = 'population-by-age-datapoints'


def make_age_group_dps(ingredient, age_group, indicator_name):
    filtered_result = 'filtered-' + indicator_name
    groupby_result = 'sum-' + filtered_result
    translated_result = 'translated-' + groupby_result

    chef.add_procedure(collection,
                       procedure='filter',
                       ingredients=[ingredient],
                       result=filtered_result,
                       options={"row":
                                {"age":
                                 {"$in": age_group}}})
    chef.add_procedure(collection,
                       procedure='groupby',
                       result=groupby_result,
                       ingredients=[filtered_result],
                       options={"groupby": ["country_code", "year"],
                                "aggregate":
                                {"population": "sum"}})
    chef.add_procedure(collection,
                       result=translated_result,
                       procedure='translate_header',
                       options={"dictionary":
                                {"country_code": "geo"}})
    return translated_result


results = []
for g, n in zip(groups, names):
    res = make_age_group_dps(ingredient_source, g, n)
    results.append(res)

chef.add_procedure(collection,
                   procedure='merge',
                   result='merged_result',
                   ingredients=results)

# TODO: add serving() for Chef
# chef.add_serving(ingredients=['merged-result], options={})

print(chef.to_recipe())
