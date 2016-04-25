# ddf_utils

## Installation

```$ pip3 install git+https://github.com/semio/ddf_utils.git```

## Usage

### Recipe

```
In [6]: import ddf_utils.recipe as ddfrecipe

# set the path for searching ddf repos and translation dictionaries.
In [14]: ddfrecipe.SEARCH_PATH = '/Users/semio/src/work/Gapminder/'

In [17]: ddfrecipe.DICT_PATH = '/Users/semio/src/work/Gapminder/ddf--gapminder--systema_globalis/etl/translation_dictionaries'

In [21]: recipe = '/Users/semio/src/work/Gapminder/ddf--gapminder--systema_globalis/etl/recipe.yaml'

# make a temp dir and run the recipe, store the result in the tempdir
In [34]: import tempfile

In [41]: dir = tempfile.mkdtemp()

In [82]: dishes = ddfrecipe.run_recipe(recipe)
running concepts
running entities
running datapoints

In [105]: ddfrecipe.dish_to_csv(dishes, dir)

```
