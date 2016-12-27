# Cooking with recipes (draft)

## What is recipe

Recipe is an [Domain-specific language(DSL)][1] for DDF datasets, to manipulate existing
datasets and create new ones.
By reading and running the recipe executor(Chef), one can generate a new dataset based
on the `ingredients` and `procedures` described in the recipe.

[1]: https://en.wikipedia.org/wiki/Domain-specific_language

## structure of a recipe

A recipe is made of following parts:

- basic info
- configuration
- includes
- cooking procedures
- serving section

Each part can be optional or mandatory based on the use case, which we will show below. 
A recipe file can be in either json or yaml format. Check
[our recipes in production][2] (starting with recipe_main.yaml) for examples.

[2]: https://github.com/open-numbers/ddf--gapminder--systema_globalis/blob/develop/etl/recipes/

### info section

All basic info are stored in `info` section of the recipe. an `id` field is
required inside this section. Any other information about the new dataset can be
store inside this section, such as `name`, `provider`, `description` and so on.
This part is mainly for human and is optional for now, but 
**maybe later on we will add connections for this section and the datapackage file.**

**Note on yaml format**

In the example above we use `base` to indicate where the ingredients comes from. We set 
an _yaml anchor_ to each of them (`&d1` etc) so that we can reference them later 
in the recipe (`*d1` etc).

### config section

Inside `config` section, we define the configuration of dirs. currently we
can set below path:

- `ddf_dir`: the directory that contains all ddf csv repos. Must set this
variable in the main recipe to run with chef, or provide as an command line option
using the `ddf` utility.
- `recipes_dir`: the directory contains all recipes to include. Must set this 
variable if we have `include` section. If relative path is provided, the path will be related
to the path of the recipe.
- `dictionary_dir`: the directory contains all translation files. Must set this
variable if we have json file in the options of procedures. (translation
will be discussed later). If relative path is provided, the path will be related
to the path of the recipe.

### include section

A recipe can include other recipes inside itself. to include a recipe, simply
append the filename to the `include` section. note that it should be a absolute
path or a filename inside the `recipes_dir`.

### cooking section

`cooking` section is a dictionary contains one or more list of procedures to
build a dataset. valid keys for cooking section are _datapoints_, _entities_,
_concepts_.

The basic format of a procedure is:

```yaml
procedure: pro_name
ingredients:
  - ingredient_to_run_the_proc
options:  # options object to pass to the procedure
  foo: baz
result: id_of_new_ingredient
```

Available procedures will be shown in the below [section](#available-procedures).

### serving section and serve procedure

For now there are 2 ways to tell chef which ingredients should be served, and
you can choose one of them, but not both.

**serve procedure**

`serve` procedure should be placed in `cooking` section, with the following format:

```yaml
procedure: serve
ingredients:
  - ingredient_to_serve
options:
  opt: val
```

multiple serve procedures are allowed in each cooking section.

**serving section**

`serving` section should be a top level object in the recipe, with following format:

```yaml
serving:
  - id: ingredient_to_serve_1
    options:
      opt: val
  - id: ingredient_to_serve_2
    options:
      foo: baz
```

**available options**

`digits` : _int_, controls how many decimal should be kept at most in a numeric ingredient.

## Recipe execution

To run a recipe, you can use the `ddf run_recipe` command:

```shell
$ ddf run_recipe -i path_to_recipe.ml -o output_dir
```

You can specify the path where your datasets are stored:

```shell
$ ddf run_recipe -i path_to_recipe.yml -o output_dir --ddf_dir path_to_datasets
```

Internally, the process to generate a dataset have following steps:

- read the main recipe into Python object
- if there is include section, read each file in the include list and expand the 
main recipe
- if there is file name in dictionary option of each procedure, try to expand them 
if the option value is a filename
- check if all datasets are available
- build a procedure dependency tree, check if there are loops in it
- if there is no `serve` procedure and `serving` section, the last procedure result for each
section will be served. If there is `serve` procedure or `serving` section, chef will serve
the result as described
- run the procedures for each ingredient to be served and their dependencies
- save output to disk

If you want to embed the function into your script, you can write script like this:

```python
import ddf_utils.chef as chef

def run_recipe(recipe_file, outdir):
    recipe = chef.build_recipe(recipe_file)  # get all sub-recipes and dictionaries
    res = chef.run_recipe(recipe)  # run the recipe, get output for serving
    chef.dishes_to_disk(res)  # save output to disk
    
run_recipe(path_to_recipe, outdir)
```

## Available procedures

supported procedures currently:

- translate_header
    - translate the headers of the datapoints
- translate_column
    - translate the values in a column
- identity
    - identity function = nothing changes
- merge
    - merge ingredients together on the keys
- align
    - align two columns in two ingredients
    - discussion: https://github.com/semio/ddf_utils/issues/3
- groupby
    - group ingredient data by keys
    - discussion: https://github.com/semio/ddf_utils/issues/4
- filter_row
    - filter ingredient data by values
    - discussion: https://github.com/semio/ddf_utils/issues/2
- filter_item
    - filter ingredient data by concepts
    - discussion: https://github.com/semio/ddf_utils/issues/14
- run_op
    - run math operations on ingredient
    - discussion: https://github.com/semio/ddf_utils/issues/7
- accumulate
    - run cumulative functions over an ingredient
- copy
    - make copy of indicators of ingredient

### General guideline for writing recipes

- if you need to use `translate_header`/`translate_column`/`align`/`copy` in your
recipe, place them at the beginning of recipe. This can improve the performance
of running the recipe.
