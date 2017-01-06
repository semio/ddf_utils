# Cooking with recipes (draft)

## What is recipe

Recipe is a [Domain-specific language(DSL)][1] for DDF datasets, to manipulate existing
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
procedure: proc_name
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
$ ddf run_recipe -i path_to_recipe.yaml -o output_dir
```

You can specify the path where your datasets are stored:

```shell
$ ddf run_recipe -i path_to_recipe.yaml -o output_dir --ddf_dir path_to_datasets
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

Currently supported procedures:

- [translate_header](#translate-header): change ingredient data header according to a mapping dictionary
- [translate_column](#translate-column): change column values of ingredient data according to a mapping dictionary
- [identity](#identity): return the ingredient as is
- [merge](#merge): merge ingredients together on their keys
- [groupby](#groubby): group ingredient by columns and do aggregate/filter/transform
- [window](#window): run function on rolling windows
- [filter_row](#filter-row): filter ingredient data by column values
- [filter_item](#filter-item): filter ingredient data by concepts
- [run_op](#run-op): run math operations on ingredient columns
- [copy](#copy): make copy of columns of ingredient data
- [extract_concepts](#extract-concepts): generate concepts ingredient from other ingredients
- [trend_bridge](#trend-bridge)(WIP): connect 2 ingredients and make custom smoothing

### translate_header

Change ingredient data header according to a mapping dictionary.

**usage and options**

```yaml
procedure: translate_header
ingredients:  # list of ingredient id
  - ingredient_id
result: str  # new ingledient id
options:
  dictionary: str or dict  # file name or mappings dictionary
```

**notes**

- if `dictionary` option is a dictionary, it should be a dictionary of oldname -> newname mappings; if it's a string, the string should be a json file name that contains such dictionary.
- currently chef only support one ingredient in the `ingredients` parameter

### translate_column

Change column values of ingredient data according to a mapping dictionary, the dictionary can be generated from an other ingredient.

**usage and options**

```yaml
procedure: translate_column
ingredients:  # list of ingredient id
  - ingredient_id
result: str  # new ingledient id
options:
  column: str  # the column to be translated
  target_column: str  # optinoal, the target column to store the translated data
  not_found: {'drop', 'include', 'error'}  # optional, the behavior when there is values not found in the mapping dictionary, default is 'drop'
  ambiguity: {'prompt', 'skip', 'error'}  # optional, the behavior when there is ambiguity in the dictionary
  dictionary: str or dict  # file name or mappings dictionary
```

**notes**

- If `base` is provided in `dictionary`, `key` and `value` should also in `dictionary`. In this case chef will generate a mapping dictionary using the `base` ingredient. The dictionary format will be:

```yaml
dictionary:
	base: str  # ingredient name
	key: str or list  # the columns to be the keys of the dictionary, can accept a list
	value: str  # the column to be the values of the the dictionary, must be one column
```

- currently chef only support one ingredient in the `ingredients` parameter

**examples**

here is an example when we translate the BP geo names into Gapminder's

```yaml
procedure: translate_column
ingredients:
	- bp-geo
options:
	column: name
	target_column: geo_new
	dictionary:
		base: gw-countries
		key: ['alternative_1', 'alternative_2', 'alternative_3',
			'alternative_4_cdiac', 'pandg', 'god_id', 'alt_5', 'upper_case_name',
			'iso3166_1_alpha2', 'iso3166_1_alpha3', 'arb1', 'arb2', 'arb3', 'arb4',
			'arb5', 'arb6', 'name']
		value: country
	not_found: drop
result: geo-aligned
```

### identity

Return the ingredient as is.

**usage and options**

```yaml
procedure: identity
ingredients:  # list of ingredient id
  - ingredient_id
result: str  # new ingledient id
options:
  copy: bool  # if true, treat all data as string, default is false
```

**notes**

- currently chef only support one ingredient in the `ingredients` parameter

### merge

Merge ingredients together on their keys.

**usage and options**

```yaml
procedure: merge
ingredients:  # list of ingredient id
  - ingredient_id_1
  - ingredient_id_2
  - ingredient_id_3
  # ...
result: str  # new ingledient id
options:
  deep: bool  # use deep merge if true
```

**notes**

- The ingredients will be merged one by one in the order of how they are provided to this function. Later ones will overwrite the pervious merged results.
- **deep merge** is when we check every datapoint for existence if false, overwrite is on the file level. If key-value (e.g. geo,year-population_total) exists, whole file gets overwritten if true, overwrite is on the row level. If values (e.g. afr,2015-population_total) exists, it gets overwritten, if it doesn’t it stays

### groupby

Group ingredient by columns and do aggregate/filter/transform.

**usage and options**

```yaml
procedure: groupby
ingredients:  # list of ingredient id
  - ingredient_id
result: str  # new ingledient id
options:
  groupby: str or list  # colunm(s) to group
  aggregate: dict  # function block
  transform: dict  # function block
  filter: dict  # function block
```

**notes**

- Only one of `aggregate`, `transform` or `filter` can be used in one procedure.
- Any columns not mentioned in groupby or functions are dropped.
- Currently chef only support one ingredient in the `ingredients` parameter

**function block**

Two styles of function block are supported, and they can mix in one procedure:

```yaml
aggregate:  # or transform, filter
  col1: sum  # run sum to col1
  col2: mean
  col3:  # run foo to col3 with param1=baz
    function: foo
    param1: baz
```

### window

Run function on rolling windows.

**usage and options**

```yaml
procedure: window
ingredients:  # list of ingredient id
  - ingredient_id
result: str  # new ingledient id
options:
  window:
    column: str  # column which window is created from
    size: int or 'expanding'  # if int then rolling window, if expanding then expanding window
    min_periods: int  # as in pandas
    center: bool  # as in pandas
  aggregate: dict
```

**function block**

Two styles of function block are supported, and they can mix in one procedure:

```yaml
aggregate:
  col1: sum  # run rolling sum to col1
  col2: mean  # run rolling mean to col2
  col3:  # run foo to col3 with param1=baz
    function: foo
    param1: baz
```

**notes**

- currently chef only support one ingredient in the `ingredients` parameter

### filter_row

Filter ingredient data by column values.

**usage and options**

```yaml
procedure: filter_row
ingredients:  # list of ingredient id
  - ingredient_id
result: str  # new ingledient id
options:
  dictionary: dict  # filter definition block
```

**filter definition**

A filter definitioin block have following format:

```yaml
new_column_name:
  from: column_name_to_filter
  key_col_1: object  # type should match the data type of the key column, can be a list
  key_col_2: object
```

 **example**

An example can be found in this [github issue](https://github.com/semio/ddf_utils/issues/2#issuecomment-254132615).

**notes**

- currently chef only support one ingredient in the `ingredients` parameter

### filter_item

Filter ingredient data by concepts.

**usage and options**

```yaml
procedure: filter_item
ingredients:  # list of ingredient id
  - ingredient_id
result: str  # new ingledient id
options:
  items: list  # a list of items should be in the result ingredient
```

**notes**

- currently chef only support one ingredient in the `ingredients` parameter

### run_op

Run math operations on ingredient columns.

**usage and options**

```yaml
procedure: run_op
ingredients:  # list of ingredient id
  - ingredient_id
result: str  # new ingledient id
options:
  op: dict  # column name -> calculation mappings
```

**notes**

- currently chef only support one ingredient in the `ingredients` parameter

**Examples**

for exmaple, if we want to add 2 columns, `col_a` and `col_b`, to create an new column, we can write

```yaml
procedure: run_op
ingredients:
  - ingredient_to_run
result: new_ingredient_id
options:
  op:
    new_col_name: "col_a + col_b"
```

### copy

Make copy of columns of ingredient data.

**usage and options**

```yaml
procedure: copy
ingredients:  # list of ingredient id
  - ingredient_id
result: str  # new ingledient id
options:
  dictionary: dict  # old name -> new name mappings
```

**dictionary object**

The `dictionary` option should be in following format:

```yaml
dictionary:
  col1: copy_1_1  # string
  col2:  # list of string 
    - copy_2_1
    - copy_2_2
```

**notes**

- currently chef only support one ingredient in the `ingredients` parameter

### extract_concepts

Generate concepts ingredient from other ingredients.

**usage and options**

```yaml
procedure: extract_concepts
ingredients:  # list of ingredient id
  - ingredient_id_1
  - ingredient_id_2
result: str  # new ingledient id
options:
  join:  # optional
    base: str  # base concept ingredient id
    type: {'full_outer', 'ingredients_outer'}  # default is full_outer
  include_keys: true  # if we should include the primaryKeys of the ingredients
  overwrite:  # overwirte some of the concept types
      year: time
```

**notes**

- all concepts in ingredients in the `ingredients` parameter will be extracted to a new concept ingredient
- `join` option is optional; if present then the `base` will merge with concepts from `ingredients`
- `full_outer` join means get the union of concepts; `ingredients_outer` means only keep concepts from `ingredients`

### trend_bridge

(WIP) Connect 2 ingredients and make custom smoothing.

see discussion [here](https://github.com/semio/ddf_utils/issues/42).


### General guideline for writing recipes

- if you need to use `translate_header`/`translate_column`/`align`/`copy` in your
  recipe, place them at the beginning of recipe. This can improve the performance
  of running the recipe.
