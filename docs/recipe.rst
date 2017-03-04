Recipe Cookbook (draft)
=======================

What is Recipe
--------------

Recipe is a `Domain-specific language(DSL)`_ for DDF datasets, to manipulate
existing datasets and create new ones. By reading and running the recipe
executor(Chef), one can generate a new dataset easily with the procedures we
provide. Continue with `Write Your First Recipe`_ or `Structure of a Recipe`_ to
learn how to use recipes.

.. _Domain-specific language(DSL): https://en.wikipedia.org/wiki/Domain-specific_language

Write Your First Recipe
-----------------------

In this section we will go though the usage of recipes. Suppose you are a data
provider and you have access to 2 DDF datasets, `ddf--gapminder--population`_
and `ddf--bp--energy`_. Now you want to make a new dataset, which contains **oil
consumption per person** data for each country. Let's do it with Recipe!

.. _ddf--gapminder--population: https://github.com/open-numbers/ddf--gapminder--population 
.. _ddf--bp--energy: https://github.com/semio/ddf--bp--energy

0. Prologue
~~~~~~~~~~~

Before you begin, you need to create a project. With the command line tool
:ref:`ddf <ddf-cli>`, we can get a well designed dataset project directory.
Just run ``ddf new`` and answer some questions. The script will generate the
directory for you. When it's done we are ready to write the recipe.

As you might notice, there is a template in project_root/recipes/, and it's in
YAML_ format. Chef support recipes in both YAML and JSON format, but we
recommend YAML because it's easier to write and read. In this document we will
use YAML recipes.

.. _YAML: https://en.wikipedia.org/wiki/YAML 

1. Add basic info
~~~~~~~~~~~~~~~~~

First of all, we want to add some meta data to describe the target dataset. This
kind of information can be written in the ``info`` section. The ``info`` section
is optional and it's only for human reading for now. You can put anything you
want in this section. In our example, we add the following information:

.. code-block:: yaml

   info:
       id: ddf--your_company--oil_per_person
       author: your_company
       version: v1
       license: MIT
       language: en
       base:
           - ddf--gapminder--population
           - ddf--gapminder--geo_entity_domain
           - ddf--bp--energy

2. Set Options
~~~~~~~~~~~~~~

The next thing to do is to tell Chef where to look for the source datasets. We
can set this option in ``config`` section.

.. code-block:: yaml

   config:
       ddf_dir: /path/to/datasets/

There are other options you can set in this section, check `config section`_ for
available options.

3. Define Ingredients
~~~~~~~~~~~~~~~~~~~~~

The basic object in recipe is ingredient. A ingredient defines a collection of
data which comes form existing dataset or result of computation between several
ingredients. To define ingredients form existing datasets, we append the
``ingredients`` section; to define ingredients from computation, we append the
``cooking`` section.

The ``ingredients`` section is a list of ingredient objects. An
ingredient should be defined with following parameters:

-  ``id``: the name of the ingredient
-  ``dataset``: the dataset where the ingredient is from
- ``key``: the primary keys to filter from datapackage, should be comma
  seperated strings
- ``value``: a list of concept names to filter from the result of filtering
  keys, or pass "*" to select all.
- ``row_filter``: optional, only select rows match the filter. The
  filter is a dictionary where keys are colunm names and values are
  values to filter

In our example, we need datapoints from both gapminder population dataset and
oil consumption datapoints from bp dataset. Noticing the bp is using lower case
short names for its geo and gapminder is using 3 letter iso for its country
entities, we shoule align them to use one system too. So we end up with below
ingredients:

.. code-block:: yaml

   ingredients:
       - id: oil-consumption-datapoints
         dataset: ddf--bp--energy
         key: geo, year
         value:
             - oil_consumption_tonnes
       - id: population-datapoints
         dataset: ddf--gapminder--population
         key: country, year
         value: "*"  # note that the * symbol is reserved symbol in yaml,
                     # we should quote it if we mean a string
       - id: bp-geo-entities
         dataset: ddf--bp--energy
         key: geo
         value: "*"
       - id: gapminder-country-entities
         dataset: ddf--gapminder--population
         key: country
         value: "*"

4. Add Cooking Procedures
~~~~~~~~~~~~~~~~~~~~~~~~~

We have all ingredients we need, the next step is to cook with these
ingredients. In recipe we put all cooking procedures under the ``cooking``
section. Because in DDF model we have 3 kinds of collections: ``concepts``,
``datapoints`` and ``entities``, we divide the cooking section into 3
corresponding sub-sections, and in each section will be a list of
``procedures``. So the basic format is:

.. code-block:: yaml

   cooking:
       concepts:
           # procedures for concepts here
       entities:
           # procedures for entities here
       datapoints:
           # procedures for datapoints here
           
Procedures are like functions. They take ingredients as input, operate with
options, and return new ingredients as result. For a complete list of supported
procedures, see `Available Procedures`_. With this in mind, we can start writing
our cooking procedures. Suppose after some discussion, we decided our task list
is:

- datapoints: oil consumption per capita, and use country/year as dimensions.
- entities: use the country entities from Gapminder
- concepts: all concepts from datapoints and entities

Firstly we look at datapoints. What we need to do to get what we need are:

1. change the dimensions to country/year for bp and gapminder datapoints
2. align bp datapoints to use gapminder's country entities
3. calculate per capita data

We can use `translate_header`_, `translate_column`_, `merge`_, `run_op`_ to get
these tasks done.

.. code-block:: yaml

   datapoints:
       # change dimension for bp
       - procedure: translate_header
         ingredients:
             - bp-datapoints
         options:
             dictionary:
                 geo: country
         result: bp-datapoints-translated

       # align bp geo to gapminder country
       - procedure: translate_column
         ingredients:
             - bp-geo-entities
         result: bp-geo-translated
         options:
             column: geo_name  # the procedure will search for values in this column
             target_column: country  # ... and put the matched value in this column
             dictionary:
                 base: gapminder-country-entities
                 # key is the columns to search for match of geo names
                 key: ['gapminder_list', 'alternative_1', 'alternative_2', 'alternative_3',
                       'alternative_4_cdiac', 'pandg', 'god_id', 'alt_5', 'upper_case_name',
                       'arb1', 'arb2', 'arb3', 'arb4', 'arb5', 'arb6', 'name', 'iso3166_1_alpha2',
                       'iso3166_1_alpha3', 'iso3166_2'
                      ]
                 # value is the column to get new value
                 value: country

         # align bp datapoints to new bp entities
         - procedure: translate_column
           ingredients:
               - bp-datapoints-translated
           result: bp-datapoints-translated-aligned
           options:
               column: country
               target_column: country
               dictionary:
                   base: bp-geo-translated
                   key: geo
                   value: country

         # merge bp/gapminder data and calculate the result
         - procedure: merge
           ingredients:
               - bp-datapoints-translated-aligned
               - population-datapoints
           result: bp-population-merged-datapoints
         - procedure: run_op
           ingredients:
               - bp-population-merged-datapoints
           option:
               op:
                   oil_consumption_per_capita: |
                       oil_consumption_tonnes * 1000 / population
           result: datapoints-calculated
         # only keep the indicator we need
         - procedure: filter_item
           ingredients:
               - datapoints-calculated
           options:
               items:
                   - oil_consumption_per_capita
           result: datapoints-final

For entities, we will just use the country entities from gapminder, so we can skip this part.
For concepts, we need to extract concepts from the ingredients:

.. code-block:: yaml

   concepts:
       - procedure: extract_concepts
         ingredients:
             - datapoints-final
             - gapminder-country-entities
         result: concepts-final
         options:
             overwrite:  # manually set some concept_types
                 year: time
                 country: entity_domain


5. Serve Dishes
~~~~~~~~~~~~~~~

After all these procedure, we have cook the dishes and it's time to serve it! In
recipe we can set which ingredients are we going to serve(save to disk) in the
``serve`` section. Note that this section is optional, and if you don't specify
then the last procedure of each sub-section of ``cooking`` will be served.

.. code-block:: yaml

   serve:
       - id: concepts-final
       - id: gapminder-country-entities
       - id: datapoints-final
           
Now we have finished the recipe. For the complete recipe, please check this
`gist`_.

.. _gist: https://gist.github.com/semio/63bdc3414336ed6e0be164e115d04169

6. Running the Recipe
~~~~~~~~~~~~~~~~~~~~~

To run the recipe to generate the dataset, we use the ddf command line tool. Run
the following command and it will cook for you and result will be saved into
``out_dir``.

.. code-block:: bash

   ddf run_recipe -i example.yml -o out_dir

If you want to just do a dry run without saving the result, you can run with the
``-d`` option.

.. code-block:: bash

   ddf run_recipe -i example.yml -d

Now you have learned the basics of Recipe. We will go though more details in
Recipe in the next sections.

Structure of a Recipe
---------------------

A recipe is made of following parts:

-  basic info
-  configuration
-  includes
-  ingredients
-  cooking procedures
-  serving section

A recipe file can be in either json or yaml format. We will explain each
part of recipe in details in the next sections.

info section
~~~~~~~~~~~~

All basic info are stored in ``info`` section of the recipe. an ``id``
field is required inside this section. Any other information about the
new dataset can be store inside this section, such as ``name``,
``provider``, ``description`` and so on. This part is mainly for human
and is optional for now, but **later on we might add connections for
this section and the datapackage file.**


config section
~~~~~~~~~~~~~~

Inside ``config`` section, we define the configuration of dirs.
currently we can set below path:

-  ``ddf_dir``: the directory that contains all ddf csv repos. Must set
   this variable in the main recipe to run with chef, or provide as an
   command line option using the ``ddf`` utility.
-  ``recipes_dir``: the directory contains all recipes to include. Must
   set this variable if we have ``include`` section. If relative path is
   provided, the path will be related to the path of the recipe.
-  ``dictionary_dir``: the directory contains all translation files.
   Must set this variable if we have json file in the options of
   procedures. (translation will be discussed later). If relative path
   is provided, the path will be related to the path of the recipe.

include section
~~~~~~~~~~~~~~~

A recipe can include other recipes inside itself. to include a recipe,
simply append the filename to the ``include`` section. note that it
should be a absolute path or a filename inside the ``recipes_dir``.

ingredients section
~~~~~~~~~~~~~~~~~~~

A recipe must have some ingredients for cooking. We can either define
the ingredients in the ``ingredients`` section or include other recipes
which have an ``ingredients`` section.

The ``ingredients`` section is a list of ingredient objects. An
ingredient should be defined with following parameters:

-  ``id``: the name of the ingredient
-  ``dataset``: the dataset where the ingredient is from
- ``key``: the primary keys to filter from datapackage, should be comma
   seperated strings
- ``value``: a list of concept names to filter from the result of filtering
   keys, or pass "*" to select all.
-  ``row_filter``: optional, only select rows match the filter. The
   filter is a dictionary where keys are colunm names and values are
   values to filter

Here is an example of ``ingredient`` section:

.. code-block:: yaml

    ingredients:
      - id: example-concepts
        dataset: ddf_example_dataset
        key: concept
        value: "*"
        row_filter:
          concept:
            - geo
            - time
            - some_measure_concept
      - id: example-datapoints
        dataset: ddf_example_dataset
        key: geo, time
        value: some_measure_concept
      - id: example-entities
        dataset: ddf_example_dataset
        key: geo
        value: "*"

cooking section
~~~~~~~~~~~~~~~

``cooking`` section is a dictionary contains one or more list of
procedures to build a dataset. valid keys for cooking section are
*datapoints*, *entities*, *concepts*.

The basic format of a procedure is:

.. code-block:: yaml

    procedure: proc_name
    ingredients:
      - ingredient_to_run_the_proc
    options:  # options object to pass to the procedure
      foo: baz
    result: id_of_new_ingredient

Available procedures will be shown in the below
`section <#available-procedures>`__.

serving section and serve procedure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For now there are 2 ways to tell chef which ingredients should be
served, and you can choose one of them, but not both.

**serve procedure**

``serve`` procedure should be placed in ``cooking`` section, with the
following format:

.. code-block:: yaml

    procedure: serve
    ingredients:
      - ingredient_to_serve
    options:
      opt: val

multiple serve procedures are allowed in each cooking section.

**serving section**

``serving`` section should be a top level object in the recipe, with
following format:

.. code-block:: yaml

    serving:
      - id: ingredient_to_serve_1
        options:
          opt: val
      - id: ingredient_to_serve_2
        options:
          foo: baz

**available options**

``digits`` : *int*, controls how many decimal should be kept at most in
a numeric ingredient.

Recipe execution
----------------

To run a recipe, you can use the ``ddf run_recipe`` command:

.. code-block:: shell

    $ ddf run_recipe -i path_to_rsecipe.yaml -o output_dir

You can specify the path where your datasets are stored:

.. code-block:: shell

    $ ddf run_recipe -i path_to_recipe.yaml -o output_dir --ddf_dir path_to_datasets

Internally, the process to generate a dataset have following steps:

-  read the main recipe into Python object
-  if there is include section, read each file in the include list and
   expand the main recipe
-  if there is file name in dictionary option of each procedure, try to
   expand them if the option value is a filename
-  check if all datasets are available
-  build a procedure dependency tree, check if there are loops in it
-  if there is no ``serve`` procedure and ``serving`` section, the last
   procedure result for each section will be served. If there is
   ``serve`` procedure or ``serving`` section, chef will serve the
   result as described
-  run the procedures for each ingredient to be served and their
   dependencies
-  save output to disk

If you want to embed the function into your script, you can write script
like this:

.. code-block:: python

    import ddf_utils.chef as chef

    def run_recipe(recipe_file, outdir):
        recipe = chef.build_recipe(recipe_file)  # get all sub-recipes and dictionaries
        res = chef.run_recipe(recipe)  # run the recipe, get output for serving
        chef.dishes_to_disk(res)  # save output to disk

    run_recipe(path_to_recipe, outdir)

Available procedures
--------------------

Currently supported procedures:

-  `translate\_header <#translate-header>`__: change ingredient data
   header according to a mapping dictionary
-  `translate\_column <#translate-column>`__: change column values of
   ingredient data according to a mapping dictionary
-  `identity <#identity>`__: return the ingredient as is
-  `merge <#merge>`__: merge ingredients together on their keys
-  `groupby <#groubby>`__: group ingredient by columns and do
   aggregate/filter/transform
-  `window <#window>`__: run function on rolling windows
-  `filter\_row <#filter-row>`__: filter ingredient data by column
   values
-  `filter\_item <#filter-item>`__: filter ingredient data by concepts
-  `run\_op <#run-op>`__: run math operations on ingredient columns
-  `copy <#copy>`__: make copy of columns of ingredient data
-  `extract\_concepts <#extract-concepts>`__: generate concepts
   ingredient from other ingredients
-  `trend\_bridge <#trend-bridge>`__\ (WIP): connect 2 ingredients and
   make custom smoothing

translate\_header
~~~~~~~~~~~~~~~~~

Change ingredient data header according to a mapping dictionary.

**usage and options**

.. code-block:: yaml

    procedure: translate_header
    ingredients:  # list of ingredient id
      - ingredient_id
    result: str  # new ingledient id
    options:
      dictionary: str or dict  # file name or mappings dictionary

**notes**

-  if ``dictionary`` option is a dictionary, it should be a dictionary
   of oldname -> newname mappings; if it's a string, the string should
   be a json file name that contains such dictionary.
-  currently chef only support one ingredient in the ``ingredients``
   parameter

translate\_column
~~~~~~~~~~~~~~~~~

Change column values of ingredient data according to a mapping
dictionary, the dictionary can be generated from an other ingredient.

**usage and options**

.. code-block:: yaml

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

**notes**

-  If ``base`` is provided in ``dictionary``, ``key`` and ``value``
   should also in ``dictionary``. In this case chef will generate a
   mapping dictionary using the ``base`` ingredient. The dictionary
   format will be:

.. code-block:: yaml

    dictionary:
        base: str  # ingredient name
        key: str or list  # the columns to be the keys of the dictionary, can accept a list
        value: str  # the column to be the values of the the dictionary, must be one column

-  currently chef only support one ingredient in the ``ingredients``
   parameter

**examples**

here is an example when we translate the BP geo names into Gapminder's

.. code-block:: yaml

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

identity
~~~~~~~~

Return the ingredient as is.

**usage and options**

.. code-block:: yaml

    procedure: identity
    ingredients:  # list of ingredient id
      - ingredient_id
    result: str  # new ingledient id
    options:
      copy: bool  # if true, treat all data as string, default is false

**notes**

-  currently chef only support one ingredient in the ``ingredients``
   parameter

merge
~~~~~

Merge ingredients together on their keys.

**usage and options**

.. code-block:: yaml

    procedure: merge
    ingredients:  # list of ingredient id
      - ingredient_id_1
      - ingredient_id_2
      - ingredient_id_3
      # ...
    result: str  # new ingledient id
    options:
      deep: bool  # use deep merge if true

**notes**

-  The ingredients will be merged one by one in the order of how they
   are provided to this function. Later ones will overwrite the pervious
   merged results.
-  **deep merge** is when we check every datapoint for existence if
   false, overwrite is on the file level. If key-value (e.g.
   geo,year-population\_total) exists, whole file gets overwritten if
   true, overwrite is on the row level. If values (e.g.
   afr,2015-population\_total) exists, it gets overwritten, if it
   doesn’t it stays

groupby
~~~~~~~

Group ingredient by columns and do aggregate/filter/transform.

**usage and options**

.. code-block:: yaml

    procedure: groupby
    ingredients:  # list of ingredient id
      - ingredient_id
    result: str  # new ingledient id
    options:
      groupby: str or list  # colunm(s) to group
      aggregate: dict  # function block
      transform: dict  # function block
      filter: dict  # function block
      insert_key: dict  # manually add columns

**notes**

- Only one of \ ``aggregate``, ``transform`` or ``filter`` can be used
  in one procedure.
- Any columns not mentioned in groupby or functions are dropped.
- If you want to add back dropped columns with same values, use ``insert_key``
  option.
- Currently chef only support one ingredient in the ``ingredients``
  parameter

**function block**

Two styles of function block are supported, and they can mix in one
procedure:

.. code-block:: yaml

    aggregate:  # or transform, filter
      col1: sum  # run sum to col1
      col2: mean
      col3:  # run foo to col3 with param1=baz
        function: foo
        param1: baz

window
~~~~~~

Run function on rolling windows.

**usage and options**

.. code-block:: yaml

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

**function block**

Two styles of function block are supported, and they can mix in one
procedure:

.. code-block:: yaml

    aggregate:
      col1: sum  # run rolling sum to col1
      col2: mean  # run rolling mean to col2
      col3:  # run foo to col3 with param1=baz
        function: foo
        param1: baz

**notes**

-  currently chef only support one ingredient in the ``ingredients``
   parameter

filter\_row
~~~~~~~~~~~

Filter ingredient data by column values. By default, it will remove columns if
they contain only one value.

**usage and options**

.. code-block:: yaml

    procedure: filter_row
    ingredients:  # list of ingredient id
      - ingredient_id
    result: str  # new ingledient id
    options:
      dictionary: dict  # filter definition block
      keep_all_columns: bool  # if true don't drop any columns

**filter definition**

A filter definitioin block have following format:

.. code-block:: yaml

    new_column_name:
      from: column_name_to_filter
      key_col_1: object  # type should match the data type of the key column, can be a list
      key_col_2: object

**example**

An example can be found in this `github
issue <https://github.com/semio/ddf_utils/issues/2#issuecomment-254132615>`__.

**notes**

-  currently chef only support one ingredient in the ``ingredients``
   parameter

filter\_item
~~~~~~~~~~~~

Filter ingredient data by concepts.

**usage and options**

.. code-block:: yaml

    procedure: filter_item
    ingredients:  # list of ingredient id
      - ingredient_id
    result: str  # new ingledient id
    options:
      items: list  # a list of items should be in the result ingredient

**notes**

-  currently chef only support one ingredient in the ``ingredients``
   parameter

run\_op
~~~~~~~

Run math operations on ingredient columns.

**usage and options**

.. code-block:: yaml

    procedure: run_op
    ingredients:  # list of ingredient id
      - ingredient_id
    result: str  # new ingledient id
    options:
      op: dict  # column name -> calculation mappings

**notes**

-  currently chef only support one ingredient in the ``ingredients``
   parameter

**Examples**

for exmaple, if we want to add 2 columns, ``col_a`` and ``col_b``, to
create an new column, we can write

.. code-block:: yaml

    procedure: run_op
    ingredients:
      - ingredient_to_run
    result: new_ingredient_id
    options:
      op:
        new_col_name: "col_a + col_b"

copy
~~~~

Make copy of columns of ingredient data.

**usage and options**

.. code-block:: yaml

    procedure: copy
    ingredients:  # list of ingredient id
      - ingredient_id
    result: str  # new ingledient id
    options:
      dictionary: dict  # old name -> new name mappings

**dictionary object**

The ``dictionary`` option should be in following format:

.. code-block:: yaml

    dictionary:
      col1: copy_1_1  # string
      col2:  # list of string 
        - copy_2_1
        - copy_2_2

**notes**

-  currently chef only support one ingredient in the ``ingredients``
   parameter

extract\_concepts
~~~~~~~~~~~~~~~~~

Generate concepts ingredient from other ingredients.

**usage and options**

.. code-block:: yaml

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

**notes**

-  all concepts in ingredients in the ``ingredients`` parameter will be
   extracted to a new concept ingredient
-  ``join`` option is optional; if present then the ``base`` will merge
   with concepts from ``ingredients``
-  ``full_outer`` join means get the union of concepts;
   ``ingredients_outer`` means only keep concepts from ``ingredients``

trend\_bridge
~~~~~~~~~~~~~

(WIP) Connect 2 ingredients and make custom smoothing.

**usage and options**

.. code-block:: yaml

    - procedure: trend_bridge
      ingredients:
        - data_ingredient                 # optional, if not set defaults to empty ingredient
      options:
        bridge_start:
          ingredient: old_data_ingredient # optional, if not set then assume it's the input ingredient
          column: concept_old_data
        bridge_end:
          ingredient: new_data_ingredient # optional, if not set then assume it's the input ingredient
          column: concept_new_data
        bridge_length: 5                  # steps in time. If year, years, if days, days.
        bridge_on: time                   # the index column to build the bridge with
        target_column: concept_in_result  # overwrites if exists. creates if not exists.
                                          # defaults to bridge_end.column
      result: data_bridged

Checking Intermediate Results
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most of the procedures supports ``debug`` option, which will save the result
ingredient to ``_debug/<ingredient_id>/`` folder of your working directory. So
if you want to check the intermediate results, just add ``debug: true`` to the
``options`` dictionary.


General guideline for writing recipes
-------------------------------------

- if you need to use
  ``translate_header``/``translate_column``/``align``/``copy`` in your recipe,
  place them at the beginning of recipe. This can improve the performance of
  running the recipe.
- run recipe with ``ddf --debug run_recipe`` will enable debug output when
  running recipes. use it with the ``debug`` option will help you in the
  development of recipes.
