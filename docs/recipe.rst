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
is optional and these metadata will be also written in the `datapackage.json`_
in the final output. You can put anything you want in this section. In our
example, we add the following information:

.. _datapackage.json: http://frictionlessdata.io/guides/data-package/#datapackagejson

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

.. _ingredient def:

3. Define Ingredients
~~~~~~~~~~~~~~~~~~~~~

The basic object in recipe is ingredient. A ingredient defines a collection of
data which comes form existing dataset or result of computation between several
ingredients. To define ingredients form existing datasets or csv files, we
append to the ``ingredients`` section; to define ingredients on the fly, we
append to the ``cooking`` section.

The ``ingredients`` section is a list of ingredient objects. An ingredient that
reads data from a data package should be defined with following parameters:

-  ``id``: the name of the ingredient
-  ``dataset``: the dataset where the ingredient is from
- ``key``: the primary keys to filter from datapackage, should be comma
  seperated strings
- ``value``: a list of concept names to filter from the result of
  filtering keys, or pass "*" to select all. Alternatively a
  mongodb-like query can be used.
- ``filter``: optional, only select rows match the filter. The
  filter is a dictionary where keys are colunm names and values are
  values to filter. Alternatively a mongodb-like query can be used.

There are more parameters for ingredient definition, see the `ingredients
section`_ document.

In our example, we need datapoints from both gapminder population dataset and
oil consumption datapoints from bp dataset. Noticing the bp is using lower case
short names for its geo and gapminder is using 3 letter iso for its country
entities, we should align them to use one system too. So we end up with below
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
``serving`` section. Note that this section is optional, and if you don't specify
then the last procedure of each sub-section of ``cooking`` will be served.

.. code-block:: yaml

   serving:
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
``provider``, ``description`` and so on. Data in this section will be
written into `datapackage.json`_ file of the generated dataset.


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
- ``procedures_dir``: when you want to use `custom procedures`_, you should set
  this option to tell which dir the procedures are in.

include section
~~~~~~~~~~~~~~~

A recipe can include other recipes inside itself. to include a recipe,
simply append the filename to the ``include`` section. note that it
should be a absolute path or a filename inside the ``recipes_dir``.

ingredients section
~~~~~~~~~~~~~~~~~~~

A recipe must have some ingredients for cooking. There are 2 places where we can
define ingredients in recipe:

- in ``ingredients`` section
- in the ``ingredients`` parameter in procedures, which is called on-the-fly
  ingredients

in either case, the format of ingredient definition object is the same. An
ingredient should be defined with following parameters:

- ``id``: the name of the ingredient, which we can refer later in the
  procedures. ``id`` is optional when the ingredient is in a procedure object.
- ``dataset`` or ``data``: one of them should be defined in the ingredient. Use
  ``dataset`` when we want to read data from an dataset, and use ``data`` when
  we want to read data from a csv file.
- ``key``: the primary keys to filter from datapackage, should be comma
  seperated strings
- ``value``: optional, a list of concept names to filter from the result of
  filtering keys, or pass "\*" to select all. Mongo-like queries are also
  supported, see examples below. If omitted, assume "\*".
- ``filter``: optional, only select rows match the filter. The filter is a
  dictionary where keys are colunm names and values are values to filter.
  Mongo-like queries are also supported, see examples below and examples in
  ``filter`` procedure.


Here is an example ingredient object in recipe:

 .. code-block:: yaml

    id: example-ingredient
    dataset: ddf--example--dataset
    key: "geo,time"  # key columns of ingredient
    value:  # only include concepts listed here
      - concept_1
      - concept_2
    filter:  # select rows by column values
      geo:  # only keep datapoint where `geo` is in [swe, usa, chn]
        - swe
        - usa
        - chn

``value`` and ``filter`` can accept mongo like queries to make more
complex statements, for example:

.. code-block:: yaml

   id: example-ingredient
   dataset: ddf--example--dataset
   key: geo, time
   value:
       $nin:  # exclude following indicators
           - concept1
           - concept2
   filter:
       geo:
           $in:
               - swe
               - usa
               - chn
       year:
           $and:
               $gt: 2000
               $lt: 2015

for now, value accepts ``$in`` and ``$nin`` keywords, but only one of
them can be in the value option; filter supports logical keywords:
``$and``, ``$or``, ``$not``, ``$nor``, and comparision keywords:
``$eq``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$ne``, ``$in``,
``$nin``.

The other way to define the ingredient data is using the ``data``
keyword to include external csv file, or inline the data in the
ingredient definition. Example:

.. code-block:: yaml

   id: example-ingredient
   key: concept
   data: external_concepts.csv

You can also create On-the-fly ingredient:

.. code-block:: yaml

   id: example-ingredient
   key: concept
   data:
       - concept: concept_1
         name: concept_name_1
         concept_type: string
         description: concept_description_1
       - concept: concept_2
         name: concept_name_2
         concept_type: measure
         description: concept_description_2


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

- ``digits`` : *int*, controls how many decimal should be kept at most in a
  numeric ingredient.
- ``no_keep_sets`` : *bool*, by default chef will serve the entities by
  entity_sets, i.e. each entity set will have one file. Enabling this will make
  chef serve entire domain in one file, no separated files


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

- `translate\_header <#translate-header>`__: change ingredient data
  header according to a mapping dictionary
- `translate\_column <#translate-column>`__: change column values of
  ingredient data according to a mapping dictionary
- `merge <#merge>`__: merge ingredients together on their keys
- `groupby <#groubby>`__: group ingredient by columns and do
  aggregate/filter/transform
- `window <#window>`__: run function on rolling windows
- `filter`_: filter ingredient data with Mongo-like query
- `filter\_row <#filter-row>`__: filter ingredient data by column
  values
- `filter\_item <#filter-item>`__: filter ingredient data by concepts
- `run\_op <#run-op>`__: run math operations on ingredient columns
- `extract\_concepts <#extract-concepts>`__: generate concepts
  ingredient from other ingredients
- `trend\_bridge <#trend-bridge>`__: connect 2 ingredients and
  make custom smoothing
- `flatten <#flatten>`__: flatten dimensions in the indicators to
  create new indicators
- `split_entity <#split-entity>`__: split an entity and create new entity from it
- `merge_entity <#merge-entity>`__: merge some entity to create a new entity

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

also, we can use wildcard in the column names:

.. code-block:: yaml

    aggregate:  # or transform, filter
      "population*": sum  # run sum to all indicators starts with "population"

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

filter
~~~~~~

Filter ingredient data with Mongo-like queries. You can filter the
ingredient by item, which means indicators in datapoints or columns in
other type of ingredients, and/or by row.

``item`` filter accepts a list of items, or a list followed by ``$in``
or ``$nin``. ``row`` filter accepts a query similar to mongo queries,
supportted keywords are ``$and``, ``$or``, ``$eq``, ``$ne``, ``$gt``,
``$lt``. See below for an example.

**usage and options**:

.. code-block:: yaml

   - procedure: filter
     ingredients:
         - ingredient_id
     options:
         item:  # just as `value` in ingredient definition
             $in:
                 - concept_1
                 - concept_2
         row:  # just as `filter` in ingredient definition
             $and:
                 geo:
                     $ne: usa
                 year:
                     $gt: 2010
      result: output_ingredient

for more information, see the
:py:class:`ddf_utils.chef.model.ingredient.Ingredient` class and
:py:func:`ddf_utils.chef.procedure.filter` function.


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

Connect 2 ingredients and make custom smoothing.

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

flatten
~~~~~~~

Flatten dimension to create new indicators.

This procedure only applies for datapoints ingredients.

**usage and options**

.. code-block:: yaml

    - procedure: flatten
      ingredients:
        - data_ingredient
      options:
        flatten_dimensions:  # a list of dimensions to be flattened
          - entity_1
          - entity_2
        dictionary:  # old name -> new name mappings, supports wildcard and template.
          "old_name_wildcard": "new_name_{entity_1}_{entity_2}"

**example**

For example, if we have datapoints for population by gender, year, country. And gender entity domain
has ``male`` and ``female`` entity. And we want to create 2 seperated indicators: ``population_male``
and ``population_female``. The procedure should be:

.. code-block:: yaml

    - procedure: flatten
      ingredients:
        - population_by_gender_ingredient
      options:
        flatten_dimensions:
          - gender
        dictionary:
          "population": "{concept}_{gender}"  # concept will be mapped to the concept name being flattened


split_entity
~~~~~~~~~~~~

(WIP) split an entity into several entities


merge_entity
~~~~~~~~~~~~

(WIP) merge several entities into one new entity


custom procedures
~~~~~~~~~~~~~~~~~

You can also load your own procedures. The procedure name should be
``module.function``, where ``module`` should be in the ``procedures_dir`` or
other paths in ``sys.path``.

The procedure should be defined as following structure:

.. code-block:: python

   from ddf_utils.chef.cook import Chef
   from ddf_utils.chef.ingredient import ProcedureResult
   from ddf_utils.chef.helpers import debuggable

   @debuggable  # adding debug option to the procedure
   def custom_procedure(chef, ingredients, result, **options):
       # you must have chef(a Chef object), ingredients (a list of string),
       # result (a string) as parameters
       #

       # procedures...

       # and finally return a ProcedureResult object
       return ProcedureResult(chef, result, primarykey, data)

Check our `predefined procedures`_ for examples.

.. _`predefined procedures`: https://github.com/semio/ddf_utils/blob/master/ddf_utils/chef/procedure.py


Checking Intermediate Results
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most of the procedures supports ``debug`` option, which will save the result
ingredient to ``_debug/<ingredient_id>/`` folder of your working directory. So
if you want to check the intermediate results, just add ``debug: true`` to the
``options`` dictionary.


Validate the Result with ddf-validation
---------------------------------------

After generating the dataset, it would be good to check if the output dataset is
valid against the DDF CSV model. There is a tool `ddf-validation`_ for that,
which is written in nodejs.

.. _ddf-validation: https://github.com/Gapminder/ddf-validation

to check if a dataset is valid, install ddf-validation and run:

.. code-block:: shell

   cd path_to_your_dataset
   validate-ddf

Validate Recipe with Schema
---------------------------

In ddf_utils we provided a command for recipe writers to check if the recipe is
valid using a `JSON schema`_ for recipe. The following command check and report
any errors in recipe:

.. _`JSON schema`: http://json-schema.org/

::

   $ ddf validate_recipe input.yaml

Note that if you have includes in your recipe, you may want to build a complete
recipe before validating it. You can firstly build your recipe and validate it:

::

   $ ddf build_recipe input.yaml > output.json

   $ ddf validate_recipe output.json

or just run ``ddf validate_recipe --build input.yaml`` without creating a new
file.

The validate command will output the json paths that are invalid, so that you can
easily check which part of your recipe is wrong. For example,

::

   $ ddf validate_recipe --build etl.yml
   On .cooking.datapoints[7]
   {'procedure': 'translate_header', 'ingredients': ['unpop-datapoints-pop-by-age-aligned'], 'options': {'dictionary_f': {'country_code': 'country'}}, 'result': 'unpop-datapoints-pop-by-age-country'} is not valid under any of the given schemas

For a pretty printed output of the invalid path, try using json processors like
`jq`_:

.. _`jq`: https://stedolan.github.io/jq/

.. code-block:: javascript

  // $ ddf build_recipe etl.yml | jq ".cooking.datapoints[7]"
  {
    "procedure": "translate_header",
    "ingredients": [
      "unpop-datapoints-pop-by-age-aligned"
    ],
    "options": {
      "dictionary_f": {
        "country_code": "country"
      }
    },
    "result": "unpop-datapoints-pop-by-age-country"
  }

Other then the json schema, we can also valiate recipe using ``dhall``, as we will talk about in `next section <Write recipe in Dhall>`_.

Write recipe in Dhall
---------------------

Sometimes there will be recurring tasks, for example, you might
applying same procedures again and again to different ingredients. In
this case we would benefit from Dhall_ language. Also, there are more
advantages on using Dhall over yaml, such as type checking. We provide
type definitions for the recipe in `an other repo`_. Check examples in
the repo to see how to use them.

.. _Dhall: https://dhall-lang.org
.. _`an other repo`: https://github.com/semio/dhall-ddf-recipe

General guidelines for writing recipes
--------------------------------------

- if you need to use ``translate_header`` / ``translate_column`` in your recipe,
  place them at the beginning of recipe. This can improve the performance of
  running the recipe.
- run recipe with ``ddf --debug run_recipe`` will enable debug output when
  running recipes. use it with the ``debug`` option will help you in the
  development of recipes.


The Hy Mode
-----------

From `Hy's home page`_:

    Hy is a wonderful dialect of Lisp that's embedded in Python.

    Since Hy transforms its Lisp code into the Python Abstract Syntax Tree, you
    have the whole beautiful world of Python at your fingertips, in Lisp form!

.. _`Hy's home page`: http://docs.hylang.org/en/stable/index.html

Okay, if you're still with me, then let's dive into the world of Hy recipe!

We provided some macros for writing recipes. They are similar to the sections in
YAML:

.. code-block:: clojure

   ;; import all macros
   (require [ddf_utils.chef.hy_mod.macros [*]])

   ;; you should call init macro at the beginning.
   ;; This will initial a global variable *chef*
   (init)

   ;; info macro, just like the info section in YAML
   (info :id "my_fancy_dataset"
         :date "2017-12-01")

   ;; config macro, just like the config section in YAML
   (config :ddf_dir "path_to_ddf_dir"
           :dictionary_dir "path_to_dict_dir")

   ;; ingredient macro, each one defines an ingredient. Just like a
   ;; list element in ingredients section in YAML
   (ingredient :id "datapoints-source"
               :dataset "source_dataset"
               :key "geo, year")

   ;; procedure macro, each one defines a procedure. Just like an element
   ;; in the cooking blocks. First 2 parameters are the result id and the
   ;; collection it's in.
   (procedure "result-ingredient" "datapoints"
              :procedure "translate_header"
              :ingredients ["datapoints-source"]
              :options {:dictionary
                        {"geo" "country"}})  ;; it doesn't matter if you use keyword or plain string
                                             ;; for the options dictionary's key

    ;; serve macro
    (serve :ingredients ["result-ingredient"]
           :options {"digits" 2})

    ;; run the recipe
    (run)

    ;; you can do anything to the global chef element
    (print (*chef*.to_recipe))



There are more examples in the `example folder`_.

 .. _`example folder`: https://github.com/semio/ddf_utils/tree/master/examples
