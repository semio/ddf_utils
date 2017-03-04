Introduction
============

ddf_utils is a Python library and command line tool for people working with
`Tabular Data Package`_ in `DDF model`_. It provides various functions for ETL_
tasks, including string formatting, data transforming, generating
datapackage.json, reading data form DDF datasets, running :doc:`recipes
<recipe>`, a decleative DSL designed to manipulate datasets to generate new
datasets, and other functions we find useful in daily works in Gapminder_.

.. _Tabular Data Package: http://specs.frictionlessdata.io/tabular-data-package
.. _DDF model: https://github.com/open-numbers/wiki/wiki/Introduction-to-DDF
.. _ETL: https://en.wikipedia.org/wiki/Extract,_transform,_load
.. _Gapminder: https://www.gapminder.org

Installation
------------

We are using python3 only features such as type signature in this repo. So
python 3 is required in order to run this module.

.. code-block:: bash

   $ pip3 install git+https://github.com/semio/ddf_utils.git

For Windows users
~~~~~~~~~~~~~~~~~

If you encounter ``failed to create process.`` when you run the ddf command, please
try updating setuptools to latest version:

.. code-block:: bash

   > pip3 install -U setuptools

Usage
-----

ddf_utils can be use as a library and also a commandline utility.

Library
~~~~~~~

ddf_utils' helper functions are divided into a few modules based on their
domain, namely:

- :py:mod:`chef <ddf_utils.chef.cook>`: Recipe cooking functions. See :doc:`recipe`
  for how to write recipes
- :py:mod:`ddf_reader <ddf_utils.ddf_reader>`: Reader for reading data from DDF datasets
- :py:mod:`i18n <ddf_utils.i18n>`: Splitting/merging translation files
- :py:mod:`index <ddf_utils.index>`: Generating/updating datapackage.json
- :py:mod:`ops <ddf_utils.ops>`: Computations on data
- :py:mod:`patch <ddf_utils.patch>`: Applying patch in `daff format`_
- :py:mod:`str <ddf_utils.str>`: Functions for string/number formatting
- :py:mod:`transformer <ddf_utils.transformer>`: Data transforming functions,
  such as column/row translation, trend bridge, etc.

see above links for documents for each module.

.. _daff format: https://github.com/paulfitz/daff#reading-material

.. _ddf-cli:

Command line helper
~~~~~~~~~~~~~~~~~~~

We provide a commandline utility ``ddf`` for common etl tasks. For now supported
commands are:

::

  $ ddf --help
  Usage: ddf [OPTIONS] COMMAND [ARGS]...

  Options:
    --debug / --no-debug
    --help                Show this message and exit.

  Commands:
    cleanup             clean up ddf files or translation files.
    create_datapackage  create datapackage.json
    merge_translation   merge all translation files from crowdin
    new                 create a new ddf project
    run_recipe          generate new ddf dataset with recipe
    split_translation   split ddf files for crowdin translation

run ``ddf <command> --help`` for detail usage on each command.
