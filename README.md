# ddf_utils

![status](https://img.shields.io/travis/semio/ddf_utils.svg)
![version](https://img.shields.io/pypi/v/ddf_utils.svg)
![pyversion](https://img.shields.io/pypi/pyversions/ddf_utils.svg)
[![codecov](https://codecov.io/gh/semio/ddf_utils/branch/master/graph/badge.svg)](https://codecov.io/gh/semio/ddf_utils)

ddf_utils is a Python library and command line tool for people working with
[Tabular Data Package][1] in [DDF model][2]. It provides various functions for [ETL tasks][3],
including string formatting, data transforming, generating datapackage.json,
reading data form DDF datasets, running [recipes][4], a decleative
DSL designed to manipulate datasets to generate new datasets, and other
functions we find useful in daily works in [Gapminder][5].

[1]: http://specs.frictionlessdata.io/tabular-data-package
[2]: https://open-numbers.github.io/ddf.html
[3]: https://en.wikipedia.org/wiki/Extract,_transform,_load
[4]: https://ddf-utils.readthedocs.io/en/latest/recipe.html
[5]: https://www.gapminder.org/

## Installation

Python 3.6+ is required in order to run this module.

To install this package from pypi, run:

```$ pip install ddf_utils```

To install from the latest source, run:

```$ pip install git+https://github.com/semio/ddf_utils.git```

### For Windows users

If you encounter `failed to create process` when you run the `ddf` command, please
try updating setuptools to latest version:

`$ pip3 install -U setuptools`

and then reinstall ddf_utils should fix the problem.

## Usage

Check the [documents](https://ddf-utils.readthedocs.io/en/latest/intro.html) for
how to use ddf_utils.
