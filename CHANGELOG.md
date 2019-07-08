## version 1.0.2 2019-07-08

- ddf reader: read synonyms for entity set from entity domain synonyms
- chef: imporvments on `window` procedure, improvements for `to_recipe` method
- datapackage: improvments on ddfschema generation process
- minor changes

## version 1.0.1 2019-06-29

- added options to modify column headers for chef output (#116)
- fixed an chef debug option issue
- added more examples/docs

## version 1.0.0 2019-05-20

- DDF data models/Chef data models are re-written
- various improvments/bug fix on chef procedures
- updated project sturcture
- improved documents and tests

## version 0.6.5 2019-03-17

- fix click version issue when install with pip
- add default fields to create_datapackage

## version 0.6.4 2019-03-09

- fix exception when creating datapackage for files with multiple indicators (issue #113)
- fix factory method issue for cdiac
- factory method for IGME no longer works. mark it in doc.

## version 0.6.3 2019-03-04

- fix wrong requirement (issue #115)

## version 0.6.1/0.6.2 2019-03-04

- fix installation error (issue #114)

## version 0.6.0 2019-01-16

- updated module structure, move functions to correct place
- type annotation for classes

## version 0.5.1 2018-11-07

- updated trend_bridge, adding bridge start/end detection
- updated factory methods for a few sources
- improvments and bug fix in chef

## version 0.5.0 2018-10-17

- split `procedure.py` into seperated files for each procedure
- bug fix, improvments for procedures
- bug fix, improvments for datapackage loading and creation of datapackage.json
- more factory methods for data sources

## version 0.4.2 2018-04-21

- bug fix for various procedures
- improvment the running time for recipes (#98)

## version 0.4.1 2018-03-23

- update commandline options for `ddf` sub-commands
- update documents

## version 0.4.0 2018-03-20

- use dask and categorical types to improve memory usage (#90)
- sorting column/row order before serving result in Chef
- add `breakpoint` option to chef procedures to stop and debug chef
- add data/metadata downloading functions to various data sources
- minor changes and improvments

## version 0.3.3 2017-11-29

- new support Hy mode
- bug fixes and improvements

## version 0.3.2 2017-09-28

- add support for reading mutiple indicators in one DDF datapoints file (#76)
- add support for reading repo from github / local path (#79)
- add dry_run option in Ingredient to speed up loading speed
- get_datapoints_df() for Dataset object always return a DataFrame
- to_graph() for Chef now includes procedure names
- bug fixes and improvements

## version 0.3.1 2017-08-14

- support mongo-like queries in ingredient definition and filter procedures (#72)
- new procedure: filter (#72)
- inline ingredients in ingredients section and procedures (#36)
- external csv file as ingredient (#36)
- colourful terminal output for chef
- bug fix and minor improvements

## version 0.3.0 2017-07-18

- totally rewrite the ddf reader, make separated models for dataset and datapackage in
  `ddf_utils.models`.
- add Chef class, which can create or load recipes interactively. Now all recipe related
  tasks should run under a Chef instance.
- all procedures takes a chef instance as first arguments now. Also we support writing
  custom procedures now.
- removed `indentity` and `copy` procedure, updated `filter_row` and `groupby` procedure
- added new `flatten` procedure
- minor improvements

## version 0.2.20 2017-06-17

- bug fix: #67

## version 0.2.19 2017-06-16

- add ddfSchema creation to datapackage
- bug fix and improvements

## version 0.2.18 2017-06-01

- bug fix in datapackage generation

## version 0.2.17 2017-06-01

- performance improvement on datapackage generation
- add progress bar to datapackage generation

## version 0.2.16 2017-05-31

- bug fix in ddfSchema generation

## version 0.2.15 2017-05-31

- the chef_new module, which is to replace the chef module. But it's still WIP, not working yet.
- ddf_utils.index renamed to ddf_utils.datapackage
- new interface for DDF Dataset and Datapackage
- removed python 3.3 support, because it's not supported by latest pandas now
- add support for ddfSchema creation
- bug fix for procedures/functions
- minor improvments

## version 0.2.14 2017-04-07

- new subcommands for `ddf`: `diff` for comparing 2 datasets
- DDF() now accepts absolute path to datasets

## version 0.2.13 2017-03-22

- new subcommands for `ddf`: `validate_recipe` and `build_recipe`
- include a recipe schema for validating recipes
- documents for new commands
- minor improvments

## version 0.2.12 2017-03-16

- bug fix in several procedures
- minor improvments

## version 0.2.11 2017-03-04

- new `insert_keys` option to `groupby` procedure
- new `keep_columns` option to `fliter_row` procedure
- new `split_datapoints_by` and `sub_folder` option for serving
- misc improvements and bug fixes

## version 0.2.10 2017-02-14

- new options for `extract_concepts` procedure (#40)
- the `key` parameter for ingredients now only accepts string (#39)
    - Note: this will break recipes worked on pervious version with entities
      ingredients which `key`s are lists.
- `translate_column` can ask for user input when ambiguity found (#34)
- the ingredient dataframe's dtype will set according to the concepts table (#43)
- added debug options to all procedures (#46)
- added `trend_bridge` procedure (#42)
- code cleanup and formatting
- added lots of documents

## version 0.2.9 2017-01-03

- added tests for all procedures
- removed outdated procedures (align, accumulate)
- more api docs, and all docs are available in read the docs now
- new `--ddf_dir` option for `ddf run_recipe`  #45
- add options for `serve` procedures and `serving` section. Now you should
provide a list of dictionaries in `serving` section, instead of a list of
ids as pervious version
- minor improvements and bug fixes

## version 0.2.8 2016-12-13

- new proecedures: `window` (#25)
- updated `groupby` procedure (#25)
- updated `translate_column` procedure to include the function in `align` (#3)
- minor improvements

## version 0.2.7 2016-12-06

- use DAG to model the recipe. changes are:
    - procedure result can not have same id with other ingredients
    (can't overwrite existing ingredients)
    - the `result` of procedure is mandantory field now
    - recipe cooking procedures can be written in any order. Chef will check dependencies
    - new show-tree option to display a tree view of procedures/ingredients in recipe
- added support for serve section
- renamed procedure `add_concepts` to `extract_concepts` #40
