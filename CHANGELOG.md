## version 0.2.9 2017-01-03

- added tests for all procedures
- removed outdated procedures (align, accumulate)
- more api docs, and all docs are available in read the docs now
- new `--ddf_dir` option for `ddf run_recipe`  #45
- add options for `serve` procedures and `serving` section. Now you should
provide a list of dictionaries in `serving` section, instead of a list of
ids as pervious version
- improvements and bug fixes

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

