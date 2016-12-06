## version 0.2.7 2016-12-06

- use DAG to model the recipe. changes are:
    - procedure result can not have same id with other ingredients 
    (can't overwrite existing ingredients)
    - the `result` of procedure is mandantory field now
    - recipe cooking procedures can be written in any order. Chef will check dependencies
    - new show-tree option to display a tree view of procedures/ingredients in recipe
- added support for serve section
- renamed procedure `add_concepts` to `extract_concepts` #40
