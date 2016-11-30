# ddf_utils

## Installation

We are using python3 only features such as type signature in this repo.
So python 3 is required in order to run this module.

```$ pip3 install git+https://github.com/semio/ddf_utils.git```

### For Windows users

If you encounter `failed to create process.` when you run the `ddf` command, please
try updating setuptools to latest version:

`> pip3 install -U setuptools`

and then reinstall ddf_utils should fix the problem.

## Commandline helper

we provide a commandline utility `ddf` for etl tasks. For now supported commands are:

```
$ ddf --help
Usage: ddf [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  cleanup             clean up ddf files or translation files
  create_datapackage  create datapackage.json
  merge_translation   merge all translation files from crowdin
  new                 create a new ddf project
  run_recipe          generate new ddf dataset with recipe
  split_translation   split ddf files for crowdin translation
```

for each subcommands, you can run `ddf <subcommand> --help` to get help
of that subcommand

### Recipe

document for recipe: [link](https://github.com/semio/ddf--gapminder--systema_globalis/blob/feature/autogenerated/etl/recipes/README.md)

to run a recipe, simply run following:

```
$ ddf run_recipe -i path_to_recipe -o outdir
```

to run a recipe without saving the result into disk, run

```
$ ddf run_recipe -i path_to_recipe -d
```

note that you should set `ddf_dir`/`recipes_dir`/`dictionary_dir` correct in order 
the chef can find the correct file. if there are includes in the recipe, only the top 
level `ddf_dir` will be used (so the ddf_dir setting in sub-recipes will be ignored). 

### useful API for etl tasks

You can check the api documents at [readthedoc][1] or clone this repo and read it in
docs/_html. Note that the chef module document is not complete in readthedoc due to a 
bug in their system.

[1]: https://ddf-utils.readthedocs.io/en/latest/py-modindex.html
