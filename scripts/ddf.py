# -*- coding: utf-8 -*-
#!usr/bin/env python

"""script for ddf dataset management tasks"""

import click

@click.group()
def ddf():
    pass


@ddf.command()
def new():
    """create a new ddf project"""
    from cookiecutter.main import cookiecutter
    cookiecutter('https://github.com/semio/ddf_project_template')


@ddf.command()
@click.option('--recipe', '-i')
@click.option('--outdir', '-o')
# @click.option('--update', 'update', flag_value=True)  # not impletmented
@click.option('--dry_run', 'dry_run', flag_value=True)
def run_recipe(recipe, outdir, update, dry_run):
    """generate new ddf dataset with recipe"""
    import ddf_utils.chef as ddfrecipe
    import logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s -%(levelname)s- %(message)s',
                        datefmt="%H:%M:%S"
                        )
    print('running recipe...')
    recipe = ddfrecipe.build_recipe(recipe_file)
    if update:
        pass
    res = ddfrecipe.run_recipe(recipe)
    if not dry_run:
        print('saving result to disk...')
        ddfrecipe.dish_to_csv(res, outdir)
        print('creating index file...')
        create_index_file(outdir)
    print("done.")


@ddf.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('--overwrite/--no-overwrite', default=False)
@click.option('--split_path', default='langsplit')
@click.option('--exclude_concepts', '-x', multiple=True)
def split_translation(path, split_path, exclude_concepts, overwrite):
    """split ddf files for crowdin translation"""
    from ddf_utils.i18n import split_translations
    split_translations(path, split_path, exclude_concepts, overwrite)


@ddf.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('--overwrite/--no-overwrite', default=False)
@click.option('--split_path', default='langsplit')
@click.option('--lang_path', default='lang')
def merge_translation(path, split_path, lang_path, overwrite):
    """merge all translation files from crowdin"""
    from ddf_utils.i18n import merge_translations
    merge_translations(path, split_path, lang_path, overwrite)


if __name__ == '__main__':
    ddf()
