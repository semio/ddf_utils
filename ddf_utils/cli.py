# -*- coding: utf-8 -*-
#!usr/bin/env python

"""script for ddf dataset management tasks"""

import click
import os
import logging

@click.group()
@click.option('--debug/--no-debug', default=False)
def ddf(debug):
    if debug:
        level = logging.DEBUG
    else:
        level = logging.WARNING
    logging.basicConfig(level=level, format='%(asctime)s -%(levelname)s- %(message)s',
                        datefmt="%H:%M:%S"
                        )

# project management
@ddf.command()
def new():
    """create a new ddf project"""
    from cookiecutter.main import cookiecutter
    cookiecutter('https://github.com/semio/ddf_project_template')


@ddf.command()
@click.argument('how', default='ddf', type=click.Choice(['ddf', 'lang', 'langsplit']))
@click.argument('path', default='./')
@click.option('--force', flag_value=True, default=False)
def cleanup(path, how, force):
    """clean up ddf files or translation files"""
    from ddf_utils.io import cleanup as cl
    from ddf_utils.ddf_reader import is_dataset
    if force:
        cl(path, how)
    else:
        if not is_dataset(path):
            print('not a dataset path: {}. please set correct path or use --force to force run.'.format(path))
        else:
            cl(path, how)
    click.echo('Done.')


@ddf.command()
@click.argument('path')
@click.option('--update', '-u', 'update', flag_value=True, default=False)
def create_datapackage(path, update):
    """create datapackage.json"""
    from ddf_utils.index import get_datapackage
    import json
    if not update:
        if os.path.exists(os.path.join(path, 'datapackage.json')):
            print('datapackage.json already exists. skipping')
            return
        res = get_datapackage(path)
        with open(os.path.join(path, 'datapackage.json'), 'w', encoding='utf8') as f:
            json.dump(res, f, indent=4, ensure_ascii=False)
    else:
        get_datapackage(path, update_existing=True)
    click.echo('Done.')


# chef and recipe
@ddf.command()
@click.option('--recipe', '-i', type=click.Path(exists=True), required=True)
@click.option('--outdir', '-o', type=click.Path(exists=True))
@click.option('--update', 'update', flag_value=False)  # not impletmented
@click.option('--dry_run', '-d', 'dry_run', flag_value=True, default=False)
def run_recipe(recipe, outdir, update, dry_run):
    """generate new ddf dataset with recipe"""
    import ddf_utils.chef as ddfrecipe
    from ddf_utils.index import get_datapackage
    import json
    click.echo('running recipe...')
    recipe = ddfrecipe.build_recipe(recipe)
    if update:
        pass
    res = ddfrecipe.run_recipe(recipe)
    if not dry_run:
        click.echo('saving result to disk...')
        ddfrecipe.dish_to_csv(res, outdir)
        click.echo('creating datapackage file...')
        res = get_datapackage(outdir)
        with open(os.path.join(outdir, 'datapackage.json'), 'w', encoding='utf8') as f:
            json.dump(res, f, indent=4, ensure_ascii=False)
    click.echo("Done.")


# Translation related tasks
@ddf.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('--type', '-t', 'dtype', type=click.Choice(['csv', 'json']))
@click.option('--overwrite/--no-overwrite', default=False)
@click.option('--split_path', default='langsplit')
@click.option('--exclude_concepts', '-x', multiple=True)
def split_translation(path, split_path, dtype, exclude_concepts, overwrite):
    """split ddf files for crowdin translation"""
    from ddf_utils.i18n import split_translations_csv, split_translations_json
    if dtype == 'csv':
        split_translations_csv(path, split_path, exclude_concepts, overwrite)
    elif dtype == 'json':
        split_translations_json(path, split_path, exclude_concepts, overwrite)
    click.echo('Done.')


@ddf.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('--overwrite/--no-overwrite', default=False)
@click.option('--type', '-t', 'dtype', type=click.Choice(['json', 'csv']))
@click.option('--split_path', default='langsplit')
@click.option('--lang_path', default='lang')
def merge_translation(path, split_path, lang_path, dtype, overwrite):
    """merge all translation files from crowdin"""
    from ddf_utils.i18n import merge_translations_csv, merge_translations_json
    if dtype == 'csv':
        merge_translations_csv(path, split_path, lang_path, overwrite)
    else:
        merge_translations_json(path, split_path, lang_path, overwrite)
    click.echo('Done.')


if __name__ == '__main__':
    ddf()
