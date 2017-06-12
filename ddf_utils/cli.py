# -*- coding: utf-8 -*-
#!/usr/bin/env python

"""script for ddf dataset management tasks"""

import click
import os
import shutil
import logging


@click.group()
@click.option('--debug/--no-debug', default=False)
def ddf(debug):
    if debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
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
@click.option('--force', flag_value=True, default=False, help='force deletion')
def cleanup(path, how, force):
    """clean up ddf files or translation files.

    :arguments

    \b
    how: what to clean, choose from 'ddf', 'lang' and 'langsplit'
    path: the dataset path
    """
    from ddf_utils.io import cleanup as cl
    from ddf_utils.ddf_reader import is_dataset
    if force:
        cl(path, how)
    else:
        if not is_dataset(path):
            print('not a dataset path: {}. Please set correct path or '
                  'use --force to force run.'.format(os.path.abspath(path)))
        else:
            cl(path, how)
    click.echo('Done.')


@ddf.command()
@click.argument('path')
@click.option('--update', '-u', 'update', flag_value=True, default=False,
              help='update existing datapackage.json')
@click.option('--overwrite', '-n', 'overwrite', flag_value=True, default=False,
              help='overwrite existing datapackage.json')
def create_datapackage(path, update, overwrite):
    """create datapackage.json"""
    from ddf_utils.datapackage import get_datapackage
    from ddf_utils.model.package import Datapackage
    import json
    if not update and not overwrite:
        if os.path.exists(os.path.join(path, 'datapackage.json')):
            click.echo('datapackage.json already exists. use --update to update or --overwrite to create new')
            return
        res = get_datapackage(path, use_existing=False)
    else:
        if os.path.exists(os.path.join(path, 'datapackage.json')):
            click.echo('backing up previous datapackage.json...')
            # make a backup
            shutil.copy(os.path.join(path, 'datapackage.json'),
                        os.path.join(path, 'datapackage.json.bak'))
        if overwrite:
            res = get_datapackage(path, use_existing=False)
        else:
            res = get_datapackage(path, use_existing=True, update=True)

    with open(os.path.join(path, 'datapackage.json'), 'w', encoding='utf8') as f:
        json.dump(res, f, indent=4, ensure_ascii=False)
    click.echo('Done.')


# chef and recipe
@ddf.command()
@click.option('--recipe', '-i', type=click.Path(exists=True), required=True)
@click.option('--outdir', '-o', type=click.Path(exists=True))
@click.option('--ddf_dir', type=click.Path(exists=True), default=None)
@click.option('--update', 'update', flag_value=False, help="Don't use. Not implemented yet")
@click.option('--dry_run', '-d', 'dry_run', flag_value=True, default=False,
              help="don't save output to disk")
@click.option('--show-tree', 'show_tree', flag_value=True, default=False,
              help='show the dependency tree')
def run_recipe(recipe, outdir, ddf_dir, update, dry_run, show_tree):
    """generate new ddf dataset with recipe"""
    import ddf_utils.chef as chef
    from ddf_utils.datapackage import get_datapackage, dump_json
    click.echo('building recipe...')
    if ddf_dir:
        recipe = chef.build_recipe(recipe, ddf_dir=ddf_dir)
    else:
        recipe = chef.build_recipe(recipe)
    if show_tree:
        dag = chef.cook.build_dag(recipe)
        dag.tree_view()
        return
    if update:
        pass
    serve = not dry_run
    chef.run_recipe(recipe, serve=serve, outpath=outdir)
    if serve:
        click.echo('creating datapackage file...')
        dump_json(os.path.join(outdir, 'datapackage.json'), get_datapackage(outdir))
    click.echo("Done.")


@ddf.command()
@click.argument('recipe', type=click.Path(exists=True))
@click.option('--format', '-f', type=click.Choice(['json', 'yaml']), default='json',
              help='set output format')
def build_recipe(recipe, format):
    """create a complete recipe by expanding all includes in the input recipe."""
    from ddf_utils.chef.cook import build_recipe as buildrcp
    recipe = buildrcp(recipe)
    fp = click.open_file('-', 'w')
    if format == 'json':
        import json
        json.dump(recipe, fp, indent=4, ensure_ascii=False)
    elif format == 'yaml':
        import yaml
        yaml.dump(recipe, fp)


@ddf.command()
@click.argument('recipe', type=click.Path(exists=True))
@click.option('--build/--no--build', default=False)
def validate_recipe(recipe, build):
    """validate the recipe"""
    import json
    from jsonschema import Draft4Validator
    schema_file = os.path.join(os.path.dirname(__file__), '../res/specs/recipe.json')
    schema = json.load(open(schema_file))
    if build:
        from ddf_utils.chef.cook import build_recipe as buildrcp
        recipe = buildrcp(recipe)
        # reload the recipe to get rid of AttrDict in the object
        recipe = json.loads(json.dumps(recipe))
    else:
        if recipe.endswith('.json'):
            recipe = json.load(open(recipe))
        else:
            import yaml
            recipe = yaml.load(open(recipe))

    v = Draft4Validator(schema)
    errors = list(v.iter_errors(recipe))
    if len(errors) == 0:
        click.echo("The recipe is valid.")
    else:
        for e in errors:
            path = ''
            for p in e.path:
                if isinstance(p, int):
                    path = path + '[{}]'.format(p)
                else:
                    path = path + '.{}'.format(p)
            if path == '':
                path = '.'
            click.echo('On {}'.format(path))
            click.echo(e.message)


# Translation related tasks
@ddf.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('--type', '-t', 'dtype', type=click.Choice(['csv', 'json']), help='split file type',
              default='csv')
@click.option('--overwrite/--no-overwrite', default=False, help='overwrite existing files or not')
@click.option('--split_path', default='langsplit', help='path to langsplit folder')
@click.option('--exclude_concept', '-x', 'exclude_concepts', multiple=True,
              help='concepts to exclude', metavar='concept')
def split_translation(path, split_path, dtype, exclude_concepts, overwrite):
    """split ddf files for crowdin translation"""
    from ddf_utils.i18n import split_translations_csv, split_translations_json
    if dtype == 'csv':
        split_translations_csv(path, split_path, exclude_concepts, overwrite)
    elif dtype == 'json':
        split_translations_json(path, split_path, exclude_concepts, overwrite)
    else:
        click.echo('Please specify correct input type (csv or json) with -t option.')
        return
    click.echo('Done.')


@ddf.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('--overwrite/--no-overwrite', default=False, help='overwrite existing files or not')
@click.option('--type', '-t', 'dtype', type=click.Choice(['json', 'csv']), help='split file type')
@click.option('--split_path', default='langsplit', help='path to langsplit folder')
@click.option('--lang_path', default='lang', help='path to lang folder')
def merge_translation(path, split_path, lang_path, dtype, overwrite):
    """merge all translation files from crowdin"""
    from ddf_utils.i18n import merge_translations_csv, merge_translations_json
    if dtype == 'csv':
        merge_translations_csv(path, split_path, lang_path, overwrite)
    elif dtype == 'json':
        merge_translations_json(path, split_path, lang_path, overwrite)
    else:
        click.echo('Please specify correct input type (csv or json) with -t option.')
        return
    click.echo('Done.')


# for QA
@ddf.command()
@click.argument('dataset1')
@click.argument('dataset2')
@click.option('--git', '-g', is_flag=True)
@click.option('--checkout-path', type=click.Path(), default='./etl/diff')
@click.option('--diff-only', is_flag=True)
def diff(dataset1, dataset2, git, checkout_path, diff_only):
    """give a report on the statistical differences for datapoints between 2 datasets."""
    import ddf_utils.ddf_reader as dr
    from ddf_utils.qa import compare_with_func
    import tabulate
    from os.path import join

    if git:
        from subprocess import check_output
        assert dr.is_dataset('./')

        c1 = check_output(['git', 'rev-parse', dataset1])
        p1 = c1.strip().decode('utf8')

        c2 = check_output(['git', 'rev-parse', dataset2])
        p2 = c2.strip().decode('utf8')

        try:
            os.makedirs(join(checkout_path, p1))
            logging.info('checkout git rev {} into {}'.format(dataset1, join(checkout_path, p1)))
            os.system('git --work-tree={} checkout {} -- .'.format(join(checkout_path, p1), p1))
        except FileExistsError:
            pass

        try:
            os.makedirs(join(checkout_path, p2))
            logging.info('checkout git rev {} into {}'.format(dataset2, join(checkout_path, p2)))
            os.system('git --work-tree={} checkout {} -- .'.format(join(checkout_path, p2), p2))
        except FileExistsError:
            pass

        dr.config.DDF_SEARCH_PATH = checkout_path

        d1 = dr.DDF(p1)
        d2 = dr.DDF(p2)

    else:
        d1 = dr.DDF(dataset1)
        d2 = dr.DDF(dataset2)

    result = compare_with_func(d1, d2)
    if diff_only:
        result = result[result.rval != 1]

    cols = result.columns

    # sort it
    result = result.sort_values(by='rval', ascending=False).set_index('indicator')

    click.echo(tabulate.tabulate(result,
                                 headers=cols, tablefmt='psql'))


# csv to ddfcsv
@ddf.command()
@click.option('-i', 'input', type=click.Path(exists=True))
@click.option('-o', 'out_path', type=click.Path(exists=True))
def from_csv(input, out_path):
    """create ddfcsv dataset from a set of csv files"""
    from .io import csvs_to_ddf

    if os.path.isfile(input):
        files = [input]
    else:
        files = [os.path.join(input, x)
                 for x in os.listdir(input) if x.endswith('.csv')]

    csvs_to_ddf(files, out_path)

    click.echo('Done.')


if __name__ == '__main__':
    ddf()
