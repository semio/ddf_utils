#-*- coding: utf-8 -*-
"""i18n project management for Gapminder's datasets.
workflow: https://docs.google.com/document/d/11d5D5CPlr6I2BqP8z0p2o_dYQC9AYUyxcLGUq1WYCfk/
'"""

import pandas as pd
from . index import get_datapackage
import json
import os


def split_translations(path, split_path='langsplit', exclude_concepts=None, overwrite=False):
    datapackage = get_datapackage(path)
    split_path = os.path.join(path, split_path)

    try:
        lang = datapackage['language']['id']
    except KeyError:
        print('no language tag found, assuming en')
        lang = 'en'

    basepath = os.path.join(split_path, lang)
    os.makedirs(basepath, exist_ok=True)

    # exclude some columns for translation by default
    if not exclude_concepts:
        excluded_cols = ['concept', 'concept_type', 'domain']

    concepts = list()
    for res in datapackage['resources']:
        file_path = res['path']
        key = res['schema']['primaryKey']
        if key == 'concept':
            concepts.append(pd.read_csv(os.path.join(path, file_path)))
    concepts = pd.concat(concepts, ignore_index=True).set_index('concept')

    for res in datapackage['resources']:
        file_path = res['path']
        key = res['schema']['primaryKey']

        df = pd.read_csv(os.path.join(path, file_path))
        for c in df.columns:
            if c in excluded_cols:
                continue
            if c.startswith('is--'):  # it will be boolean, skip
                continue
            try:
                if concepts.loc[c, 'concept_type'] == 'string':
                    os.makedirs(os.path.join(basepath, file_path), exist_ok=True)
                    split_csv_path = os.path.join(basepath, file_path, '{}.csv'.format(c))
                    if os.path.exists(split_csv_path) and not overwrite:
                        print('file exists: ' + split_csv_path)
                        continue
                    df.set_index(key)[[c]].to_csv(split_csv_path)
            except KeyError:
                print('concept not found in ddf--concepts: ' + c)
                continue
    return


def merge_translations(path, split_path='langsplit', lang_path='lang', overwrite=False):

    if overwrite:
        # TODO: overwrite existing translation instead of update
        raise NotImplementedError

    # make the paths full paths
    split_path = os.path.join(path, split_path)
    lang_path = os.path.join(path, lang_path)

    datapackage = get_datapackage(path)
    source_lang = datapackage['language']['id']
    # get all available translations, update it when necessary below.
    if 'translations' not in datapackage.keys():
        datapackage['translations'] = []
    available_translations = [x['id'] for x in datapackage['translations']]
    new_translations = False

    assert os.path.exists(split_path)

    langs = next(os.walk(split_path))[1]  # all sub folders in split path.

    for lang in langs:
        if lang == source_lang:
            continue

        if lang not in available_translations:
            datapackage['translations'].append({'id': lang})
            new_translations = True

        basepath = os.path.join(split_path, lang) + '/'
        target_lang_path = os.path.join(lang_path, lang)
        os.makedirs(target_lang_path, exist_ok=True)

        for root, dirs, files in os.walk(basepath):
            csvfiles = [x for x in files if 'csv' in x]  # there are csv files.
            if len(csvfiles) > 0:
                p = root.replace(basepath, '')

                os.makedirs(os.path.join(target_lang_path, os.path.dirname(p)), exist_ok=True)

                target_file_path = os.path.join(target_lang_path, p)
                to_concat = [pd.read_csv(os.path.join(root, x), index_col=0) for x in csvfiles]
                df_new = pd.concat(to_concat, axis=1)
                if os.path.exists(target_file_path):
                    df_old = pd.read_csv(target_file_path, index_col=0)
                    df_old.update(df_new)
                    df_new = df_old.copy()
                df_new.to_csv(target_file_path)
    # update datapackage if there are new translations
    if new_translations:
        with open(os.path.join(path, 'datapackage.json'), 'w') as f:
            json.dump(datapackage, f, indent=4)
    return

