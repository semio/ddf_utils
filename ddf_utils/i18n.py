# -*- coding: utf-8 -*-
"""i18n project management for Gapminder's datasets.

The workflow is described in this `google doc`_

.. _google doc: https://docs.google.com/document/d/11d5D5CPlr6I2BqP8z0p2o_dYQC9AYUyxcLGUq1WYCfk/
"""

import os
import json
import numpy as np
import pandas as pd
from . datapackage import get_datapackage


def split_translations_json(path, split_path='langsplit', exclude_concepts=None, overwrite=False):
    """split all string concepts and save them as json files"""
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
        exclude_concepts = ['concept_type', 'domain']

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
        df = df.set_index(key)
        for c in list(df.columns):
            if c in exclude_concepts:
                df = df.drop(c, axis=1)
                continue
            if c.startswith('is--'):  # it will be boolean, skip
                df = df.drop(c, axis=1)
                continue
            try:
                if not concepts.loc[c, 'concept_type'] == 'string':
                    df = df.drop(c, axis=1)
                    continue
            except KeyError:
                print('concept not found in ddf--concepts: ' + c)
                df = df.drop(c, axis=1)
                continue
        # save file to disk
        if not np.all(pd.isnull(df)):
            json_path = os.path.join(basepath, res['name']+'.json')
            dir_path = os.path.dirname(json_path)
            if os.path.exists(json_path) and not overwrite:
                print('file exists: ' + json_path)
                continue
            os.makedirs(dir_path, exist_ok=True)
            # print(json_path)
            df = df.fillna('')
            # FIXME: handle multiIndex issue
            # see https://github.com/pandas-dev/pandas/issues/4889
            df.to_json(json_path, orient='index', force_ascii=False)
    return


def split_translations_csv(path, split_path='langsplit', exclude_concepts=None, overwrite=False):
    """split all string concepts and save them as csv files"""
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
        exclude_concepts = ['concept', 'concept_type', 'domain']

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
            if c in exclude_concepts:
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
                    df.set_index(key)[[c]].to_csv(split_csv_path, encoding='utf8')
            except KeyError:
                print('concept not found in ddf--concepts: ' + c)
                continue
    return


def merge_translations_csv(path, split_path='langsplit', lang_path='lang', overwrite=False):
    """merge all translated csv files and update datapackage.json"""
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
            csvfiles = [x for x in files if 'csv' in x]
            if len(csvfiles) > 0:  # there are csv files.
                p = root.replace(basepath, '')

                os.makedirs(os.path.join(target_lang_path, os.path.dirname(p)), exist_ok=True)

                target_file_path = os.path.join(target_lang_path, p)
                to_concat = [pd.read_csv(os.path.join(root, x), index_col=0) for x in csvfiles]
                df_new = pd.concat(to_concat, axis=1)
                if os.path.exists(target_file_path):
                    df_old = pd.read_csv(target_file_path, index_col=0)
                    df_old.update(df_new)
                    df_new = df_old.copy()
                df_new.to_csv(target_file_path, encoding='utf8')
    # update datapackage if there are new translations
    if new_translations:
        with open(os.path.join(path, 'datapackage.json'), 'w') as f:
            json.dump(datapackage, f, indent=4, ensure_ascii=False)
    return


def merge_translations_json(path, split_path='langsplit', lang_path='lang', overwrite=False):
    """merge all translated json files and update datapackage.json"""
    # make the paths full paths
    split_path = os.path.join(path, split_path)
    lang_path = os.path.join(path, lang_path)

    datapackage = get_datapackage(path)
    source_lang = datapackage['language']['id']

    # because the primaryKey is not in the splited files, we generate
    # a mapping for later use.
    key_mapping = dict([(x['name'], x['schema']['primaryKey']) for x in datapackage['resources']])

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
            jsonfiles = [x for x in files if 'json' in x]
            if len(jsonfiles) > 0:
                p = root.replace(basepath, '')
                for fn in jsonfiles:
                    name = os.path.splitext(fn)[0]
                    target_file_path = os.path.join(target_lang_path, p, fn).replace('.json', '.csv')
                    if not os.path.exists(os.path.join(target_lang_path, p)):
                        os.makedirs(os.path.join(target_lang_path, p))
                    df = pd.read_json(os.path.join(root, fn), orient='index')
                    # add back the index name.
                    # the index name will be missing because it's not in the json file.
                    df.index.name = key_mapping[name]
                    if os.path.exists(target_file_path) and not overwrite:
                        df_old = pd.read_csv(target_file_path, index_col=0)
                        df_old.update(df)
                        df = df_old.copy()
                    df.to_csv(target_file_path, encoding='utf8')
    # update datapackage if there are new translations
    if new_translations:
        with open(os.path.join(path, 'datapackage.json'), 'w') as f:
            json.dump(datapackage, f, indent=4, ensure_ascii=False)
    return
