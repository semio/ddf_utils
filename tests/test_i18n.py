# -*- coding: utf-8 -*-

import os
import shutil


test_dataset_path = os.path.join(os.path.dirname(__file__),
                                 'chef/datasets/ddf--gapminder--dummy_companies')


def test_split_translation():
    from ddf_utils.i18n import split_translations_csv, split_translations_json
    dataset_path = test_dataset_path
    split_path = 'langsplit_'

    csv_path = os.path.join(dataset_path, split_path+'csv')
    json_path = os.path.join(dataset_path, split_path+'json')

    if os.path.exists(csv_path):
        shutil.rmtree(csv_path)
    if os.path.exists(json_path):
        shutil.rmtree(json_path)

    split_translations_json(dataset_path, split_path=split_path+'json')
    split_translations_csv(dataset_path, split_path=split_path+'csv')

    assert os.path.isdir(os.path.join(dataset_path, split_path+'json'))
    assert os.path.isdir(os.path.join(dataset_path, split_path+'csv'))

    assert os.path.exists(os.path.join(dataset_path, split_path+'csv', 'en',
                                       'ddf--entities--company.csv', 'country.csv'))
    assert os.path.exists(os.path.join(dataset_path, split_path+'json', 'en',
                                       'ddf--entities--company.json'))


def test_merge_translation():
    from ddf_utils.i18n import merge_translations_csv, merge_translations_json

    dataset_path = test_dataset_path
    split_path = 'langsplit'

    if os.path.exists(os.path.join(dataset_path, 'lang')):
        shutil.rmtree(os.path.join(dataset_path, 'lang'))

    merge_translations_csv(dataset_path, split_path=split_path)

    assert os.path.isdir(os.path.join(dataset_path, 'lang', 'zh_CN'))

    split_path = 'langsplit_2'

    if os.path.exists(os.path.join(dataset_path, 'lang')):
        shutil.rmtree(os.path.join(dataset_path, 'lang'))

    merge_translations_json(dataset_path, split_path=split_path)

    assert os.path.isdir(os.path.join(dataset_path, 'lang', 'zh_CN'))
