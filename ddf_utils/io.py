# -*- coding: utf-8 -*-
"""io functions for ddf files"""

import os
import shutil
import json
import threading
import time
import typing
from urllib.parse import urlsplit

from io import BytesIO
import pandas as pd
import requests as req

from ddf_utils.str import format_float_digits
from ddf_utils.package import get_datapackage


# helper for dumping datapackage json
def dump_json(path, obj):
    """convenient function to dump a dictionary object to json"""
    with open(path, 'w+') as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)
        f.close()


# TODO: integrate with Ingredient.serve
def serve_datapoint(df_: pd.DataFrame, out_dir, concept, copy=True,
                    by: typing.Iterable = None,
                    formatter: typing.Callable = format_float_digits, **kwargs):
    """save a pandas dataframe to datapoint file.
    the file path of csv will be out_dir/ddf--datapoints--$concept--$by.csv

    addition keyword arguments can be passed to `pd.DataFrame.to_csv()` function.
    """
    if copy:
        df = df_.copy()
    else:
        df = df_
    # formatting the concept column
    if formatter is not None:
        df[concept] = df[concept].map(formatter)

    if by is None:
        by = df.index.names
    by = '--'.join(by)

    path = os.path.join(out_dir, 'ddf--datapoints--{}--by--{}.csv'.format(concept, by))
    df.to_csv(path, **kwargs)


def serve_concept():
    pass


def serve_entity():
    pass


def open_google_spreadsheet(docid):
    """read google spreadsheet into excel io object"""
    tmpl_xls = "https://docs.google.com/spreadsheets/d/{docid}/export?format=xlsx&id={docid}"
    url = tmpl_xls.format(docid=docid)
    res = req.get(url)
    if res.ok:
        return BytesIO(res.content)
    return None


def cleanup(path, how='ddf', exclude=None, use_default_exclude=True):
    """remove all ddf files in the given path"""
    default_exclude = ['etl', 'lang', 'langsplit', 'datapackage.json', 'README.md', 'assets']
    if exclude and not isinstance(exclude, list):
        if isinstance(exclude, tuple):
            exclude = list(exclude)  # this is not working for str. and [exclude] not working for tuple
        else:
            exclude = [exclude]
    if use_default_exclude:
        if exclude:
            for e in default_exclude:
                exclude.append(e)
        else:
            exclude = default_exclude

    if how == 'ddf':
        for f in os.listdir(path):
            # only keep dot files and etl/ lang/ langsplit/ and datapackage.json
            if f not in exclude and not f.startswith('.'):
                p = os.path.join(path, f)
                if os.path.isdir(p):
                    shutil.rmtree(p)
                else:
                    os.remove(p)
        # TODO: think a best way to handle metadata in datapackage.json
        # if os.path.exists(os.path.join(path, 'datapackage.json')):
        #     os.remove(os.path.join(path, 'datapackage.json'))
    if how == 'lang':
        if os.path.exists(os.path.join(path, 'lang')):
            shutil.rmtree(os.path.join(path, 'lang'))
    if how == 'langsplit':
        if os.path.exists(os.path.join(path, 'langsplit')):
            shutil.rmtree(os.path.join(path, 'langsplit'))


def download_csv(urls, out_path):
    """download csv files"""

    def download(url_, out_path_):
        r = req.get(url_, stream=True)
        total_length = int(r.headers.get('content-length'))
        if total_length == 0:
            return
        fn = urlsplit(url_).path.split('/')[-1]
        print('writing to: {}\n'.format(fn), end='')
        with open(os.path.join(out_path, fn), 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)

    def create_tread(url_, out_path_):
        download_thread = threading.Thread(target=download, args=(url_, out_path_))
        download_thread.start()
        return download_thread

    threads = []
    for url in urls:
        threads.append(create_tread(url, out_path))

    # wait until all downloads are done
    is_alive = [t.is_alive() for t in threads]
    while any(is_alive):
        time.sleep(1)
        is_alive = [t.is_alive() for t in threads]


def csvs_to_ddf(files, out_path):
    """convert raw files to ddfcsv

    Args
    ----
    files: list
        a list of file paths to build ddf csv
    out_path: `str`
        the directory to put the ddf dataset

    """
    import re
    from os.path import join
    from ddf_utils.str import to_concept_id

    concepts_df = pd.DataFrame([['name', 'Name', 'string']],
                               columns=['concept', 'name', 'concept_type'])
    concepts_df = concepts_df.set_index('concept')

    all_entities = dict()

    pattern = r'indicators--by--([ 0-9a-zA-Z_-]*).csv'

    for f in files:
        data = pd.read_csv(f)
        basename = os.path.basename(f)
        keys = re.match(pattern, basename).groups()[0].split('--')
        keys_alphanum = list(map(to_concept_id, keys))

        # check if there is a time column. Assume last column is time.
        try:
            pd.to_datetime(data[keys[-1]], format='%Y')
        except (ValueError, pd.tslib.OutOfBoundsDatetime):
            has_time = False
        else:
            has_time = True

        if has_time:
            ent_keys = keys[:-1]
        else:
            ent_keys = keys

        # set concept type
        for col in data.columns:
            concept = to_concept_id(col)

            if col in keys:
                if col in ent_keys:
                    t = 'entity_domain'
                else:
                    t = 'time'
            else:
                t = 'measure'

            concepts_df.loc[concept] = [col, t]

        for ent in ent_keys:
            ent_df = data[[ent]].drop_duplicates().copy()
            ent_concept = to_concept_id(ent)
            ent_df.columns = ['name']
            ent_df[ent_concept] = ent_df.name.map(to_concept_id)

            if ent_concept not in all_entities.keys():
                all_entities[ent_concept] = ent_df
            else:
                all_entities[ent_concept] = pd.concat([all_entities[ent_concept], ent_df],
                                                      ignore_index=True)

        data = data.set_index(keys)
        for c in data:
            # output datapoints
            df = data[c].copy()
            df = df.reset_index()
            for k in keys[:-1]:
                df[k] = df[k].map(to_concept_id)
            df.columns = df.columns.map(to_concept_id)
            (df.dropna()
               .to_csv(join(out_path,
                            'ddf--datapoints--{}--by--{}.csv'.format(
                                to_concept_id(c), '--'.join(keys_alphanum))),
                       index=False))

    # output concepts
    concepts_df.to_csv(join(out_path, 'ddf--concepts.csv'))

    # output entities
    for c, df in all_entities.items():
        df.to_csv(join(out_path, 'ddf--entities--{}.csv'.format(c)), index=False)

    dp = get_datapackage(out_path, use_existing=False)
    dump_json(os.path.join(out_path, 'datapackage.json'), dp)

    return
