# -*- coding: utf-8 -*-
"""io functions for ddf files"""

import os
import shutil
import threading
import time
from urllib.parse import urlsplit

import pandas as pd
import requests


def to_csv(df, out_dir, ftype, concept, by=None, **kwargs):
    """save a ddf dataframe to csv file.
    the file path of csv will be out_dir/ddf--$ftype--$concept--$by.csv
    """

    if not by:
        path = os.path.join(out_dir, 'ddf--'+ftype+'--'+concept+'.csv')
    else:
        if isinstance(by, list):
            filename = 'dff--' + '--'.join([ftype, concept]) + '--'.join(by) + '.csv'
        else:
            filename = 'dff--' + '--'.join([ftype, concept, by]) + '.csv'

        path = os.path.join(out_dir, filename)

    df.to_csv(path, **kwargs)


def load_google_xls(filehash):
    # TODO: return the xls file with given filehash
    raise NotImplementedError


def cleanup(path, how='ddf'):
    """remove all ddf files in the given path"""
    # TODO: support names don't have standard ddf name format.
    if how == 'ddf':
        for f in os.listdir(path):
            if f.startswith("ddf--"):
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
        r = requests.get(url_, stream=True)
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
    from ddf_utils.datapackage import get_datapackage, dump_json

    concepts_df = pd.DataFrame([['name', 'Name', 'string']],
                               columns=['concept', 'name', 'concept_type'])
    concepts_df = concepts_df.set_index('concept')

    all_entities = dict()

    pattern = 'indicators--by--([ 0-9a-zA-Z_-]*).csv'

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

            concepts_df.ix[concept] = [col, t]

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
