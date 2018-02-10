# -*- coding: utf-8 -*-

"""Functions to load data from cdiac co2 website
"""

import os.path as osp

import requests
import pandas as pd

from urllib.parse import urljoin


url = 'http://cdiac.ess-dive.lbl.gov/ftp/ndp030/CSV-FILES/'
metadata = None


def load_metadata():
    r = requests.get(url)
    assert r.status_code == 200, 'load cdiac website failed!'

    data = pd.read_html(r.content)[0]

    data = data.dropna(axis=1, how='all')
    data.columns = data.iloc[0]
    data = data.iloc[2:]

    data['Last modified'] = pd.to_datetime(data['Last modified'])

    global metadata
    metadata = data

    return data


def has_newer_source(date):
    if metadata is None:
        load_metadata()
    newer = metadata[metadata['Last modified'] > date].values
    if len(newer) > 0:
        return True
    return False


def bulk_download(out_dir):
    """download the latest nation/global file"""
    if metadata is None:
        load_metadata()

    # latest nation/global file names
    lm = 'Last modified'
    nation_fn = (metadata[metadata['Name'].str.startswith('nation')]
                 .sort_values(by=[lm, 'Name'], ascending=False)['Name']
                 .values[0])
    global_fn = (metadata[metadata['Name'].str.startswith('global')]
                 .sort_values(by=[lm, 'Name'], ascending=False)['Name']
                 .values[0])

    # download files
    nation_url = urljoin(url, nation_fn)
    nation_fs_path = osp.join(out_dir, 'nation.csv')

    with open(osp.expanduser(nation_fs_path), 'wb') as f:
        r = requests.get(nation_url)
        f.write(r.content)
        f.close()

    global_url = urljoin(url, global_fn)
    global_fs_path = osp.join(out_dir, 'global.csv')

    with open(osp.expanduser(global_fs_path), 'wb') as f:
        r = requests.get(global_url)
        f.write(r.content)
        f.close()

    return out_dir
