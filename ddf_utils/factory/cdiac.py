# -*- coding: utf-8 -*-

"""Functions to load data from cdiac co2 website

source link: The `CDIAC ftp site`_

.. _`CDIAC ftp site`: http://cdiac.ess-dive.lbl.gov/ftp/ndp030/CSV-FILES/
"""

import os.path as osp

import requests
import pandas as pd

from urllib.parse import urljoin

from . common import DataFactory


class CDIACLoader(DataFactory):

    url = 'http://cdiac.ess-dive.lbl.gov/ftp/ndp030/CSV-FILES/'

    def load_metadata(self):
        """load the file info listed in the CDIAC website."""
        r = requests.get(self.url)
        assert r.status_code == 200, 'load cdiac website failed!'

        data = pd.read_html(r.content)[0]

        data = data.dropna(axis=1, how='all').dropna(axis=0, how='all')
        data = data.iloc[2:]

        data['Last modified'] = pd.to_datetime(data['Last modified'])

        self.metadata = data
        return data

    def has_newer_source(self, date):
        if self.metadata is None:
            self.load_metadata()
        metadata = self.metadata
        newer = metadata[metadata['Last modified'] > date].values
        if len(newer) > 0:
            return True
        return False

    def bulk_download(self, out_dir):
        """download the latest nation/global file"""
        if self.metadata is None:
            self.load_metadata()
        metadata = self.metadata

        # latest nation/global file names
        lm = 'Last modified'
        nation_fn = (metadata[metadata['Name'].str.startswith('nation')]
                     .sort_values(by=[lm, 'Name'], ascending=False)['Name']
                     .values[0])
        global_fn = (metadata[metadata['Name'].str.startswith('global')]
                     .sort_values(by=[lm, 'Name'], ascending=False)['Name']
                     .values[0])

        # download files
        nation_url = urljoin(self.url, nation_fn)
        nation_fs_path = osp.join(out_dir, 'nation.csv')

        with open(osp.expanduser(nation_fs_path), 'wb') as f:
            r = requests.get(nation_url)
            f.write(r.content)
            f.close()

        global_url = urljoin(self.url, global_fn)
        global_fs_path = osp.join(out_dir, 'global.csv')

        with open(osp.expanduser(global_fs_path), 'wb') as f:
            r = requests.get(global_url)
            f.write(r.content)
            f.close()

        return out_dir
