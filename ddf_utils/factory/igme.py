# -*- coding: utf-8 -*-

"""download sources from CME info portal

source link: `CME data portal`_

.. _`CME data portal`: http://www.childmortality.org

Note: This factory class is no longer works as CME website
switched to a javascript app.

"""


import os.path as osp

import re
import requests
import pandas as pd

from lxml import html
from urllib.parse import urlsplit, urljoin

from . common import DataFactory


class IGMELoader(DataFactory):

    url = 'https://www.childmortality.org/'

    def load_metadata(self):
        r = requests.get(self.url)
        h = html.fromstring(r.content)

        flist = []

        for l in h.xpath('//a/@href'):
            if l.endswith('xlsx'):
                # print(urljoin(url, l))
                flist.append(urljoin(self.url, l))

        md = pd.DataFrame(flist, columns=['link'])
        md['name'] = md['link'].map(lambda x: osp.basename(x)[:-5])

        metadata = md[['name', 'link']].copy()
        self.metadata = metadata
        return metadata

    def has_newer_source(self, v):
        """accepts a int and return true if version inferred from metadata is bigger."""
        if self.metadata is None:
            self.load_metadata()
        metadata = self.metadata
        link = metadata.loc[0, 'link']

        ver = re.match(r'.*files_v(\d+).*', link).groups()[0]

        if int(ver) > v:
            return True
        return False

    def bulk_download(self, out_dir, name=None):
        if self.metadata is None:
            self.load_metadata()
        metadata = self.metadata

        if name:
            names = [name]
        else:
            names = metadata['name'].values

        for n in names:
            if n not in metadata['name'].values:
                raise KeyError("{} not found in page.".format(n))

            link = metadata.loc[metadata['name'] == n, 'link'].values[0]
            res = requests.get(link)
            out_path = osp.join(out_dir, osp.basename(link))

            with open(osp.expanduser(out_path), 'wb') as f:
                f.write(res.content)
                f.close()
