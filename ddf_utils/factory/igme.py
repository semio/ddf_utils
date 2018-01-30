# -*- coding: utf-8 -*-

"""download sources from CME info portal"""

__doc__ = """T.B.D"""

import os.path as osp

import re
import requests
import pandas as pd

from lxml import html
from urllib.parse import urlsplit, urljoin


url = 'http://www.childmortality.org/'
metadata = None

def load_metadata():
    r = requests.get(url)
    h = html.fromstring(r.content)

    flist = []

    for l in h.xpath('//a/@href'):
        if l.endswith('xlsx'):
            #print(urljoin(url, l))
            flist.append(urljoin(url, l))

    md = pd.DataFrame(flist, columns=['link'])
    md['name'] = md['link'].map(lambda x: osp.basename(x)[:-5])

    global metadata
    metadata = md[['name', 'link']].copy()


def has_newer_source(v):
    """accepts a int and return true if version inferred from metadata is bigger."""
    if metadata is None:
        load_metadata()
    link = metadata.loc[0, 'link']

    ver = re.match('.*files_v(\d+).*', link).groups()[0]

    if int(ver) > v:
        return True
    return False


def bulk_download(out_dir, name=None):
    if metadata is None:
        load_metadata()

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
