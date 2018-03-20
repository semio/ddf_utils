# -*- coding: utf-8 -*-

"""functions for scraping data from clio infra website."""

import os.path as osp
import pandas as pd
from lxml import etree
import requests
from urllib.parse import urljoin


__doc__ = """T.B.D"""

url = 'https://www.clio-infra.eu/index.html'
metadata = None


def _get_home_page(url):
    response = requests.get(url)
    content = response.content
    tree = etree.fromstring(content, parser=etree.HTMLParser())

    return tree


def has_newer_source(ver):
    print('there is no version info in this site.')
    raise NotImplementedError


def load_metadata():
    tree = _get_home_page(url)
    elem = tree.xpath('//div[@class="col-sm-4"]/div[@class="list-group"]/p[@class="list-group-item"]')

    res1 = {}
    res2 = {}

    for e in elem:
        try:
            name = e.find('a').text
            link = e.find('*/a').attrib['href']
            if '../data' in link:  # it's indicator file
                res1[name] = link
            else:  # it's country file
                res2[name] = link
        except:  # FIXME: add exception class here.
            name = e.text
            res2[name] = ''

    # create the metadata dataframe
    md_dataset = pd.DataFrame(columns=['name', 'url', 'type'])
    md_dataset['name'] = list(res1.keys())
    md_dataset['url'] = list(res1.values())
    md_dataset['type'] = 'dataset'
    md_country = pd.DataFrame(columns=['name', 'url', 'type'])
    md_country['name'] = list(res2.keys())
    md_country['url'] = list(res2.values())
    md_country['type'] = 'country'

    global metadata
    metadata = pd.concat([md_dataset, md_country], ignore_index=True)

    return metadata


def bulk_download(out_dir, data_type=None):

    if metadata is None:
        load_metadata()

    if data_type:
        to_download = metadata[metadata['type'] == data_type]
    else:
        to_download = metadata

    for i, row in to_download.iterrows():

        name = row['name']
        path = row['url']

        res = requests.get(urljoin(url, path), stream=True)
        fn = osp.join(out_dir, f'{name}.xls')
        print(fn, end=', ')
        with open(fn, 'wb') as f:
            for chunk in res.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
            f.close()
    print('Done downloading source files.')
