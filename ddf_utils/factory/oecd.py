# -*- coding: utf-8 -*-

"""Functions for scraping OECD website
"""

from io import BytesIO
from pathlib import Path
import json

import requests
from lxml import etree
import pandas as pd

__doc__ = """T.B.D"""


# TODO: there is 1 000 000 row limitation. handle it

data_url_tmpl = 'http://stats.oecd.org/SDMX-JSON/data/{dataset}/all/all'
# dataflow_url_tmpl = 'http://stats.oecd.org/SDMX-JSON/dataflow/{dataset}/all/all'
datastructure_url_tmpl = 'http://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/{dataset}'
metadata_url = 'http://stats.oecd.org/RestSDMX/sdmx.ashx/GetKeyFamily/all'

metadata = None


def load_metadata():
    res = requests.get(metadata_url)
    parser = etree.XMLParser(ns_clean=True)  # TODO: why ns_clean can't clean namespace?
    tree = etree.parse(BytesIO(res.content), parser)
    root = tree.getroot()

    datasets = []

    for e in root.findall('.//KeyFamily', namespaces=root.nsmap):
        dataset_id = (e.attrib['id'])
        dataset_name = None
        for e_ in e.findall('.//Name', namespaces=root.nsmap):
            if 'en' in e_.attrib.values():
                dataset_name = e_.text

        datasets.append({'id': dataset_id, 'name': dataset_name})

    global metadata
    metadata = pd.DataFrame.from_records(datasets)

    return metadata


def has_newer_source(dataset, version):
    # There is no version info in the response from OECD server.
    return NotImplementedError


def bulk_download(out_dir, dataset):
    """download the full json, including observation/dimension lists."""
    p = Path(out_dir, dataset+'.json')
    url = data_url_tmpl.format(dataset=dataset)

    res = requests.get(url)
    assert res.status_code == 200, 'error: {}'.format(res.status_code)

    with p.expanduser().open('wb') as f:
        f.write(res.content)
        f.close()
