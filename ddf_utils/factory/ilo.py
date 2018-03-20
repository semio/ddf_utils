# -*- coding: utf-8 -*-

"""Functions for scraping ILO datasets

using the bulk downloader, see `its doc`_.

.. _its doc: http://www.ilo.org/ilostat-files/WEB_bulk_download/ILOSTAT_BulkDownload_Guidelines.pdf
"""

from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests

__doc__ = """T.B.D"""

main_url = 'http://www.ilo.org/ilostat-files/WEB_bulk_download/'
indicator_meta_url_tmpl = urljoin(main_url, 'indicator/table_of_contents_{lang}.csv')
other_meta_url_tmpl = urljoin(main_url, 'dic/{table}_{lang}.csv')

metadata = None


def load_metadata(table='indicator', lang='en'):
    if table == 'indicator':
        tmpl = indicator_meta_url_tmpl
    else:
        tmpl = other_meta_url_tmpl

    url = tmpl.format(table=table, lang=lang)

    global metadata
    metadata = {}
    metadata[table] = pd.read_csv(url)

    return metadata[table]


def has_newer_source(indicator, date):
    if not metadata:
        md = load_metadata()
    else:
        try:
            md = metadata['indicator']
        except KeyError:
            md = load_metadata()

    last_update = md.loc[md.id == 'indicator', 'last.update']
    if last_update > pd.to_datetime(date):
        return True
    return False


def bulk_download(out_dir, indicators:list):
    for i in indicators:
        url = urljoin(main_url, f'indicator/{i}.csv.gz')
        res = requests.get(url)
        assert res.status_code == 200, f'can not download source file: {url}'

        with Path(out_dir, f'{i}.csv.gz').expanduser().open('wb') as f:
            f.write(res.content)
            f.close()
