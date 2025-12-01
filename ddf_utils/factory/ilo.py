# -*- coding: utf-8 -*-

"""Functions for scraping ILO datasets

using the bulk downloader, see `its doc`_.

.. _its doc: http://www.ilo.org/ilostat-files/WEB_bulk_download/ILOSTAT_BulkDownload_Guidelines.pdf
"""

from . common import requests_retry_session, DataFactory

from pathlib import Path
from urllib.parse import urljoin
from multiprocessing import Pool
from functools import partial

import pandas as pd


class ILOLoader(DataFactory):
    main_url = 'https://rplumber.ilo.org/'
    indicator_meta_url_tmpl = urljoin(main_url, 'metadata/toc/indicator?lang={lang}&format=.csv')
    other_meta_url_tmpl = urljoin(main_url, 'metadata/dic?var={table}&lang={lang}&format=.csv')

    def __init__(self):
        self.metadata = dict()

    def load_metadata(self, table='indicator', lang='en'):
        """get code list for a specified table and language.

        Check ILO doc for all available tables and languages.
        """
        key = f"{table}-{lang}"
        if key in self.metadata:
            return self.metadata[key]

        if table == 'indicator':
            tmpl = self.indicator_meta_url_tmpl
        else:
            tmpl = self.other_meta_url_tmpl

        url = tmpl.format(table=table, lang=lang)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        session = requests_retry_session()
        session.headers.update(headers)
        data = pd.read_csv(url, storage_options={'User-Agent': headers['User-Agent']})

        self.metadata[key] = data
        return data

    def has_newer_source(self, indicator, date):
        """check if an indicator's last modified date is newer than given date.
        """
        if self.metadata is None:
            self.load_metadata()
        md = self.metadata
        last_update = md.loc[md.id == indicator, 'last.update']
        assert len(last_update) == 1
        last_update = last_update.values[0]
        if pd.to_datetime(last_update) > pd.to_datetime(date):
            return True
        return False

    def download(self, i, out_dir):
        """Download an indicator to out_dir.
        """
        url = urljoin(self.main_url, f'data/indicator?id={i}&type=code&format=.csv.gz')
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        session = requests_retry_session()
        session.headers.update(headers)
        res = session.get(url, stream=True, timeout=60)
        if res.status_code != 200:
            print(f'can not download source file: {url}')
            return

        with Path(out_dir, f'{i}.csv.gz').expanduser().open('wb') as f:
            for chunk in res.iter_content(chunk_size=1024):
                f.write(chunk)
                f.flush()

    def bulk_download(self, out_dir, indicators: list, pool_size=5):
        """Download a list of indicators simultaneously.
        """
        download_ = partial(self.download, out_dir=out_dir)

        with Pool(pool_size) as p:
            p.map(download_, indicators)
