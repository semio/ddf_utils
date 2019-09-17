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
    main_url = 'http://www.ilo.org/ilostat-files/WEB_bulk_download/'
    indicator_meta_url_tmpl = urljoin(main_url, 'indicator/table_of_contents_{lang}.csv')
    other_meta_url_tmpl = urljoin(main_url, 'dic/{table}_{lang}.csv')

    def load_metadata(self, table='indicator', lang='en'):
        """get code list for a specified table and language.

        Check ILO doc for all available tables and languages.
        """
        if table == 'indicator':
            tmpl = self.indicator_meta_url_tmpl
        else:
            tmpl = self.other_meta_url_tmpl

        url = tmpl.format(table=table, lang=lang)

        metadata = {}
        metadata[table] = pd.read_csv(url)

        self.metadata = metadata[table]
        return self.metadata

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
        url = urljoin(self.main_url, f'indicator/{i}.csv.gz')
        res = requests_retry_session().get(url, stream=True, timeout=60)
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
