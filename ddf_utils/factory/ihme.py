# -*- coding: utf-8 -*-

"""Functions for IHME

The `GBD result tool`_ at IHME contains all data for GBD results, but
they don't have an open API to query the data. However the website
uses a json endpoint and it doesn't need authorization. So we also
make use of it.

.. _`GBD result tool`: http://ghdx.healthdata.org/gbd-results-tool

To see how to use this downloader, please check the scripts at
open-numbers' `IHME dataset`_

.. _`IHME dataset`: https://github.com/open-numbers/ddf--ihme--death_cause_compact/blob/0e0951eb6a41c02a3f3928849ace7fb56e968c38/etl/scripts/update_source.py

"""

import os
import os.path as osp
import math
from time import sleep
import requests
import pandas as pd

from tqdm import tqdm

from ddf_utils.chef.helpers import read_opt
from . common import requests_retry_session, DataFactory, download


# TODO: add missing context/base configures.
class IHMELoader(DataFactory):
    url_hir = 'https://vizhub.healthdata.org/gbd-results/php/hierarchy/'
    url_metadata = 'https://vizhub.healthdata.org/gbd-results/php/metadata/'
    url_version = 'https://vizhub.healthdata.org/gbd-results/php/version/'
    # url for query data:
    # http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/data.php
    # below is url for download data as zip
    url_data = 'https://vizhub.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/download.php'
    url_task = 'https://s3.healthdata.org/gbd-api-2019-public/{hash}'  # access to download link

    def load_metadata(self):
        """load all codes used in GBD in a dictionary."""

        session = requests_retry_session()
        meta = session.get(self.url_metadata).json()
        versions = session.get(self.url_version).json()

        metadata = {}

        for k, v in meta['data'].items():
            # if k == 'location':  # locations metadata id messed up, need to reset
            #     loc = pd.DataFrame.from_dict(meta['data'][k], orient='index').T
            #     loc['id'] = loc.index
            #     loc = loc.drop('location_id', axis=1)
            #     metadata[k] = loc
            # else:
            #     metadata[k] = pd.DataFrame.from_dict(meta['data'][k], orient='index')
            if v:
                metadata[k] = pd.DataFrame.from_dict(v, orient='index')
            else:
                metadata[k] = None

        metadata['version'] = pd.DataFrame.from_dict(versions['data'], orient='index')
        self.metadata = metadata
        return metadata

    def has_newer_source(self, ver):
        if not self.metadata:
            self.load_metadata()
        metadata = self.metadata
        versions = metadata['version']
        newer = versions[versions['id'] > ver].values
        return bool(len(newer) > 0)

    def download_links(self, url):
        session = requests_retry_session()

        sent_urls = []

        success_results = [
            'Your search returned no results.',
            'success'
        ]  # other results means still working

        while True:
            res_json = session.get(url).json()
            if 'urls' not in res_json:
                sleep(30)
                continue
            dus = res_json['urls']
            for du in dus:
                if du in sent_urls:
                    continue
                else:
                    sent_urls.append(du)
                    print(du)
                    yield du

            if res_json['state'] in success_results:
                break
            sleep(30)

    def send_query(self, version, context, **kwargs):
        """download the selected contexts/queries from GBD result tools.

        ``context`` should be a string which available values are in
        the ``context`` option of GBD results tool. The complete query
        will be generated with ``_make_query`` method and all keywork
        args. When context is a list, multiple queries will be run.

        """
        if not self.metadata:
            self.load_metadata()

        metadata = self.metadata

        query = self._make_query(context, version, **kwargs)
        session = requests_retry_session()
        res_data = session.post(self.url_data, data=query)
        if res_data.status_code not in [200, 202]:
            print(res_data.text)
            raise ValueError("status code not 200: {}".format(res_data.status_code))
        print("Finished sending query to GBD results tool. Please check your "
              "mailbox and get the taskID and run bulk_down with the taskID. ")

    def bulk_download(self, out_dir, taskID):
        url = self.url_task.format(hash=taskID)
        print('working on {}'.format(url))
        print('check status as https://ghdx.healthdata.org/gbd-results-tool/result/{}'.format(taskID))
        print('available downloads:')

        for u in self.download_links(url):
            tries = 1
            while tries <= 5:
                try:
                    self._run_download(u, out_dir)
                    break
                except (ValueError, requests.exceptions.ConnectionError):
                    if tries == 5:
                        raise
                    print("download interrupted, retrying...")
                    tries = tries + 1

    def _run_download(self, u, out_dir):
        '''accept an URL and download it to out_dir'''

        if not osp.exists(osp.join(out_dir)):
            os.mkdir(osp.join(out_dir))

        fn = osp.join(out_dir, osp.basename(u))
        print('downloading {} to {}'.format(u, fn))

        download(u, fn)

    def _make_query(self, context, version, **kwargs):
        """generate a query with the context, version and all keyword arguments.

        if a parameter is mandatory but not provided, it will fill
        with default values.

        """
        # metadata
        if not self.metadata:
            self.load_metadata()

        metadata = self.metadata
        ages = metadata['age']['id'].values
        # location: there is a `custom` location. don't include that one.
        locations_md = metadata['location']
        locations = locations_md[locations_md['id'] != 'custom']['id'].tolist()
        sexs = metadata['sex']['id'].tolist()
        years = metadata['year']['id'].tolist()
        metrics = metadata['metric']['id'].tolist()
        measures = metadata['measure']['id'].tolist()
        causes = metadata['cause']['id'].tolist()
        # risk/etiology/impairment
        # There are actually 4 types data in this dictionary:
        # risk, etiology, impairment and injury n-codes.
        # however injury n-codes is not enabled.
        # we might need to take of it later.
        rei = metadata['rei']
        # risks = rei[rei['type'] == 'risk']['rei_id'].values
        # etiologys = rei[rei['type'] == 'etiology']['rei_id'].values
        # impairments = rei[rei['type'] == 'impairment']['rei_id'].values

        # others metadata filed not include:
        # - groups
        # - year_range

        queries = {}

        # create query base on context and user input
        if context == 'le':
            measure = read_opt(kwargs, 'measure', default=26)
            metric = read_opt(kwargs, 'metric', default=5)
            cause = read_opt(kwargs, 'cause', default=causes)
            queries.update({
                'measure[]': measure,
                'metric[]': metric,
                'cause[]': cause
            })
        elif context == 'cause':
            measure = read_opt(kwargs, 'measure', default=measures)
            metric = read_opt(kwargs, 'metric', default=[1, 2, 3])
            cause = read_opt(kwargs, 'cause', default=causes)
            queries.update({
                'measure[]': measure,
                'metric[]': metric,
                'cause[]': cause
            })
        elif context in ['risk', 'etiology', 'impairment']:
            # TODO: should be split, don't combine these context here.
            measure = read_opt(kwargs, 'measure', default=measures)
            metric = read_opt(kwargs, 'metric', default=[1, 2, 3])
            context_values = rei[rei['type'] == context]['id'].tolist()
            context_values = read_opt(kwargs, 'rei', default=context_values)  # use user-provided ids if given
            cause = read_opt(kwargs, 'cause', default=causes)
            queries.update({
                'measure[]': measure,
                'metric[]': metric,
                context+'[]':context_values,
                'rei[]': context_values,
                'cause[]':  cause
            })
        else:
            # SEV/HALE/haqi
            print('not supported context.')
            raise NotImplementedError

        # insert context and version and other configs
        rows = read_opt(kwargs, 'rows', default=500000)  # the maximum records we can get
        # ^ Note: user guide[1] says it's 500000 row. But actually we can set this to 10000000
        # [1]: http://www.healthdata.org/sites/default/files/files/Data_viz/GBD_2017_Tools_Overview.pdf
        email = read_opt(kwargs, 'email', default='downloader@gapminder.org')
        idsOrNames = read_opt(kwargs, 'idsOrNames', default='ids')           # ids / names / both
        singleOrMult = read_opt(kwargs, 'singleOrMult', default='multiple')  # single / multiple
        base = read_opt(kwargs, 'base', default='single')

        location = read_opt(kwargs, 'location', default=locations)
        age = read_opt(kwargs, 'age', default=ages)
        sex = read_opt(kwargs, 'sex', default=sexs)
        year = read_opt(kwargs, 'year', default=years)
        audience = read_opt(kwargs, 'audience', default='public')
        toolid = read_opt(kwargs, 'toolID', default=1)
        gbdRound = read_opt(kwargs, 'gbdRound', default=2019)

        queries.setdefault('context', context)
        queries.setdefault('version', version)
        queries.setdefault('rows', rows)
        queries.setdefault('email', email)
        queries.setdefault('idsOrNames', idsOrNames)
        queries.setdefault('singleOrMult', singleOrMult)
        queries.setdefault('base', base)
        queries.setdefault('location[]', location)
        queries.setdefault('age[]', age)
        queries.setdefault('sex[]', sex)
        queries.setdefault('year[]', year)
        queries.setdefault('audience', audience)
        queries.setdefault('toolID', toolid)
        queries.setdefault('gbdRound', gbdRound)
        queries['numParams'] = (
            len(age)
            * len(year)
            * len(sex)
            * len(location)
            * len(cause)
            * len(metric)
            * len(measure))

        return queries
