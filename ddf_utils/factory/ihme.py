# -*- coding: utf-8 -*-

"""Functions for IHME

The `GBD result tool`_ at IHME contains all data for GBD results, but
they don't have an open API to query the data. However the website
uses a json endpoint and it doesn't need authorization. So we also
make use of it.

.. _`GBD result tool`: http://ghdx.healthdata.org/gbd-results-tool

"""

import os
import os.path as osp
import math
from time import sleep
import requests
import pandas as pd

from tqdm import tqdm

from ddf_utils.chef.helpers import read_opt
from . common import requests_retry_session, DataFactory, retry


# TODO: add missing context/base configures.
class IHMELoader(DataFactory):
    url_hir = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/hierarchy/'
    url_metadata = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/metadata/'
    url_version = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/version/'
    # url for query data:
    # http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/data.php
    # below is url for download data as zip
    url_data = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/download.php'
    url_task = 'https://s3.healthdata.org/gbd-api-2017-public/{hash}'  # access to download link

    def load_metadata(self):
        """load all codes used in GBD in a dictionary."""

        session = requests_retry_session()
        meta = session.get(self.url_metadata).json()
        versions = session.get(self.url_version).json()

        metadata = {}

        for k in meta['data'].keys():
            metadata[k] = pd.DataFrame.from_dict(meta['data'][k], orient='index')

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
            sleep(10)

    def bulk_download(self, out_dir, version, context=None, query=None, **kwargs):
        """download the selected contexts/queries from GBD result tools.

        Either context or query should be supplied. If both are supplied,
        query will be used.

        `context` should be a list of string and `query` should be a list
        of dictionaries containing post requests data.
        """
        if not self.metadata:
            self.load_metadata()

        metadata = self.metadata

        if query is None and context is None:
            raise ValueError('one of context and query should be supplied!')
        elif query is None:
            if isinstance(context, list):
                query = [self._make_query(c, version, **kwargs) for c in context]
            else:
                query = [self._make_query(context, version, **kwargs)]
        else:
            if not isinstance(query, list):
                query = [query]

        taskIDs = set()

        # make a series of queries, the server will response a series of task ids.
        session = requests_retry_session()
        for q in query:
            res_data = session.post(self.url_data, data=q)
            # print(res_data.json())
            if res_data.status_code not in [200, 202]:
                print(res_data.text)
                raise ValueError("status code not 200: {}".format(res_data.status_code))
            if isinstance(res_data.json()['taskID'], list):
                for taskID in res_data.json()['taskID']:
                    taskIDs.add(taskID)
            else:
                taskIDs.add(res_data.json()['taskID'])

        # then, we check each task, download all files linked to the task.
        if len(taskIDs) == 0:
            print('no available results')
            return

        for i in taskIDs:
            url = self.url_task.format(hash=i)
            print('working on {}'.format(url))
            print('check status as http://ghdx.healthdata.org/gbd-results-tool/result/{}'.format(i))
            print('available downloads:')

            for u in self.download_links(url):
                tries = 1
                while tries <= 5:
                    try:
                        self._run_download(u, out_dir, taskID=i)
                        break
                    except (ValueError, requests.exceptions.ConnectionError) as e:
                        if tries == 5:
                            raise
                        print("download interrupted, retrying...")
                        tries = tries + 1

        return [i[:8] for i in taskIDs]

    @retry(times=3)
    def _run_download(self, u, out_dir, taskID):
        '''accept an URL and download it to out_dir'''
        download_file = requests_retry_session().get(u, stream=True, timeout=60)

        if download_file.status_code != 200:
            print(f'can not download source file: {u}')
            return

        if not osp.exists(osp.join(out_dir, taskID[:8])):
            os.mkdir(osp.join(out_dir, taskID[:8]))

        fn = osp.join(out_dir, taskID[:8], osp.basename(u))
        print('downloading {} to {}'.format(u, fn))

        block_size = 1024
        total_size = int(download_file.headers.get('content-length', 0))
        wrote = 0
        with open(fn, 'wb') as f:
            for c in tqdm(download_file.iter_content(chunk_size=block_size),
                          total=math.ceil(total_size // block_size), unit='KB', unit_scale=True):
                f.write(c)
                wrote = wrote + len(c)
                f.flush()
        if wrote != total_size:
            raise ValueError("download failed.")

    def _make_query(self, context, version, **kwargs):
        # metadata
        if not self.metadata:
            self.load_metadata()

        metadata = self.metadata
        ages = metadata['age']['id'].values
        # location: there is a `custom` location. don't include that one.
        locations_md = metadata['location']
        locations = locations_md[locations_md['location_id'] != 'custom']['id'].tolist()
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
            context_values = rei[rei['type'] == context]['rei_id'].tolist()
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
        rows = read_opt(kwargs, 'rows', default=10000000)  # the maximum records we can get
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

        return queries
