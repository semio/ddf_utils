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
from time import sleep

import requests

import pandas as pd
from ddf_utils.chef.helpers import read_opt


url_hir = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/hierarchy/'
url_metadata = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/metadata/'
url_version = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/version/'
# url for query data: http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/data.php
# below is url for download data as zip
url_data = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/download.php'
url_task = 'https://s3.healthdata.org/gbd-api-2016-production/{hash}'  # access to download link

metadata = None


def load_metadata():
    """load all codes used in GBD in a dictionary."""
    meta = requests.get(url_metadata).json()
    versions = requests.get(url_version).json()

    global metadata
    metadata = {}

    for k in meta['data'].keys():
        metadata[k] = pd.DataFrame.from_dict(meta['data'][k], orient='index')

    metadata['version'] = pd.DataFrame.from_dict(versions['data'], orient='index')

    return metadata


def has_newer_source(ver):
    if not metadata:
        load_metadata()

    versions = metadata['version']
    newer = versions[versions['vesrion_id'] > ver].values
    return bool(len(newer) > 0)


def bulk_download(out_dir, version, context=None, query=None, **kwargs):
    """download the selected contexts/queries from GBD result tools.

    Either context or query should be supplied. If both are supplied,
    query will be used.

    `context` should be a list of string and `query` sould be a list
    of dictionaries containing post requests data.
    """
    if not metadata:
        load_metadata()

    if query is None and context is None:
        raise ValueError('one of context and query should be supplied!')
    elif query is None:
        if isinstance(context, list):
            query = [_make_query(c, version, **kwargs) for c in context]
        else:
            query = [_make_query(context, version, **kwargs)]
    else:
        if not isinstance(query, list):
            query = [query]

    success_results = [
        'Your search returned no results.',
        'success'
    ]

    taskIDs = set()

    # make a series of queries, the server will response a series of task ids.
    for q in query:
        res_data = requests.post(url_data, data=q)
        # print(res_data.json())
        if isinstance(res_data.json()['taskID'], list):
            for taskID in res_data.json()['taskID']:
                taskIDs.add(taskID)
        else:
            taskIDs.add(res_data.json()['taskID'])

    # then, we check each task, download all files linked to the task.
    if len(taskIDs) == 0:
        print('no available results')
        return False

    successed = 0

    for i in taskIDs:
        url = url_task.format(hash=i)
        print('working on {}'.format(url))
        print('check status as http://ghdx.healthdata.org/gbd-results-tool/result/{}'.format(i))
        print('available downloads:')

        download_urls = []

        while True:
            res_json = requests.get(url).json()

            dus = res_json['urls']
            for du in dus:
                if du in download_urls:
                    continue
                else:
                    download_urls.append(du)
                    print(du)

            if res_json['state'] in success_results:
                break
            sleep(10)

        if res_json['state'] == success_results[0]:
            continue
        else:
            successed = successed + 1

        download_urls = res_json['urls']

        for u in download_urls:
            _run_download(u, out_dir, taskID=i)
    if successed == 0:
        return False
    return True


def _run_download(u, out_dir, taskID):
    '''accept an URL and download it to out_dir'''
    download_file = requests.get(u, stream=True)
    if not osp.exists(osp.join(out_dir, taskID[:8])):
        os.mkdir(osp.join(out_dir, taskID[:8]))
    fn = osp.join(out_dir, taskID[:8], osp.basename(u))
    print('downloading {} to {}'.format(u, fn))
    with open(fn, 'wb') as f:
        for c in download_file.iter_content(chunk_size=1024):
            f.write(c)
        f.close()


def _make_query(context, version, **kwargs):
    # metadata
    if not metadata:
        load_metadata()

    ages = metadata['age']['age_id'].values
    # location: there is a `custom` location. don't include that one.
    locations = [x for x in metadata['location']['location_id'].values if x != 'custom']
    sexs = metadata['sex']['sex_id'].values
    years = metadata['year']['year_id'].values
    metrics = metadata['metric']['metric_id'].values
    measures = metadata['measure']['measure_id'].values
    causes = metadata['cause']['cause_id'].values
    # risk/etiology/impairment
    # There are actually 4 types data in this dictionary:
    # risk, etiology, impairmen and injury n-codes.
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
        context_values = rei[rei['type'] == context]['rei_id'].values
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
    # [1]: http://ghdx.healthdata.org/sites/default/files/ihme_query_tool/GBD_Data_Tool_User_Guide_(2016).pdf
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
