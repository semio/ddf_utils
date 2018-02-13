# -*- coding: utf-8 -*-

"""Functions for IHME
"""

import os.path as osp
from time import sleep

import requests
from lxml import html

import pandas as pd


url_hir = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/hierarchy/'
url_metadata = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/metadata/'
url_version = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/version/'
# url for query data: http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/data.php
# below is url for download data as zip
url_data = 'http://ghdx.healthdata.org/sites/all/modules/custom/ihme_query_tool/gbd-search/php/download.php'
url_task = 'https://s3.healthdata.org/gbd-api-2016-production/{hash}'  # access to download link

metadata = None


def load_metadata():
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
    if len(newer) > 0:
        return True
    else:
        return False


def bulk_download(out_dir, version, context, query=None):
    if not metadata:
        load_metadata()

    if query is None:
        query = _make_query(context, version)
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

        res_json = requests.get(url).json()

        while res_json['state'] not in success_results:
            print('download is not ready yet, retrying download in 10 seconds...')
            sleep(10)
            res_json = requests.get(url).json()

        if res_json['state'] == success_results[0]:
            continue
        else:
            successed = successed + 1

        download_urls = res_json['urls']

        for u in download_urls:
            download_file = requests.get(u, stream=True)
            fn = osp.join(out_dir, osp.basename(u))

            with open(fn, 'wb') as f:
                for c in download_file.iter_content(chunk_size=1024):
                    f.write(c)
                f.close()
    if successed == 0:
        return False
    return True


def _make_query(context, version):
    if not metadata:
        load_metadata()

    # fixed parameters
    rows = 10000000  # the maximum records we can get
    email = 'downloader@gapminder.org'
    idsOrNames = 'ids'
    singleOrMult = 'single'
    base = 'single'

    # metadata
    ages = metadata['age']['age_id'].values
    # location: there is a `custom` location. don't include that one.
    locations = [x for x in metadata['location']['location_id'].values if x != 'custom']
    sexs = metadata['sex']['sex_id'].values
    years = metadata['year']['year_id'].values
    metrics = metadata['metric']['metric_id'].values
    measures = metadata['measure']['measure_id'].values
    causes = metadata['cause']['cause_id'].values

    # others metadata filed not incluede:
    # - rei
    # - groups
    # - year_range

    queries = []

    if context == 'le':
        # TODO: improve here.
        # for now all records can fit in one request
        # but might not true for later.
        measure = 26
        metric = 5
        queries.append({
            'measure[]': measure,
            'metric[]': metric
        })
    else:
        print('not supported context.')
        raise NotImplementedError

    # insert context and version
    for q in queries:
        q.setdefault('context', context)
        q.setdefault('version', version)
        q.setdefault('rows', rows)
        q.setdefault('email', email)
        q.setdefault('idsOrNames', idsOrNames)
        q.setdefault('singleOrMult', singleOrMult)
        q.setdefault('base', base)
        q.setdefault('location[]', locations)
        q.setdefault('age[]', ages)
        q.setdefault('sex[]', sexs)
        q.setdefault('year[]', years)
        q.setdefault('metric[]', metrics)
        q.setdefault('measure[]', measures)
        q.setdefault('cause[]', causes)

    return queries
