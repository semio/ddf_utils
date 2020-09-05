# -*- coding: utf-8 -*-

"""functions for handling DDF datapackage"""

import os
import os.path as osp
import json
import csv
import re
from datetime import datetime, timezone

from collections.abc import Mapping, Sequence
from collections import OrderedDict
from itertools import product

import dask.dataframe as dd
import pandas as pd

from tqdm import tqdm

from .model.package import DDFcsv
from .model.utils import sort_json
from ddf_utils.chef.helpers import read_opt

import logging


logger = logging.getLogger(__name__)


# check if a directory is dataset root dir
def is_datapackage(path):
    """check if a directory is a dataset directory

    This function checks if ddf--index.csv and datapackage.json exists
    to judge if the dir is a dataset.
    """
    index_path = osp.join(path, 'ddf--index.csv')
    datapackage_path = osp.join(path, 'datapackage.json')
    if osp.exists(index_path) or osp.exists(datapackage_path):
        return True
    else:
        return False


def get_datapackage(path, use_existing=True, update=False, progress_bar=False):
    """get the datapackage.json from a dataset path, create one if it's not exists

    Parameters
    ----------
    path : `str`
        the dataset path

    Keyword Args
    ------------
    use_existing : bool
        whether or not to use the existing datapackage
    update : bool
        if update is true, will update the resources and schema in existing datapackage.json. else just return existing
        datapackage.json
    progress_bar : bool
        whether progress bar should be shown when generating ddfSchema.
    """
    datapackage_path = os.path.join(path, 'datapackage.json')

    if os.path.exists(datapackage_path):
        with open(datapackage_path, encoding='utf8') as f:
            datapackage_old = json.load(f, object_pairs_hook=OrderedDict)

        if use_existing:
            if not update:
                return datapackage_old
            try:
                datapackage_old.pop('resources')  # don't use the old resources
                datapackage_old.pop('ddfSchema')  # and ddf schema
            except KeyError:
                logger.warning('no resources or ddfSchema in datapackage.json')
            datapackage_new = create_datapackage(path, progress_bar=progress_bar, **datapackage_old)
        else:
            datapackage_new = create_datapackage(path, progress_bar=progress_bar)
    else:
        if use_existing:
            print("WARNING: no existing datapackage.json")
        datapackage_new = create_datapackage(path, progress_bar=progress_bar)

    return datapackage_new


def create_datapackage(path, gen_schema=True, progress_bar=False, **kwargs):
    """create datapackage.json base on the files in `path`.

    If you want to set some attributes manually, you can pass them as
    keyword arguments to this function

    Note
    ----
    A DDFcsv datapackage MUST contain the fields `name` and `resources`.

    if name is not provided, then the base name of `path` will be used.

    Parameters
    ----------
    path : `str`
        the dataset path to create datapackage.json
    gen_schema : bool
        whether to create DDFSchema in datapackage.json. Default is True
    progress_bar : bool
        whether progress bar should be shown when generating ddfSchema.
    kwargs : dict
        metadata to write into datapackage.json. According to spec,
        title, description, author and license SHOULD be fields in datapackage.json.
    """

    datapackage = OrderedDict()

    # setting default fields
    default_name = os.path.basename(os.path.normpath(os.path.abspath(path)))
    name = read_opt(kwargs, 'name', default=default_name, method='pop')
    title = read_opt(kwargs, 'title', default=default_name, method='pop')
    description = read_opt(kwargs, 'description', default='', method='pop')
    author = read_opt(kwargs, 'author', default='', method='pop')
    dp_license = read_opt(kwargs, 'license', default='', method='pop')
    default_lang = {'id': 'en'}
    lang = read_opt(kwargs, 'lang', default=default_lang, method='pop')

    datapackage['name'] = name
    datapackage['language'] = lang
    datapackage['title'] = title
    datapackage['description'] = description
    datapackage['author'] = author
    datapackage['license'] = dp_license

    # add all optional settings
    for k in sorted(kwargs.keys()):
        datapackage[k] = kwargs[k]

    # update the last updated time
    datapackage['created'] = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    # generate resources
    resources = []
    names_sofar = dict()

    for f in get_ddf_files(path):
        path_res = f
        name_res = os.path.splitext(os.path.basename(f))[0]

        if name_res in names_sofar.keys():
            names_sofar[name_res] = names_sofar[name_res] + 1
            # adding a tail to the resource name, because it should be unique
            name_res = name_res + '-' + str(names_sofar[name_res])
        else:
            names_sofar[name_res] = 0

        resources.append(OrderedDict([('path', path_res), ('name', name_res)]))

    for n, r in enumerate(resources):
        name_res = r['name']
        path_res = r['path']
        schema = {"fields": [], "primaryKey": None}

        if 'datapoints' in name_res:
            keys = re.match(r'ddf--datapoints--.*--by--(.*)', name_res).groups()[0]
            primary_keys = keys.split('--')
            # print(conc, primary_keys)
            for i, k in enumerate(primary_keys):
                if '-' in k:
                    k_new, *enums = k.split('-')
                    primary_keys[i] = k_new
                    constraint = {'enum': enums}
                    schema['fields'].append({'name': k_new, 'constraints': constraint})
                else:
                    schema['fields'].append({'name': k})

            with open(os.path.join(path, path_res)) as f:
                headers_line = f.readline()
                f.close()

            headers_line = headers_line.strip('\n')
            headers = headers_line.split(',')
            headers = [x.strip() for x in headers]
            headers = set(headers)
            fields = headers.difference(set(primary_keys))

            # check if headers and file name matched
            if len(headers) - len(fields) != len(primary_keys):
                logger.critical(f'in file: {name_res}: ')
                logger.critical(f'expected keys are {primary_keys}, but columns are {headers}')
                logger.critical(f'which does not contain all primary keys')
                raise ValueError("file name and file headers not matched")

            for field in fields:
                schema['fields'].append({'name': field})
            schema['primaryKey'] = primary_keys

            resources[n].update({'schema': schema})

        elif 'entities' in name_res:
            match = re.match(r'ddf--entities--([\w_]+)(--[\w_]*)?-?.*', name_res).groups()
            domain, concept = match
            if concept is not None:
                concept = concept[2:]

            with open(os.path.join(path, r['path'])) as f:
                reader = csv.reader(f, delimiter=',', quotechar='"')
                # we only need the headers for index file
                header = next(reader)

            if domain in header:
                key = domain
            elif concept is not None and concept in header:
                key = concept
            else:
                raise ValueError('no header in {} matches its implied domain/entity_set!'.format(name_res))
                # print(
                #     """There is no matching header found for {}. Using the first column header
                #     """.format(name_res)
                # )
                # key = header[0]

            schema['primaryKey'] = key
            for h in header:
                schema['fields'].append({'name': h})
            resources[n].update({'schema': schema})

        elif 'synonyms' in name_res:
            with open(os.path.join(path, r['path'])) as f:
                reader = csv.reader(f, delimiter=',', quotechar='"')
                header = next(reader)
            k1 = 'synonym'
            k2 = r['name'].split('--')[-1]
            schema['primaryKey'] = [k1, k2]
            for h in header:
                schema['fields'].append({'name': h})
            resources[n].update({'schema': schema})

        elif 'concepts' in name_res:
            with open(os.path.join(path, r['path'])) as f:
                reader = csv.reader(f, delimiter=',', quotechar='"')
                header = next(reader)
            schema['primaryKey'] = 'concept'
            for h in header:
                schema['fields'].append({'name': h})
            resources[n].update({'schema': schema})

        else:  # not entity/concept/datapoint/synonym. it's not supported yet so we don't include them.
            print("not supported file: " + name_res)
            resources[n] = None

    datapackage['resources'] = [x for x in resources if x is not None]

    # generate ddf schema
    if gen_schema:
        logger.info('generating ddf schema, may take some time...')
        dp = DDFcsv.from_dict(datapackage, base_path=path)
        dp.ddfSchema = dp.generate_ddf_schema(progress_bar=progress_bar)
        result = dp.to_dict()
    else:
        result = datapackage

    return sort_json(result)


def get_ddf_files(path, root=None):
    """yield all csv files which are named following the DDF model standard.

    Parameters
    -----------
    path : `str`
        the path to check
    root : `str`, optional
        if path is relative, append the root to all files.
    """
    info = next(os.walk(path))

    # don't include hidden and lang/etl dir.
    sub_dirs = [
        x for x in info[1] if (not x.startswith('.') and x not in ['lang', 'etl', 'langsplit'])
    ]
    files = list()
    for x in info[2]:
        if x.startswith('ddf--') and x != 'ddf--index.csv' and x.endswith('.csv'):
            files.append(x)
        else:
            logging.warning('skipping file {}'.format(x))

    for f in files:
        if root:
            yield os.path.join(root, f)
        else:
            yield f

    for sd in sub_dirs:
        for p in get_ddf_files(os.path.join(path, sd), root=sd):
            yield p
