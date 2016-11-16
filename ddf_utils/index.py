# -*- coding: utf-8 -*-
"""create ddf--index.csv"""

import csv
from pandas import DataFrame, concat
import os
import re
import json
from collections import OrderedDict


def concept_index(path, concept_file):

    df = DataFrame([], columns=index_columns)

    with open(os.path.join(path, concept_file)) as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        header = next(reader)

    header.remove('concept')
    df['value'] = header
    df['key'] = 'concept'
    df['file'] = concept_file

    return df


def entity_index(path, entity_file):
    df = DataFrame([], columns=index_columns)

    # get the domain/set name from file name.
    # there are 2 possible format for entity filename:
    # 1. ddf--entities--$domain.csv
    # 2. ddf--entities--$domain--$set.csv
    match = re.match('ddf--entities--([\w_]+)-*([\w_]*).csv', entity_file).groups()
    if len(match) == 1:
        domain = match[0]
        concept = None
    else:
        domain, concept = match

    with open(os.path.join(path, entity_file)) as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        # we only need the headers for index file
        header = next(reader)

    # find out which key is used in the file.
    # the key in index should be consistent with the one in entities
    if domain in header:
        header.remove(domain)
        key = domain
    elif concept in header:
        header.remove(concept)
        key = concept
    else:
        raise ValueError("the header in entity file not match with file name! \
        Please double check your entities files.")

    df['value'] = header
    df['key'] = key
    df['file'] = entity_file

    return df


def datapoint_index(path, datapoint_file):
    df = DataFrame([], columns=index_columns)

    value, key = re.match('ddf--datapoints--([\w_]+)--by--(.*).csv', datapoint_file).groups()

    key = ','.join(key.split('--'))

    df['value'] = value.split('--')
    df['key'] = key
    df['file'] = datapoint_file

    return df


def create_index_file(path, indexfile='ddf--index.csv'):
    fs = os.listdir(path)

    res = []
    for f in fs:
        if 'concept' in f:
            res.append(concept_index(path, f))
        if 'entities' in f:
            res.append(entity_index(path, f))
        if 'datapoints' in f:
            res.append(datapoint_index(path, f))

    res_df = concat(res, ignore_index=True)
    res_df = res_df.drop_duplicates()
    return res_df


def get_datapackage(path, update_existing=False):
    datapackage_path = os.path.join(path, 'datapackage.json')
    if os.path.exists(datapackage_path):
        with open(datapackage_path) as f:
            datapackage_old = json.load(f, object_pairs_hook=OrderedDict)

        if update_existing:
            _ = datapackage_old.pop('resources')  # don't use the old resources
            datapackage_new = create_datapackage(path, **datapackage_old)
            with open(datapackage_path, 'w') as f:
                json.dump(datapackage_new, f, indent=4)
        else:
            return datapackage_old
    else:
        datapackage_new = create_datapackage(path)

    return datapackage_new


def get_ddf_files(path, root=None):
    info = next(os.walk(path))

    # don't include hidden and lang/etl dir.
    sub_dirs = [x for x in info[1] if (not x.startswith('.') and not x in ['lang', 'etl', 'langsplit'])]
    files = [x for x in info[2] if (x.startswith('ddf--') and x != 'ddf--index.csv')]

    for f in files:
        if root:
            yield os.path.join(root, f)
        else:
            yield f

    for sd in sub_dirs:
        for p in get_ddf_files(os.path.join(path, sd), root=sd):
            yield p


def create_datapackage(path, **kwargs):
    """create datapackage.json base on the files in `path`.

    A DDFcsv datapackage MUST contain the fields name and resources.

    if name is None, then the base name of `path` will be used.
    """

    datapackage = OrderedDict()

    try:
        name = kwargs.pop('name')
    except KeyError:
        print('name not specified, using the path name')
        name = os.path.basename(os.path.normpath(os.path.abspath(path)))

    datapackage['name'] = name

    # add all optional settings
    for k in sorted(kwargs.keys()):
        datapackage[k] = kwargs[k]

    # generate resources
    resources = []
    names_sofar = dict()

    for f in get_ddf_files(path):
        path_res = f
        name_res = os.path.splitext(os.path.basename(f))[0]

        if name_res in names_sofar.keys():
            names_sofar[name_res] = names_sofar[name_res] + 1
            name_res = name_res + '-' + str(names_sofar[name_res])
        else:
            names_sofar[name_res] = 0

        resources.append(OrderedDict([('path', path_res), ('name', name_res)]))

    # TODO: make separate functions. this function is too long.
    for n, r in enumerate(resources):
        name_res = r['name']
        schema = {"fields":[], "primaryKey":None}

        if 'datapoints' in name_res:
            conc,keys = re.match('ddf--datapoints--([\w_]+)--by--(.*)', name_res).groups()
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

            schema['fields'].append({'name': conc})
            schema['primaryKey'] = primary_keys

            resources[n].update({'schema': schema})

        if 'entities' in name_res:
            match = re.match('ddf--entities--([\w_]+)-*([\w_]*)', name_res).groups()
            if len(match) == 1:
                domain = match[0]
                concept = None
            else:
                domain, concept = match

            with open(os.path.join(path, r['path'])) as f:
                reader = csv.reader(f, delimiter=',', quotechar='"')
                # we only need the headers for index file
                header = next(reader)

            if domain in header:
                header.remove(domain)
                key = domain
            elif concept in header:
                header.remove(concept)
                key = concept
            else:
                raise ValueError('no matching header found for {}!'.format(name_res))
                # print(
                #     """There is no matching header found for {}. Using the first column header
                #     """.format(name_res)
                # )
                # key = header[0]

            schema['primaryKey'] = key

            for h in header:
                schema['fields'].append({'name': h})

            resources[n].update({'schema': schema})

        if 'concepts' in name_res:
            with open(os.path.join(path, r['path'])) as f:
                reader = csv.reader(f, delimiter=',', quotechar='"')
                header = next(reader)

            header.remove('concept')
            schema['primaryKey'] = 'concept'
            for h in header:
                schema['fields'].append({'name': h})

            resources[n].update({'schema': schema})
    # return
    datapackage['resources'] = resources
    return datapackage
